use crate::core::SpinelDBError;
use crate::core::cluster::state::NodeFlags;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::{RespFrame, RespFrameCodec, RespValue};
use crate::core::storage::data_types::DataValue;
use crate::core::storage::vector::MetadataFilter;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// Implements `VS.FEDERATEDSEARCH`: scatter-gather vector search across all cluster nodes.
///
/// VS.FEDERATEDSEARCH key vector [COUNT count] [EF ef] [FILTER filter] [THRESHOLD threshold]
#[derive(Debug, Clone, Default)]
pub struct VsFederatedSearch {
    pub key: Bytes,
    pub query: Vec<f32>,
    pub count: usize,
    pub ef: Option<usize>,
    pub filter: Option<String>,
    pub threshold: Option<f32>,
}

impl ParseCommand for VsFederatedSearch {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount(
                "VS.FEDERATEDSEARCH".to_string(),
            ));
        }

        let key = extract_bytes(&args[0])?;
        let mut query = Vec::new();
        let mut i = 1;
        let mut count = 10;
        let mut ef = None;
        let mut filter = None;
        let mut threshold = None;

        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            match upper.as_str() {
                "EF" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.FEDERATEDSEARCH".to_string(),
                        ));
                    }
                    ef = Some(
                        extract_string(&args[i])?
                            .parse::<usize>()
                            .map_err(|_| SpinelDBError::NotAnInteger)?,
                    );
                    i += 1;
                }
                "COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.FEDERATEDSEARCH".to_string(),
                        ));
                    }
                    count = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    i += 1;
                }
                "FILTER" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.FEDERATEDSEARCH".to_string(),
                        ));
                    }
                    filter = Some(extract_string(&args[i])?);
                    i += 1;
                }
                "THRESHOLD" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.FEDERATEDSEARCH".to_string(),
                        ));
                    }
                    threshold = Some(
                        extract_string(&args[i])?
                            .parse::<f32>()
                            .map_err(|_| SpinelDBError::NotAFloat)?,
                    );
                    i += 1;
                }
                _ => {
                    let val = arg_str
                        .parse::<f32>()
                        .map_err(|_| SpinelDBError::NotAFloat)?;
                    query.push(val);
                    i += 1;
                }
            }
        }

        if query.is_empty() {
            return Err(SpinelDBError::InvalidRequest(
                "query vector must have at least one dimension".to_string(),
            ));
        }

        Ok(VsFederatedSearch {
            key,
            query,
            count,
            ef,
            filter,
            threshold,
        })
    }
}

#[async_trait]
impl ExecutableCommand for VsFederatedSearch {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let cluster = ctx
            .state
            .cluster
            .as_ref()
            .ok_or_else(|| SpinelDBError::InvalidRequest("not in cluster mode".into()))?;

        let password = ctx.state.config.lock().await.password.clone();

        // Build the VS.SEARCH command frame to send to each node
        let mut search_args = vec![
            RespFrame::BulkString(Bytes::from_static(b"VS.SEARCH")),
            RespFrame::BulkString(self.key.clone()),
        ];
        for &v in &self.query {
            search_args.push(RespFrame::BulkString(Bytes::from(v.to_string())));
        }
        if let Some(ef) = self.ef {
            search_args.push(RespFrame::BulkString(Bytes::from_static(b"EF")));
            search_args.push(RespFrame::BulkString(Bytes::from(ef.to_string())));
        }
        search_args.push(RespFrame::BulkString(Bytes::from_static(b"COUNT")));
        // Request more results per node to improve merged quality
        let per_node_count = (self.count * 2).max(20);
        search_args.push(RespFrame::BulkString(Bytes::from(
            per_node_count.to_string(),
        )));
        if let Some(ref f) = self.filter {
            search_args.push(RespFrame::BulkString(Bytes::from_static(b"FILTER")));
            search_args.push(RespFrame::BulkString(Bytes::from(f.clone())));
        }
        if let Some(t) = self.threshold {
            search_args.push(RespFrame::BulkString(Bytes::from_static(b"THRESHOLD")));
            search_args.push(RespFrame::BulkString(Bytes::from(t.to_string())));
        }
        let search_frame = RespFrame::Array(search_args);

        // Fan-out: connect to all primary nodes and send search
        let mut tasks = Vec::new();
        for node_entry in cluster.nodes.iter() {
            let node_info = &node_entry.value().node_info;
            let flags = node_info.get_flags();

            // Skip non-primaries, failed nodes, and self
            if !flags.contains(NodeFlags::PRIMARY)
                || flags.intersects(NodeFlags::FAIL | NodeFlags::MYSELF)
            {
                continue;
            }

            let addr_str = node_info.addr.clone();
            let password_clone = password.clone();
            let frame_clone = search_frame.clone();

            tasks.push(tokio::spawn(async move {
                let addr: std::net::SocketAddr = addr_str
                    .parse()
                    .map_err(|e| SpinelDBError::InvalidRequest(format!("bad addr: {e}")))?;
                let stream = TcpStream::connect(&addr).await?;
                let mut framed = Framed::new(stream, RespFrameCodec);

                if let Some(ref pass) = password_clone {
                    let auth_frame = RespFrame::Array(vec![
                        RespFrame::BulkString(Bytes::from_static(b"AUTH")),
                        RespFrame::BulkString(Bytes::from(pass.clone())),
                    ]);
                    framed.send(auth_frame).await?;
                    let _ = framed.next().await;
                }

                framed.send(frame_clone).await?;
                match framed.next().await {
                    Some(Ok(frame)) => Ok(frame),
                    _ => Err(SpinelDBError::Internal("no response from node".into())),
                }
            }));
        }

        // Also search locally
        let local_task = {
            let key = self.key.clone();
            let query = self.query.clone();
            let count = self.count;
            let ef = self.ef;
            let filter = self.filter.clone();
            let threshold = self.threshold;
            let shard_guard = ctx.get_single_shard_context_mut()?;
            let (_shard, shard_cache) = shard_guard;
            let entry = shard_cache.get(&key);

            if let Some(entry) = entry {
                if let DataValue::SpinelVector(ref sv) = entry.data {
                    let parsed_filter = if let Some(ref filter_expr) = filter {
                        Some(
                            MetadataFilter::parse_expr(filter_expr)
                                .map_err(SpinelDBError::InvalidRequest)?,
                        )
                    } else {
                        None
                    };
                    let results = sv
                        .search_with_filter(
                            &query,
                            count * 2,
                            ef,
                            parsed_filter.as_ref(),
                            threshold,
                        )
                        .map_err(SpinelDBError::InvalidRequest)?;
                    // Serialize results as RespFrame
                    let mut resp_results = Vec::new();
                    for result in &results {
                        let mut item = Vec::new();
                        item.push(RespValue::BulkString(result.id.clone()));
                        item.push(RespValue::BulkString(Bytes::from(
                            result.distance.to_string(),
                        )));
                        let vector_vals: Vec<RespValue> = result
                            .vector
                            .iter()
                            .map(|v| RespValue::BulkString(Bytes::from(v.to_string())))
                            .collect();
                        item.push(RespValue::Array(vector_vals));
                        if let Some(ref meta) = result.metadata {
                            item.push(RespValue::BulkString(meta.clone()));
                        } else {
                            item.push(RespValue::Null);
                        }
                        resp_results.push(RespValue::Array(item));
                    }
                    Ok(RespValue::Array(resp_results))
                } else {
                    Err(SpinelDBError::WrongType)
                }
            } else {
                Ok(RespValue::Array(Vec::new()))
            }
        };

        // Fan-in: collect all results (remote + local)
        let mut all_results: Vec<(String, f32)> = Vec::new();

        // Process local results
        let local_resp = local_task;
        if let Ok(RespValue::Array(items)) = local_resp {
            for item in items {
                if let RespValue::Array(parts) = item
                    && parts.len() >= 2
                    && let (RespValue::BulkString(id), RespValue::BulkString(dist_bytes)) =
                        (&parts[0], &parts[1])
                    && let Some(dist) = std::str::from_utf8(dist_bytes)
                        .ok()
                        .and_then(|s| s.parse::<f32>().ok())
                {
                    all_results.push((String::from_utf8_lossy(id).to_string(), dist));
                }
            }
        }

        // Process remote results
        let remote_results = join_all(tasks).await;
        for result in remote_results {
            if let Ok(Ok(RespFrame::Array(items))) = result {
                for item in items {
                    if let RespFrame::Array(parts) = item
                        && parts.len() >= 2
                        && let (RespFrame::BulkString(id), RespFrame::BulkString(dist_bytes)) =
                            (&parts[0], &parts[1])
                        && let Some(dist) = std::str::from_utf8(dist_bytes)
                            .ok()
                            .and_then(|s| s.parse::<f32>().ok())
                    {
                        all_results.push((String::from_utf8_lossy(id).to_string(), dist));
                    }
                }
            }
        }

        // Deduplicate by ID (keep best distance)
        let mut best: std::collections::HashMap<String, f32> = std::collections::HashMap::new();
        for (id, dist) in &all_results {
            best.entry(id.clone())
                .and_modify(|e| {
                    if *dist < *e {
                        *e = *dist;
                    }
                })
                .or_insert(*dist);
        }

        // Sort by distance and take top-K
        let mut sorted: Vec<(String, f32)> = best.into_iter().collect();
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted.truncate(self.count);

        // Build response
        let resp_results: Vec<RespValue> = sorted
            .into_iter()
            .map(|(id, dist)| {
                RespValue::Array(vec![
                    RespValue::BulkString(Bytes::from(id)),
                    RespValue::BulkString(Bytes::from(dist.to_string())),
                ])
            })
            .collect();

        Ok((RespValue::Array(resp_results), WriteOutcome::DidNotWrite))
    }
}

impl CommandSpec for VsFederatedSearch {
    fn name(&self) -> &'static str {
        "vs.federatedsearch"
    }
    fn arity(&self) -> i64 {
        -3
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }
    fn first_key(&self) -> i64 {
        1
    }
    fn last_key(&self) -> i64 {
        1
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![self.key.clone()];
        for &v in &self.query {
            args.push(Bytes::from(v.to_string()));
        }
        args.push(Bytes::from_static(b"COUNT"));
        args.push(Bytes::from(self.count.to_string()));
        if let Some(ef) = self.ef {
            args.push(Bytes::from_static(b"EF"));
            args.push(Bytes::from(ef.to_string()));
        }
        if let Some(ref f) = self.filter {
            args.push(Bytes::from_static(b"FILTER"));
            args.push(Bytes::from(f.clone()));
        }
        if let Some(t) = self.threshold {
            args.push(Bytes::from_static(b"THRESHOLD"));
            args.push(Bytes::from(t.to_string()));
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_parse_federatedsearch_basic() {
        let c = VsFederatedSearch::parse(&[bs("idx"), bs("1.0"), bs("2.0"), bs("3.0")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.query, vec![1.0, 2.0, 3.0]);
        assert_eq!(c.count, 10);
    }

    #[test]
    fn test_parse_federatedsearch_with_count() {
        let c = VsFederatedSearch::parse(&[bs("idx"), bs("1.0"), bs("2.0"), bs("COUNT"), bs("5")])
            .unwrap();
        assert_eq!(c.count, 5);
    }

    #[test]
    fn test_parse_federatedsearch_with_threshold() {
        let c = VsFederatedSearch::parse(&[
            bs("idx"),
            bs("1.0"),
            bs("2.0"),
            bs("THRESHOLD"),
            bs("0.5"),
        ])
        .unwrap();
        assert_eq!(c.threshold, Some(0.5));
    }

    #[test]
    fn test_parse_federatedsearch_too_few_args() {
        let r = VsFederatedSearch::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_parse_federatedsearch_no_vector_is_error() {
        let r = VsFederatedSearch::parse(&[bs("idx"), bs("COUNT"), bs("5")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }
}
