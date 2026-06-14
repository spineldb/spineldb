// src/core/commands/vector/vs_msearch.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::vector::MetadataFilter;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Implements the `VS.MSEARCH` command for batch k-nearest neighbor search.
///
/// VS.MSEARCH key [COUNT count] [EF ef] [FILTER filter] [THRESHOLD threshold] query1 query2 ...
#[derive(Debug, Clone, Default)]
pub struct VsMSearch {
    pub key: Bytes,
    pub queries: Vec<Vec<f32>>,
    pub count: usize,
    pub ef: Option<usize>,
    pub filter: Option<String>,
    pub threshold: Option<f32>,
}

impl ParseCommand for VsMSearch {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 3 {
            return Err(SpinelDBError::WrongArgumentCount("VS.MSEARCH".to_string()));
        }

        let key = extract_bytes(&args[0])?;

        let mut count = 10;
        let mut ef = None;
        let mut filter = None;
        let mut threshold = None;
        let mut i = 1;

        // Parse optional parameters first
        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            match upper.as_str() {
                "COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.MSEARCH".to_string()));
                    }
                    count = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    i += 1;
                }
                "EF" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.MSEARCH".to_string()));
                    }
                    ef = Some(
                        extract_string(&args[i])?
                            .parse::<usize>()
                            .map_err(|_| SpinelDBError::NotAnInteger)?,
                    );
                    i += 1;
                }
                "FILTER" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.MSEARCH".to_string()));
                    }
                    filter = Some(extract_string(&args[i])?);
                    i += 1;
                }
                "THRESHOLD" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("VS.MSEARCH".to_string()));
                    }
                    threshold = Some(
                        extract_string(&args[i])?
                            .parse::<f32>()
                            .map_err(|_| SpinelDBError::NotAFloat)?,
                    );
                    i += 1;
                }
                _ => break, // Not a keyword, start parsing queries
            }
        }

        // Parse remaining args as query vectors separated by QUERY keyword
        // Syntax: QUERY v1 v2 v3 QUERY v4 v5 v6 ...
        let mut queries = Vec::new();
        let mut current_query = Vec::new();

        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            if upper == "QUERY" {
                // Save current query if any
                if !current_query.is_empty() {
                    queries.push(current_query.clone());
                    current_query.clear();
                }
                i += 1;
                continue;
            }

            // Try to parse as float
            let val = arg_str
                .parse::<f32>()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            current_query.push(val);
            i += 1;
        }

        // Don't forget the last query
        if !current_query.is_empty() {
            queries.push(current_query);
        }

        if queries.is_empty() {
            return Err(SpinelDBError::InvalidRequest(
                "at least one QUERY is required".to_string(),
            ));
        }

        Ok(VsMSearch {
            key,
            queries,
            count,
            ef,
            filter,
            threshold,
        })
    }
}

#[async_trait]
impl ExecutableCommand for VsMSearch {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref sv) = entry.data {
            let parsed_filter = if let Some(ref filter_expr) = self.filter {
                Some(
                    MetadataFilter::parse_expr(filter_expr)
                        .map_err(SpinelDBError::InvalidRequest)?,
                )
            } else {
                None
            };

            let batch_results = sv
                .msearch(
                    &self.queries,
                    self.count,
                    self.ef,
                    parsed_filter.as_ref(),
                    self.threshold,
                )
                .map_err(SpinelDBError::InvalidRequest)?;

            // Flatten batch results into a single array
            let mut resp_results = Vec::new();
            for query_results in &batch_results {
                let mut query_resp = Vec::new();
                for result in query_results {
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
                    query_resp.push(RespValue::Array(item));
                }
                resp_results.push(RespValue::Array(query_resp));
            }

            Ok((RespValue::Array(resp_results), WriteOutcome::DidNotWrite))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsMSearch {
    fn name(&self) -> &'static str {
        "vs.msearch"
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
        if let Some(ef) = self.ef {
            args.push(Bytes::from_static(b"EF"));
            args.push(Bytes::from(ef.to_string()));
        }
        if let Some(ref filter) = self.filter {
            args.push(Bytes::from_static(b"FILTER"));
            args.push(Bytes::from(filter.clone()));
        }
        if let Some(threshold) = self.threshold {
            args.push(Bytes::from_static(b"THRESHOLD"));
            args.push(Bytes::from(threshold.to_string()));
        }
        args.push(Bytes::from_static(b"COUNT"));
        args.push(Bytes::from(self.count.to_string()));
        for q in &self.queries {
            args.push(Bytes::from_static(b"QUERY"));
            for &v in q {
                args.push(Bytes::from(v.to_string()));
            }
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
    fn test_vsmsearch_parse_single_query() {
        let c =
            VsMSearch::parse(&[bs("idx"), bs("QUERY"), bs("1.0"), bs("2.0"), bs("3.0")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.queries.len(), 1);
        assert_eq!(c.queries[0], vec![1.0, 2.0, 3.0]);
        assert_eq!(c.count, 10);
    }

    #[test]
    fn test_vsmsearch_parse_multiple_queries() {
        let c = VsMSearch::parse(&[
            bs("idx"),
            bs("QUERY"),
            bs("1.0"),
            bs("2.0"),
            bs("QUERY"),
            bs("3.0"),
            bs("4.0"),
        ])
        .unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.queries.len(), 2);
        assert_eq!(c.queries[0], vec![1.0, 2.0]);
        assert_eq!(c.queries[1], vec![3.0, 4.0]);
    }

    #[test]
    fn test_vsmsearch_parse_with_count() {
        let c = VsMSearch::parse(&[
            bs("idx"),
            bs("COUNT"),
            bs("5"),
            bs("QUERY"),
            bs("1.0"),
            bs("2.0"),
        ])
        .unwrap();
        assert_eq!(c.count, 5);
    }

    #[test]
    fn test_vsmsearch_parse_with_filter() {
        let c = VsMSearch::parse(&[
            bs("idx"),
            bs("FILTER"),
            bs("type=a"),
            bs("QUERY"),
            bs("1.0"),
            bs("2.0"),
        ])
        .unwrap();
        assert_eq!(c.filter, Some("type=a".to_string()));
    }

    #[test]
    fn test_vsmsearch_parse_all_params() {
        let c = VsMSearch::parse(&[
            bs("idx"),
            bs("COUNT"),
            bs("5"),
            bs("EF"),
            bs("100"),
            bs("FILTER"),
            bs("type=a"),
            bs("THRESHOLD"),
            bs("0.5"),
            bs("QUERY"),
            bs("1.0"),
            bs("2.0"),
            bs("QUERY"),
            bs("3.0"),
            bs("4.0"),
        ])
        .unwrap();
        assert_eq!(c.count, 5);
        assert_eq!(c.ef, Some(100));
        assert_eq!(c.filter, Some("type=a".to_string()));
        assert_eq!(c.threshold, Some(0.5));
        assert_eq!(c.queries.len(), 2);
    }

    #[test]
    fn test_vsmsearch_no_query_is_error() {
        let r = VsMSearch::parse(&[bs("idx"), bs("COUNT"), bs("5")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_vsmsearch_to_resp_args() {
        let c = VsMSearch::parse(&[
            bs("idx"),
            bs("COUNT"),
            bs("5"),
            bs("QUERY"),
            bs("1.0"),
            bs("2.0"),
        ])
        .unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"idx"));
        assert!(args.contains(&Bytes::from_static(b"QUERY")));
        assert!(args.contains(&Bytes::from_static(b"COUNT")));
    }
}
