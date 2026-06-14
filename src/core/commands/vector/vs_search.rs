// src/core/commands/vector/vs_search.rs

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

/// Implements the `VS.SEARCH` command for k-nearest neighbor search.
///
/// VS.SEARCH key vector [EF ef] [COUNT count] [FILTER filter] [THRESHOLD threshold]
#[derive(Debug, Clone, Default)]
pub struct VsSearch {
    pub key: Bytes,
    pub query: Vec<f32>,
    pub count: usize,
    pub ef: Option<usize>,
    pub filter: Option<String>,
    pub threshold: Option<f32>,
}

impl ParseCommand for VsSearch {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("VS.SEARCH".to_string()));
        }

        let key = extract_bytes(&args[0])?;

        // Parse query vector values until we hit a keyword
        let mut query = Vec::new();
        let mut i = 1;
        let mut count = 10; // default
        let mut ef = None;
        let mut filter = None;
        let mut threshold = None;

        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            if upper == "EF" {
                i += 1;
                if i >= args.len() {
                    return Err(SpinelDBError::WrongArgumentCount("VS.SEARCH".to_string()));
                }
                ef = Some(
                    extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?,
                );
                i += 1;
                continue;
            } else if upper == "COUNT" {
                i += 1;
                if i >= args.len() {
                    return Err(SpinelDBError::WrongArgumentCount("VS.SEARCH".to_string()));
                }
                count = extract_string(&args[i])?
                    .parse::<usize>()
                    .map_err(|_| SpinelDBError::NotAnInteger)?;
                i += 1;
                continue;
            } else if upper == "FILTER" {
                i += 1;
                if i >= args.len() {
                    return Err(SpinelDBError::WrongArgumentCount("VS.SEARCH".to_string()));
                }
                filter = Some(extract_string(&args[i])?);
                i += 1;
                continue;
            } else if upper == "THRESHOLD" {
                i += 1;
                if i >= args.len() {
                    return Err(SpinelDBError::WrongArgumentCount("VS.SEARCH".to_string()));
                }
                threshold = Some(
                    extract_string(&args[i])?
                        .parse::<f32>()
                        .map_err(|_| SpinelDBError::NotAFloat)?,
                );
                i += 1;
                continue;
            }

            // Try to parse as float
            let val = arg_str
                .parse::<f32>()
                .map_err(|_| SpinelDBError::NotAFloat)?;
            query.push(val);
            i += 1;
        }

        if query.is_empty() {
            return Err(SpinelDBError::InvalidRequest(
                "query vector must have at least one dimension".to_string(),
            ));
        }

        Ok(VsSearch {
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
impl ExecutableCommand for VsSearch {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref sv) = entry.data {
            // Parse filter if provided
            let parsed_filter = if let Some(ref filter_expr) = self.filter {
                Some(
                    MetadataFilter::parse_expr(filter_expr)
                        .map_err(SpinelDBError::InvalidRequest)?,
                )
            } else {
                None
            };

            let results = sv
                .search_with_filter(
                    &self.query,
                    self.count,
                    self.ef,
                    parsed_filter.as_ref(),
                    self.threshold,
                )
                .map_err(SpinelDBError::InvalidRequest)?;

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

            Ok((RespValue::Array(resp_results), WriteOutcome::DidNotWrite))
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for VsSearch {
    fn name(&self) -> &'static str {
        "vs.search"
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
    fn test_vssearch_parse_valid() {
        let c = VsSearch::parse(&[bs("idx"), bs("1.0"), bs("2.0"), bs("3.0")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.query, vec![1.0, 2.0, 3.0]);
        assert_eq!(c.count, 10); // default
        assert!(c.ef.is_none());
        assert!(c.filter.is_none());
        assert!(c.threshold.is_none());
    }

    #[test]
    fn test_vssearch_parse_with_count() {
        let c = VsSearch::parse(&[bs("idx"), bs("1.0"), bs("2.0"), bs("COUNT"), bs("5")]).unwrap();
        assert_eq!(c.count, 5);
    }

    #[test]
    fn test_vssearch_parse_with_ef() {
        let c = VsSearch::parse(&[
            bs("idx"),
            bs("1.0"),
            bs("2.0"),
            bs("EF"),
            bs("100"),
            bs("COUNT"),
            bs("5"),
        ])
        .unwrap();
        assert_eq!(c.ef, Some(100));
        assert_eq!(c.count, 5);
    }

    #[test]
    fn test_vssearch_parse_with_filter() {
        let c = VsSearch::parse(&[
            bs("idx"),
            bs("1.0"),
            bs("2.0"),
            bs("FILTER"),
            bs("category=cat"),
        ])
        .unwrap();
        assert_eq!(c.filter, Some("category=cat".to_string()));
    }

    #[test]
    fn test_vssearch_parse_with_threshold() {
        let c = VsSearch::parse(&[bs("idx"), bs("1.0"), bs("2.0"), bs("THRESHOLD"), bs("0.5")])
            .unwrap();
        assert_eq!(c.threshold, Some(0.5));
    }

    #[test]
    fn test_vssearch_parse_all_params() {
        let c = VsSearch::parse(&[
            bs("idx"),
            bs("1.0"),
            bs("2.0"),
            bs("EF"),
            bs("100"),
            bs("COUNT"),
            bs("5"),
            bs("FILTER"),
            bs("type=a"),
            bs("THRESHOLD"),
            bs("0.8"),
        ])
        .unwrap();
        assert_eq!(c.ef, Some(100));
        assert_eq!(c.count, 5);
        assert_eq!(c.filter, Some("type=a".to_string()));
        assert_eq!(c.threshold, Some(0.8));
    }

    #[test]
    fn test_vssearch_too_few_args() {
        let r = VsSearch::parse(&[bs("idx")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vssearch_to_resp_args() {
        let c = VsSearch::parse(&[bs("idx"), bs("1.0"), bs("2.0"), bs("COUNT"), bs("5")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"idx"));
        assert!(args.contains(&Bytes::from_static(b"COUNT")));
    }
}
