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

/// Implements the `VS.HYBRIDSEARCH` command: combined vector + BM25 search via RRF.
///
/// VS.HYBRIDSEARCH key VECTOR vec... TEXT text [COUNT count] [EF ef] [FILTER filter] [WEIGHTS wv wb]
#[derive(Debug, Clone, Default)]
pub struct VsHybridSearch {
    pub key: Bytes,
    pub query_vector: Vec<f32>,
    pub query_text: String,
    pub count: usize,
    pub ef: Option<usize>,
    pub filter: Option<String>,
    pub weight_vector: Option<f32>,
    pub weight_bm25: Option<f32>,
}

impl ParseCommand for VsHybridSearch {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 4 {
            return Err(SpinelDBError::WrongArgumentCount(
                "VS.HYBRIDSEARCH".to_string(),
            ));
        }

        let key = extract_bytes(&args[0])?;
        let mut i = 1;
        let mut query_vector = Vec::new();
        let mut query_text = None;
        let mut count = 10;
        let mut ef = None;
        let mut filter = None;
        let mut weight_vector = None;
        let mut weight_bm25 = None;

        // Parse VECTOR vec... TEXT text [options]
        while i < args.len() {
            let arg_str = extract_string(&args[i])?;
            let upper = arg_str.to_uppercase();

            match upper.as_str() {
                "VECTOR" => {
                    i += 1;
                    // Read floats until next keyword
                    while i < args.len() {
                        let peek = extract_string(&args[i])?;
                        let peek_upper = peek.to_uppercase();
                        if matches!(
                            peek_upper.as_str(),
                            "TEXT" | "COUNT" | "EF" | "FILTER" | "WEIGHTS"
                        ) {
                            break;
                        }
                        let val = peek.parse::<f32>().map_err(|_| SpinelDBError::NotAFloat)?;
                        query_vector.push(val);
                        i += 1;
                    }
                }
                "TEXT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.HYBRIDSEARCH".to_string(),
                        ));
                    }
                    query_text = Some(extract_string(&args[i])?);
                    i += 1;
                }
                "COUNT" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.HYBRIDSEARCH".to_string(),
                        ));
                    }
                    count = extract_string(&args[i])?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;
                    i += 1;
                }
                "EF" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.HYBRIDSEARCH".to_string(),
                        ));
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
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.HYBRIDSEARCH".to_string(),
                        ));
                    }
                    filter = Some(extract_string(&args[i])?);
                    i += 1;
                }
                "WEIGHTS" => {
                    i += 1;
                    if i + 1 >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount(
                            "VS.HYBRIDSEARCH".to_string(),
                        ));
                    }
                    weight_vector = Some(
                        extract_string(&args[i])?
                            .parse::<f32>()
                            .map_err(|_| SpinelDBError::NotAFloat)?,
                    );
                    i += 1;
                    weight_bm25 = Some(
                        extract_string(&args[i])?
                            .parse::<f32>()
                            .map_err(|_| SpinelDBError::NotAFloat)?,
                    );
                    i += 1;
                }
                _ => {
                    return Err(SpinelDBError::SyntaxError);
                }
            }
        }

        let query_text = query_text.ok_or_else(|| {
            SpinelDBError::WrongArgumentCount("VS.HYBRIDSEARCH: TEXT required".to_string())
        })?;

        if query_vector.is_empty() {
            return Err(SpinelDBError::InvalidRequest(
                "query vector must have at least one dimension".to_string(),
            ));
        }

        Ok(VsHybridSearch {
            key,
            query_vector,
            query_text,
            count,
            ef,
            filter,
            weight_vector,
            weight_bm25,
        })
    }
}

#[async_trait]
impl ExecutableCommand for VsHybridSearch {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_shard, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        let entry = shard_cache_guard
            .get_mut(&self.key)
            .ok_or(SpinelDBError::KeyNotFound)?;

        if let DataValue::SpinelVector(ref mut sv) = entry.data {
            let parsed_filter = if let Some(ref filter_expr) = self.filter {
                Some(
                    MetadataFilter::parse_expr(filter_expr)
                        .map_err(SpinelDBError::InvalidRequest)?,
                )
            } else {
                None
            };

            if let (Some(wv), Some(wb)) = (self.weight_vector, self.weight_bm25) {
                sv.set_hybrid_weights(wv, wb);
            }

            let results = sv
                .hybrid_search(
                    &self.query_vector,
                    &self.query_text,
                    self.count,
                    self.ef,
                    parsed_filter.as_ref(),
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

impl CommandSpec for VsHybridSearch {
    fn name(&self) -> &'static str {
        "vs.hybridsearch"
    }
    fn arity(&self) -> i64 {
        -4
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
        args.push(Bytes::from_static(b"VECTOR"));
        for &v in &self.query_vector {
            args.push(Bytes::from(v.to_string()));
        }
        args.push(Bytes::from_static(b"TEXT"));
        args.push(Bytes::from(self.query_text.clone()));
        if let Some(ef) = self.ef {
            args.push(Bytes::from_static(b"EF"));
            args.push(Bytes::from(ef.to_string()));
        }
        if let Some(ref f) = self.filter {
            args.push(Bytes::from_static(b"FILTER"));
            args.push(Bytes::from(f.clone()));
        }
        if let (Some(wv), Some(wb)) = (self.weight_vector, self.weight_bm25) {
            args.push(Bytes::from_static(b"WEIGHTS"));
            args.push(Bytes::from(wv.to_string()));
            args.push(Bytes::from(wb.to_string()));
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
    fn test_parse_hybridsearch_basic() {
        let c = VsHybridSearch::parse(&[
            bs("idx"),
            bs("VECTOR"),
            bs("1.0"),
            bs("2.0"),
            bs("TEXT"),
            bs("hello world"),
        ])
        .unwrap();
        assert_eq!(c.key, Bytes::from_static(b"idx"));
        assert_eq!(c.query_vector, vec![1.0, 2.0]);
        assert_eq!(c.query_text, "hello world");
        assert_eq!(c.count, 10);
    }

    #[test]
    fn test_parse_hybridsearch_with_weights() {
        let c = VsHybridSearch::parse(&[
            bs("idx"),
            bs("VECTOR"),
            bs("1.0"),
            bs("2.0"),
            bs("TEXT"),
            bs("query"),
            bs("WEIGHTS"),
            bs("0.7"),
            bs("0.3"),
            bs("COUNT"),
            bs("5"),
        ])
        .unwrap();
        assert_eq!(c.weight_vector, Some(0.7));
        assert_eq!(c.weight_bm25, Some(0.3));
        assert_eq!(c.count, 5);
    }

    #[test]
    fn test_parse_hybridsearch_missing_text_is_error() {
        let r = VsHybridSearch::parse(&[bs("idx"), bs("VECTOR"), bs("1.0"), bs("2.0")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_parse_hybridsearch_no_vector_is_error() {
        // Enough args to pass arity check, but TEXT before VECTOR → empty query_vector
        let r = VsHybridSearch::parse(&[bs("idx"), bs("TEXT"), bs("hello"), bs("COUNT"), bs("5")]);
        assert!(matches!(r, Err(SpinelDBError::InvalidRequest(_))));
    }

    #[test]
    fn test_parse_hybridsearch_too_few_args() {
        let r = VsHybridSearch::parse(&[bs("idx"), bs("VECTOR")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }
}
