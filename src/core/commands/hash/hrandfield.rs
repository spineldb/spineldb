// src/core/commands/hash/hrandfield.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use rand::seq::IteratorRandom;

#[derive(Debug, Clone, Default)]
pub struct HRandField {
    pub key: Bytes,
    pub count: Option<i64>,
    pub with_values: bool,
}

impl ParseCommand for HRandField {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("HRANDFIELD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let mut count = None;
        let mut with_values = false;
        let mut i = 1;

        // Logika parsing yang lebih fleksibel
        // Coba parse argumen kedua sebagai 'count' jika ada
        if i < args.len() {
            // Kita perlu memeriksa apakah argumen berikutnya adalah 'withvalues' atau angka.
            // Jika parsing ke i64 berhasil, kita anggap itu adalah 'count'.
            if let Ok(c) = extract_string(&args[i])?.parse::<i64>() {
                count = Some(c);
                i += 1;
            }
        }

        // Coba parse argumen berikutnya sebagai 'WITHVALUES'
        if i < args.len() {
            if extract_string(&args[i])?.eq_ignore_ascii_case("withvalues") {
                with_values = true;
                i += 1;
            } else {
                // Argumen tidak dikenal setelah key atau count
                return Err(SpinelDBError::SyntaxError);
            }
        }

        // Pastikan tidak ada argumen sisa setelah parsing
        if i != args.len() {
            return Err(SpinelDBError::SyntaxError);
        }

        // WITHVALUES tanpa COUNT tidak diperbolehkan oleh SpinelDB, jadi kita tambahkan validasi ini.
        // Namun, perintah HRANDFIELD key WITHVALUES valid dan count-nya 1.
        // Logika di atas sudah menangani ini dengan benar, jadi validasi ini sebenarnya tidak diperlukan.
        // Jika with_values true dan count-nya None, itu berarti count implisitnya 1.

        Ok(HRandField {
            key,
            count,
            with_values,
        })
    }
}

#[async_trait]
impl ExecutableCommand for HRandField {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;
        if let Some(entry) = shard_cache_guard.get_mut(&self.key) {
            if entry.is_expired() {
                shard_cache_guard.pop(&self.key);
                return Ok((
                    if self.with_values {
                        RespValue::Array(vec![])
                    } else {
                        RespValue::Null
                    },
                    WriteOutcome::DidNotWrite,
                ));
            }

            if let DataValue::Hash(hash) = &entry.data {
                if hash.is_empty() {
                    return Ok((
                        if self.count.is_some() {
                            RespValue::Array(vec![])
                        } else {
                            RespValue::Null
                        },
                        WriteOutcome::DidNotWrite,
                    ));
                }

                let mut rng = rand::thread_rng();
                match self.count {
                    None => {
                        // Kasus: HRANDFIELD key atau HRANDFIELD key WITHVALUES
                        let (field, value) = hash.iter().choose(&mut rng).unwrap();
                        if self.with_values {
                            Ok((
                                RespValue::Array(vec![
                                    RespValue::BulkString(field.clone()),
                                    RespValue::BulkString(value.clone()),
                                ]),
                                WriteOutcome::DidNotWrite,
                            ))
                        } else {
                            Ok((
                                RespValue::BulkString(field.clone()),
                                WriteOutcome::DidNotWrite,
                            ))
                        }
                    }
                    Some(count) => {
                        // Kasus: HRANDFIELD key <count> [WITHVALUES]
                        let mut results = Vec::new();
                        if count > 0 {
                            // Ambil 'count' field unik
                            let count_usize = count as usize;
                            let chosen_pairs: Vec<_> =
                                hash.iter().choose_multiple(&mut rng, count_usize);
                            for (field, value) in chosen_pairs {
                                if self.with_values {
                                    results.push(RespValue::BulkString(field.clone()));
                                    results.push(RespValue::BulkString(value.clone()));
                                } else {
                                    results.push(RespValue::BulkString(field.clone()));
                                }
                            }
                        } else {
                            // Ambil 'abs(count)' field, boleh duplikat
                            for _ in 0..count.abs() {
                                let (field, value) = hash.iter().choose(&mut rng).unwrap();
                                if self.with_values {
                                    results.push(RespValue::BulkString(field.clone()));
                                    results.push(RespValue::BulkString(value.clone()));
                                } else {
                                    results.push(RespValue::BulkString(field.clone()));
                                }
                            }
                        }

                        // Jika WITHVALUES, hasilnya adalah array datar [field1, value1, ...]
                        // Jika tidak, hasilnya adalah array dari field [field1, field2, ...]
                        Ok((RespValue::Array(results), WriteOutcome::DidNotWrite))
                    }
                }
            } else {
                Err(SpinelDBError::WrongType)
            }
        } else {
            Ok((
                if self.count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                },
                WriteOutcome::DidNotWrite,
            ))
        }
    }
}

// Implementasi CommandSpec tetap sama
impl CommandSpec for HRandField {
    fn name(&self) -> &'static str {
        "hrandfield"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY | CommandFlags::MOVABLEKEYS
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
        if let Some(c) = self.count {
            args.push(c.to_string().into());
        }
        if self.with_values {
            args.push("WITHVALUES".into());
        }
        args
    }
}
