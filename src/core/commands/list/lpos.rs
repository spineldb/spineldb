// src/core/commands/list/lpos.rs

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

#[derive(Debug, Clone, Default)]
pub struct LPos {
    pub key: Bytes,
    pub element: Bytes,
    pub rank: Option<i64>,
    pub count: Option<u64>,
    pub max_len: Option<u64>,
}

impl ParseCommand for LPos {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("LPOS".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let element = extract_bytes(&args[1])?;

        let mut rank = None;
        let mut count = None;
        let mut max_len = None;

        let mut i = 2;
        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "rank" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    rank = Some(
                        extract_string(&args[i])?
                            .parse()
                            .map_err(|_| SpinelDBError::NotAnInteger)?,
                    );
                }
                "count" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let c: u64 = extract_string(&args[i])?
                        .parse()
                        .map_err(|_| SpinelDBError::NotAnInteger)?;

                    // Perilaku COUNT 0 adalah 'semua'.
                    // Representasikan secara internal sebagai u64::MAX agar loop
                    // tidak berhenti sampai akhir list.
                    count = if c == 0 { Some(u64::MAX) } else { Some(c) };
                }
                "maxlen" => {
                    i += 1;
                    if i >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    max_len = Some(
                        extract_string(&args[i])?
                            .parse()
                            .map_err(|_| SpinelDBError::NotAnInteger)?,
                    );
                }
                _ => return Err(SpinelDBError::SyntaxError),
            }
            i += 1;
        }

        Ok(LPos {
            key,
            element,
            rank,
            count,
            max_len,
        })
    }
}

#[async_trait]
impl ExecutableCommand for LPos {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Pertama, periksa apakah kunci ada dan merupakan list.
        let Some(entry) = shard_cache_guard.get_mut(&self.key) else {
            // Jika kunci tidak ada, hasilnya Null atau array kosong tergantung COUNT.
            return Ok((
                if self.count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                },
                WriteOutcome::DidNotWrite,
            ));
        };

        // Lakukan penghapusan pasif jika kunci kedaluwarsa.
        if entry.is_expired() {
            shard_cache_guard.pop(&self.key);
            return Ok((
                if self.count.is_some() {
                    RespValue::Array(vec![])
                } else {
                    RespValue::Null
                },
                WriteOutcome::DidNotWrite,
            ));
        }

        let DataValue::List(list) = &entry.data else {
            // Jika kunci ada tapi bukan list.
            return Err(SpinelDBError::WrongType);
        };

        let rank = self.rank.unwrap_or(1);
        let mut occurrences_found = 0i64;
        let mut positions = Vec::new();

        // Tentukan arah iterasi berdasarkan RANK (positif atau negatif).
        let iter: Box<dyn Iterator<Item = (usize, &Bytes)>> = if rank > 0 {
            Box::new(list.iter().enumerate())
        } else {
            Box::new(list.iter().enumerate().rev())
        };

        let mut comparisons = 0u64;
        for (i, v) in iter {
            // Cek MAXLEN jika ada.
            if let Some(ml) = self.max_len {
                if ml > 0 && comparisons >= ml {
                    break;
                }
                comparisons += 1;
            }

            if v == &self.element {
                occurrences_found += 1;

                // Periksa apakah kemunculan ini sesuai dengan RANK yang diminta.
                // rank.abs() digunakan karena rank negatif hanya mengubah arah pencarian.
                if occurrences_found >= rank.abs() {
                    if self.count.is_some() {
                        // Jika COUNT ada, kumpulkan posisinya.
                        positions.push(RespValue::Integer(i as i64));

                        // Hentikan jika sudah mencapai jumlah yang diminta oleh COUNT.
                        // `u64::MAX` (representasi COUNT 0) tidak akan pernah tercapai.
                        if let Some(c) = self.count {
                            if positions.len() as u64 >= c {
                                break;
                            }
                        }
                    } else {
                        // Jika COUNT tidak ada, kita hanya mencari rank pertama yang cocok.
                        // Langsung kembalikan posisinya.
                        return Ok((RespValue::Integer(i as i64), WriteOutcome::DidNotWrite));
                    }
                }
            }
        }

        // Jika loop selesai, kembalikan hasil berdasarkan apakah COUNT digunakan.
        if self.count.is_some() {
            Ok((RespValue::Array(positions), WriteOutcome::DidNotWrite))
        } else {
            // Loop selesai tapi rank yang cocok tidak ditemukan (jika tanpa COUNT).
            Ok((RespValue::Null, WriteOutcome::DidNotWrite))
        }
    }
}

impl CommandSpec for LPos {
    fn name(&self) -> &'static str {
        "lpos"
    }
    fn arity(&self) -> i64 {
        -3
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
        let mut args = vec![self.key.clone(), self.element.clone()];
        if let Some(r) = self.rank {
            args.extend([Bytes::from_static(b"RANK"), r.to_string().into()]);
        }
        if let Some(c) = self.count {
            // Konversi u64::MAX (representasi internal kita untuk COUNT 0)
            // kembali menjadi string "0" untuk AOF/Replikasi yang akurat.
            let count_str = if c == u64::MAX {
                "0".to_string()
            } else {
                c.to_string()
            };
            args.extend([Bytes::from_static(b"COUNT"), count_str.into()]);
        }
        if let Some(m) = self.max_len {
            args.extend([Bytes::from_static(b"MAXLEN"), m.to_string().into()]);
        }
        args
    }
}
