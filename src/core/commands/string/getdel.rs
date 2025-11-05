// src/core/commands/string/getdel.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, validate_arg_count};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::DataValue;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct GetDel {
    pub key: Bytes,
}

impl ParseCommand for GetDel {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 1, "GETDEL")?;
        Ok(GetDel {
            key: extract_bytes(&args[0])?,
        })
    }
}

#[async_trait]
impl ExecutableCommand for GetDel {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Gunakan helper yang konsisten untuk mendapatkan akses ke shard.
        let (_, shard_cache_guard) = ctx.get_single_shard_context_mut()?;

        // Coba hapus kunci dan dapatkan nilai lamanya dalam satu operasi.
        // `pop` di ShardCache sudah dirancang untuk ini.
        let Some(old_value) = shard_cache_guard.pop(&self.key) else {
            // Jika `pop` mengembalikan None, berarti kuncinya tidak ada.
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        };

        // Jika kunci ada tapi sudah kedaluwarsa, `pop` akan tetap menghapusnya,
        // tapi kita harus mengembalikan Null sesuai perilaku SpinelDB.
        if old_value.is_expired() {
            return Ok((RespValue::Null, WriteOutcome::DidNotWrite));
        }

        // Pastikan tipe data yang dihapus adalah String.
        let response = match old_value.data {
            DataValue::String(s) => RespValue::BulkString(s),
            // Jika tipe datanya salah, ini seharusnya tidak terjadi jika validasi
            // tipe dilakukan sebelum operasi tulis. Namun, sebagai pengaman,
            // kita kembalikan error.
            _ => return Err(SpinelDBError::WrongType),
        };

        // Karena `pop` berhasil dan kuncinya valid, ini adalah operasi penghapusan.
        Ok((response, WriteOutcome::Delete { keys_deleted: 1 }))
    }
}

impl CommandSpec for GetDel {
    fn name(&self) -> &'static str {
        "getdel"
    }
    fn arity(&self) -> i64 {
        2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::MOVABLEKEYS
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
        vec![self.key.clone()]
    }
}
