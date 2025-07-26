// src/core/commands/generic/flushdb.rs

use crate::core::SpinelDBError;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::validate_arg_count;
use crate::core::protocol::{RespFrame, RespValue};
use crate::core::storage::db::ExecutionContext;
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct FlushDb;

impl ParseCommand for FlushDb {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        validate_arg_count(args, 0, "FLUSHDB")?;
        Ok(FlushDb)
    }
}

#[async_trait]
impl ExecutableCommand for FlushDb {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Cukup panggil clear_all_shards pada DB saat ini.
        ctx.db.clear_all_shards().await;

        // Meskipun hanya DB saat ini yang di-flush, SpinelDB menyebarkannya
        // sebagai FLUSHDB juga (bukan FLUSHALL).
        Ok((RespValue::SimpleString("OK".into()), WriteOutcome::Flush))
    }
}

impl CommandSpec for FlushDb {
    fn name(&self) -> &'static str {
        "flushdb"
    }
    fn arity(&self) -> i64 {
        1
    }
    // Flagnya sama seperti FLUSHALL, karena ini adalah operasi tulis besar.
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
    }
    fn first_key(&self) -> i64 {
        0
    }
    fn last_key(&self) -> i64 {
        0
    }
    fn step(&self) -> i64 {
        0
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        vec![]
    }
}
