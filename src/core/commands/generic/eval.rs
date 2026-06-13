// src/core/commands/generic/eval.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandExt, CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::database::{Db, ExecutionContext};
use crate::core::protocol::{RespFrame, RespValue};
use crate::core::{Command, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use mlua::IntoLua;
use mlua::prelude::*;
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Merges the write outcome from a `spinel.call` into the transaction's aggregated outcome.
fn update_aggregated_outcome(current_outcome: &RwLock<WriteOutcome>, new_outcome: WriteOutcome) {
    let mut current = current_outcome.write().unwrap_or_else(|e| e.into_inner());
    *current = current.merge(new_outcome);
}

/// Represents the EVAL command, which executes a Lua script.
///
/// # WARNING: Transaction Usage
///
/// Executing long-running or complex scripts inside a `MULTI`/`EXEC` transaction
/// can significantly impact server performance by holding locks for the entire
/// script's duration. It is recommended to use EVAL for short, fast operations
/// within transactions or to execute complex logic outside of a transaction block.
#[derive(Debug, Clone, Default)]
pub struct Eval {
    /// The Lua script to execute.
    pub script: Bytes,
    /// The number of keys passed to the script.
    pub num_keys: usize,
    /// The keys, which will be available in the script via the `KEYS` global table.
    pub keys: Vec<Bytes>,
    /// Additional arguments, available in the script via the `ARGV` global table.
    pub args: Vec<Bytes>,
}

impl ParseCommand for Eval {
    /// Parses the arguments for the EVAL command.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("EVAL".to_string()));
        }
        let script = extract_bytes(&args[0])?;
        let num_keys: usize = extract_string(&args[1])?.parse()?;

        let keys_start_index = 2;
        let keys_end_index = keys_start_index + num_keys;

        if args.len() < keys_end_index {
            return Err(SpinelDBError::InvalidState(
                "Number of keys specified is greater than the number of arguments provided.".into(),
            ));
        }

        let keys = args[keys_start_index..keys_end_index]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;
        let eval_args = args[keys_end_index..]
            .iter()
            .map(extract_bytes)
            .collect::<Result<_, _>>()?;

        Ok(Eval {
            script,
            num_keys,
            keys,
            args: eval_args,
        })
    }
}

#[async_trait]
impl ExecutableCommand for Eval {
    /// Executes the Lua script using the shared Lua VM.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let script = self.script.clone();
        let keys = self.keys.clone();
        let args = self.args.clone();
        let aggregated_outcome = Arc::new(RwLock::new(WriteOutcome::DidNotWrite));

        let server_state_clone = Arc::clone(&ctx.state);
        let db_clone: Arc<Db> = ctx.db.clone().into();
        let session_id = ctx.session_id;
        let authenticated_user = ctx.authenticated_user.clone();

        let (timeout_duration, memory_limit_mb) = {
            let config = ctx.state.config.lock().await;
            (
                Duration::from_millis(config.safety.script_timeout_ms),
                config.safety.script_memory_limit_mb,
            )
        };

        // Drop the shard locks acquired by the EVAL command itself before entering
        // the blocking Lua execution. The Lua script will re-acquire the necessary
        // locks via `spinel.call()` / `spinel.pcall()`. Holding the outer locks
        // would deadlock because `tokio::sync::Mutex` is not re-entrant.
        let original_locks = std::mem::replace(
            &mut ctx.locks,
            crate::core::database::locking::ExecutionLocks::None,
        );

        enum LockKind {
            None,
            Single(usize),
            Multi,
            All,
        }

        let lock_kind = match &original_locks {
            crate::core::database::locking::ExecutionLocks::None => LockKind::None,
            crate::core::database::locking::ExecutionLocks::Single { shard_index, .. } => {
                LockKind::Single(*shard_index)
            }
            crate::core::database::locking::ExecutionLocks::Multi { .. } => LockKind::Multi,
            crate::core::database::locking::ExecutionLocks::All { .. } => LockKind::All,
        };
        drop(original_locks);

        let script_has_timeout = timeout_duration.as_millis() > 0;
        let script_has_mem_limit = memory_limit_mb > 0;

        // Clone the Lua manager handle. With the VM pool, each `EVAL`
        // grabs its own `Lua` from the pool inside `spawn_blocking`, so
        // multiple scripts can now execute concurrently instead of being
        // serialized on a single mutex.
        let lua_manager = ctx.state.scripting.clone();

        // The Lua execution must happen within a dedicated blocking thread because
        // `mlua::Lua` is not `Send`. The guard returned by `acquire()` owns
        // the VM for the duration of the closure; on drop it is returned to
        // the pool.
        let lua_future =
            tokio::task::spawn_blocking(move || -> mlua::Result<(RespValue, WriteOutcome)> {
                let mut guard = lua_manager.acquire();
                let lua = guard.lua();

                // Enforce memory limit if configured.
                if script_has_mem_limit {
                    // `set_memory_limit` expects bytes.
                    let limit_in_bytes = memory_limit_mb * 1024 * 1024;
                    if let Err(e) = lua.set_memory_limit(limit_in_bytes) {
                        return Err(mlua::Error::external(SpinelDBError::Internal(format!(
                            "Failed to set Lua memory limit: {e}"
                        ))));
                    }
                }

                let res: mlua::Result<(RespValue, WriteOutcome)> = Result::Ok({
                    let globals = lua.globals();

                    // Sandbox the Lua environment by removing potentially dangerous functions.
                    globals.set("loadfile", mlua::Value::Nil)?;
                    globals.set("dofile", mlua::Value::Nil)?;
                    globals.set("collectgarbage", mlua::Value::Nil)?;
                    if let Ok(mlua::Value::Table(os_table)) = globals.get::<mlua::Value>("os") {
                        os_table.set("execute", mlua::Value::Nil)?;
                        os_table.set("exit", mlua::Value::Nil)?;
                    }
                    if let Ok(mlua::Value::Table(io_table)) = globals.get::<mlua::Value>("io") {
                        io_table.set("open", mlua::Value::Nil)?;
                        io_table.set("popen", mlua::Value::Nil)?;
                    }

                    // Create the `spinel` table to expose the database API.
                    let spinel_table = lua.create_table()?;

                    // Provide `spinel.call` to execute commands that will propagate errors.
                    let call_state = Arc::clone(&server_state_clone);
                    let call_db = Arc::clone(&db_clone);
                    let call_session_id = session_id;
                    let call_user = authenticated_user.clone();
                    let call_aggregated_outcome = Arc::clone(&aggregated_outcome);
                    let call_callback =
                        lua.create_async_function(move |lua, m_args: mlua::MultiValue| {
                            let state = Arc::clone(&call_state);
                            let db = Arc::clone(&call_db);
                            let aggregated_outcome = Arc::clone(&call_aggregated_outcome);
                            let user = call_user.clone();
                            async move {
                                let mut resp_args = Vec::new();
                                for val in m_args.into_vec() {
                                    resp_args.push(lua_value_to_resp_frame(val)?);
                                }
                                let command = Command::try_from(RespFrame::Array(resp_args))?;
                                let mut temp_ctx = ExecutionContext {
                                    state,
                                    locks: db.determine_locks_for_command(&command).await,
                                    db: &db,
                                    command: Some(command.clone()),
                                    session_id: call_session_id,
                                    authenticated_user: user,
                                };
                                let (resp_val, outcome) = command.execute(&mut temp_ctx).await?;
                                update_aggregated_outcome(&aggregated_outcome, outcome);
                                resp_value_to_lua_value(&lua, resp_val)
                            }
                        })?;
                    spinel_table.set("call", call_callback)?;

                    // Provide `spinel.pcall` to execute commands and capture errors.
                    let pcall_state = Arc::clone(&server_state_clone);
                    let pcall_db = Arc::clone(&db_clone);
                    let pcall_session_id = session_id;
                    let pcall_user = authenticated_user.clone();
                    let pcall_aggregated_outcome = Arc::clone(&aggregated_outcome);
                    let pcall_callback =
                        lua.create_async_function(move |lua, m_args: mlua::MultiValue| {
                            let state = Arc::clone(&pcall_state);
                            let db = Arc::clone(&pcall_db);
                            let aggregated_outcome = Arc::clone(&pcall_aggregated_outcome);
                            let user = pcall_user.clone();
                            async move {
                                let mut resp_args = Vec::new();
                                for val in m_args.into_vec() {
                                    resp_args.push(lua_value_to_resp_frame(val)?);
                                }
                                let command = Command::try_from(RespFrame::Array(resp_args))?;
                                let mut temp_ctx = ExecutionContext {
                                    state,
                                    locks: db.determine_locks_for_command(&command).await,
                                    db: &db,
                                    command: Some(command.clone()),
                                    session_id: pcall_session_id,
                                    authenticated_user: user,
                                };
                                match command.execute(&mut temp_ctx).await {
                                    Ok((resp_val, outcome)) => {
                                        update_aggregated_outcome(&aggregated_outcome, outcome);
                                        resp_value_to_lua_value(&lua, resp_val)
                                    }
                                    Err(e) => Ok(LuaValue::Table(lua_error_to_table(&lua, e)?)),
                                }
                            }
                        })?;
                    spinel_table.set("pcall", pcall_callback)?;

                    globals.set("spinel", spinel_table)?;

                    // Expose the KEYS table to the script.
                    let keys_table = lua.create_table()?;
                    for (i, k) in keys.iter().enumerate() {
                        keys_table.set(i + 1, lua.create_string(k)?)?;
                    }
                    globals.set("KEYS", keys_table)?;

                    // Expose the ARGV table to the script.
                    let argv_table = lua.create_table()?;
                    for (i, a) in args.iter().enumerate() {
                        argv_table.set(i + 1, lua.create_string(a)?)?;
                    }
                    globals.set("ARGV", argv_table)?;

                    drop(globals);

                    // Execute the async Lua script using the handle of the main Tokio runtime.
                    // This avoids creating a nested runtime, which is a major anti-pattern.
                    let result = tokio::runtime::Handle::current().block_on(async {
                        let lua_future = lua.load(&*script).eval_async::<LuaValue>();

                        if script_has_timeout {
                            match tokio::time::timeout(timeout_duration, lua_future).await {
                                Ok(Ok(val)) => Ok(val),
                                Ok(Err(e)) => Err(e),
                                Err(_) => Err(mlua::Error::external(SpinelDBError::ScriptTimeout)),
                            }
                        } else {
                            lua_future.await
                        }
                    })?;

                    let resp_value = lua_value_to_resp_value(result)?;
                    (resp_value, *aggregated_outcome.read().unwrap())
                });

                res
            });

        let result = match lua_future.await {
            Ok(Ok(res)) => Ok(res),
            Ok(Err(e)) => {
                // Check if the error is due to memory limit.
                if let LuaError::MemoryError(_) = e {
                    Err(SpinelDBError::MaxMemoryReached)
                } else {
                    Err(SpinelDBError::from(e))
                }
            }
            Err(join_err) => Err(SpinelDBError::Internal(format!(
                "Lua execution task panicked: {join_err}"
            ))),
        };

        // Restore or re-acquire locks to satisfy the ExecutionContext's post-condition,
        // which is critical for TransactionHandler and general consistency.
        ctx.locks = match lock_kind {
            LockKind::None => crate::core::database::locking::ExecutionLocks::None,
            LockKind::Multi => crate::core::database::locking::ExecutionLocks::Multi {
                guards: ctx.db.lock_shards_for_keys(&self.keys).await,
            },
            LockKind::Single(shard_index) => {
                crate::core::database::locking::ExecutionLocks::Single {
                    shard_index,
                    guard: ctx.db.get_shard(shard_index).entries.lock().await,
                }
            }
            LockKind::All => crate::core::database::locking::ExecutionLocks::All {
                guards: ctx.db.lock_all_shards().await,
            },
        };

        result
    }
}

impl CommandSpec for Eval {
    fn name(&self) -> &'static str {
        "eval"
    }

    fn arity(&self) -> i64 {
        -3
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::SCRIPTING
    }

    fn first_key(&self) -> i64 {
        3
    }

    fn last_key(&self) -> i64 {
        3 + self.num_keys as i64 - 1
    }

    fn step(&self) -> i64 {
        1
    }

    fn get_keys(&self) -> Vec<Bytes> {
        self.keys.clone()
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = Vec::with_capacity(2 + self.keys.len() + self.args.len());
        args.push(self.script.clone());
        args.push(Bytes::from(self.num_keys.to_string()));
        args.extend_from_slice(&self.keys);
        args.extend_from_slice(&self.args);
        args
    }
}

// --- Type Conversion Helpers ---

/// Converts a `LuaValue` to a `RespFrame` for command execution.
fn lua_value_to_resp_frame(lua_val: LuaValue) -> mlua::Result<RespFrame> {
    match lua_val {
        LuaValue::String(s) => Ok(RespFrame::BulkString(Bytes::copy_from_slice(&s.as_bytes()))),
        LuaValue::Integer(i) => Ok(RespFrame::Integer(i)),
        LuaValue::Number(n) => Ok(RespFrame::BulkString(n.to_string().into())),
        _ => Err(mlua::Error::FromLuaConversionError {
            from: lua_val.type_name(),
            to: "RespFrame".to_string(),
            message: Some("Unsupported type conversion".to_string()),
        }),
    }
}

/// Converts a `LuaValue` to a `RespValue` for the final client response.
fn lua_value_to_resp_value(lua_val: LuaValue) -> mlua::Result<RespValue> {
    match lua_val {
        LuaValue::String(s) => Ok(RespValue::BulkString(Bytes::copy_from_slice(&s.as_bytes()))),
        LuaValue::Integer(i) => Ok(RespValue::Integer(i)),
        LuaValue::Number(n) => Ok(RespValue::BulkString(n.to_string().into())),
        LuaValue::Boolean(b) => Ok(RespValue::Integer(b as i64)),
        mlua::Value::Nil => Ok(RespValue::Null),
        LuaValue::Table(t) => {
            // Check if the table is a sequence (array-like) or a hash (map-like).
            // A sequence has consecutive integer keys starting from 1.
            let mut array_items = Vec::new();
            let mut map_items = Vec::new();
            let mut max_index: i64 = 0;
            let mut count: i64 = 0;

            for pair in t.clone().pairs::<LuaValue, LuaValue>() {
                let (k, v) = pair?;
                let resp_k = lua_value_to_resp_value(k.clone())?;
                let resp_v = lua_value_to_resp_value(v)?;
                count += 1;

                match &resp_k {
                    RespValue::Integer(idx) if *idx >= 1 => {
                        if *idx > max_index {
                            max_index = *idx;
                        }
                        // Store for potential array conversion
                        array_items.push((Some(*idx), resp_v));
                    }
                    _ => {
                        // Non-integer key → definitely a map
                        map_items.push((resp_k, resp_v));
                    }
                }
            }

            // If there are map items, or the array is not a perfect sequence,
            // return as Map. Otherwise, return as Array.
            if !map_items.is_empty() || max_index != count {
                // Has non-integer keys or gaps → Map
                let mut entries: Vec<(RespValue, RespValue)> = Vec::with_capacity(count as usize);
                // Re-build the full map from the table
                entries.clear();
                for pair in t.pairs::<LuaValue, LuaValue>() {
                    let (k, v) = pair?;
                    entries.push((lua_value_to_resp_value(k)?, lua_value_to_resp_value(v)?));
                }
                Ok(RespValue::Map(entries))
            } else {
                // Pure sequence → Array
                Ok(RespValue::Array(
                    array_items.into_iter().map(|(_, v)| v).collect(),
                ))
            }
        }
        _ => Err(mlua::Error::FromLuaConversionError {
            from: lua_val.type_name(),
            to: "RespValue".to_string(),
            message: Some("Unsupported type conversion".to_string()),
        }),
    }
}

/// Converts a `RespValue` from a command result back into a `LuaValue`.
fn resp_value_to_lua_value(lua: &Lua, resp_val: RespValue) -> mlua::Result<LuaValue> {
    match resp_val {
        RespValue::SimpleString(s) => Ok(LuaValue::String(lua.create_string(s.as_bytes())?)),
        RespValue::BulkString(b) => Ok(LuaValue::String(lua.create_string(&b)?)),
        RespValue::Integer(i) => i.into_lua(lua),
        RespValue::Null => Ok(mlua::Value::Nil),
        RespValue::NullArray => Ok(LuaValue::Boolean(false)),
        RespValue::Error(e) => {
            let err_table = lua.create_table()?;
            err_table.set("err", e)?;
            Ok(LuaValue::Table(err_table))
        }
        RespValue::Array(arr) => {
            let table = lua.create_table_with_capacity(arr.len(), 0)?;
            for (i, item) in arr.into_iter().enumerate() {
                table.set(i + 1, resp_value_to_lua_value(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        // RESP3 types - convert to their best-effort Lua equivalents
        RespValue::Boolean(b) => Ok(LuaValue::Boolean(b)),
        RespValue::Double(d) => Ok(LuaValue::Number(d)),
        RespValue::BigNumber(s) => Ok(LuaValue::String(lua.create_string(s.as_bytes())?)),
        RespValue::Map(m) => {
            let table = lua.create_table_with_capacity(0, m.len())?;
            for (k, v) in m {
                let key = resp_value_to_lua_value(lua, k)?;
                let val = resp_value_to_lua_value(lua, v)?;
                table.set(key, val)?;
            }
            Ok(LuaValue::Table(table))
        }
        RespValue::Set(s) => {
            let table = lua.create_table_with_capacity(s.len(), 0)?;
            for (i, item) in s.into_iter().enumerate() {
                table.set(i + 1, resp_value_to_lua_value(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        RespValue::Push(p) => {
            let table = lua.create_table_with_capacity(p.len(), 0)?;
            for (i, item) in p.into_iter().enumerate() {
                table.set(i + 1, resp_value_to_lua_value(lua, item)?)?;
            }
            Ok(LuaValue::Table(table))
        }
        RespValue::VerbatimString(_fmt, data) => Ok(LuaValue::String(lua.create_string(&data)?)),
        RespValue::Attribute(_attr, inner) => resp_value_to_lua_value(lua, *inner),
    }
}

/// Converts a `SpinelDBError` into a Lua table for `spinel.pcall`.
fn lua_error_to_table(lua: &Lua, error: SpinelDBError) -> mlua::Result<LuaTable> {
    let table = lua.create_table()?;
    table.set("err", error.to_string())?;
    Ok(table)
}

impl From<SpinelDBError> for mlua::Error {
    fn from(e: SpinelDBError) -> Self {
        mlua::Error::external(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &'static str) -> RespFrame {
        RespFrame::BulkString(Bytes::from_static(s.as_bytes()))
    }

    #[test]
    fn test_eval_parse_script_and_num_keys() -> Result<(), SpinelDBError> {
        let c = Eval::parse(&[bs("return 1"), bs("0")]).unwrap();
        assert_eq!(c.script, Bytes::from_static(b"return 1"));
        assert_eq!(c.num_keys, 0);
        assert!(c.keys.is_empty());
        Ok(())
    }

    #[test]
    fn test_eval_parse_with_keys_and_args() -> Result<(), SpinelDBError> {
        let c = Eval::parse(&[
            bs("return KEYS[1]"),
            bs("1"),     // num_keys
            bs("mykey"), // key 1
            bs("arg1"),  // arg 1
            bs("arg2"),  // arg 2
        ])
        .unwrap();
        assert_eq!(c.num_keys, 1);
        assert_eq!(c.keys[0].as_ref(), b"mykey");
        assert_eq!(c.args.len(), 2);
        Ok(())
    }

    #[test]
    fn test_eval_parse_too_few_args() {
        let r = Eval::parse(&[bs("script")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_eval_parse_too_few_keys() {
        let r = Eval::parse(&[bs("script"), bs("2"), bs("key1")]); // num_keys=2 but only 1 key
        assert!(matches!(r, Err(SpinelDBError::InvalidState(_))));
    }

    #[test]
    fn test_eval_to_resp_args_round_trips() -> Result<(), SpinelDBError> {
        let c = Eval::parse(&[bs("return 1"), bs("1"), bs("key")])?;
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"return 1"));
        assert_eq!(args[1], Bytes::from_static(b"1"));
        Ok(())
    }
}
