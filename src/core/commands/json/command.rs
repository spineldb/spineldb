// src/core/commands/json/command.rs

//! The main dispatcher for all `JSON.*` subcommands.

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::extract_string;
use crate::core::protocol::RespFrame;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

// Import the concrete implementations for each subcommand.
use super::json_arrappend::JsonArrAppend;
use super::json_arrindex::JsonArrIndex;
use super::json_arrinsert::JsonArrInsert;
use super::json_arrlen::JsonArrLen;
use super::json_arrpop::JsonArrPop;
use super::json_arrtrim::JsonArrTrim;
use super::json_clear::JsonClear; // ADDED THIS LINE
use super::json_del::JsonDel;
use super::json_get::JsonGet;
use super::json_merge::JsonMerge;
use super::json_mget::JsonMGet;
use super::json_numincrby::JsonNumIncrBy;
use super::json_nummultby::JsonNumMultBy;
use super::json_objkeys::JsonObjKeys;
use super::json_objlen::JsonObjLen;
use super::json_set::JsonSet;
use super::json_strappend::JsonStrAppend;
use super::json_strlen::JsonStrLen;
use super::json_toggle::JsonToggle;
use super::json_type::JsonType; // ADDED THIS LINE

/// Enum to hold all possible parsed `JSON` subcommands.
#[derive(Debug, Clone)]
pub enum JsonSubcommand {
    Set(JsonSet),
    Get(JsonGet),
    Del(JsonDel),
    ArrAppend(JsonArrAppend),
    Type(JsonType),
    ObjLen(JsonObjLen),
    ArrLen(JsonArrLen),
    NumIncrBy(JsonNumIncrBy),
    ObjKeys(JsonObjKeys),
    ArrInsert(JsonArrInsert),
    ArrPop(JsonArrPop),
    StrLen(JsonStrLen),
    ArrTrim(JsonArrTrim),
    ArrIndex(JsonArrIndex),
    MGet(JsonMGet),
    StrAppend(JsonStrAppend),
    Toggle(JsonToggle),
    NumMultBy(JsonNumMultBy),
    Clear(JsonClear), // ADDED THIS LINE
    Merge(JsonMerge), // ADDED THIS LINE
}

/// The main `Json` command struct that holds a specific subcommand.
/// This acts as the top-level entry point for `JSON.*` commands.
#[derive(Debug, Clone)]
pub struct Json {
    pub subcommand: JsonSubcommand,
}

impl Default for Json {
    /// Provides a default variant, required for the `get_all_command_specs` function.
    fn default() -> Self {
        Self {
            subcommand: JsonSubcommand::Get(JsonGet::default()),
        }
    }
}

impl ParseCommand for Json {
    /// Parses the initial RESP frame array to determine which `JSON` subcommand to use.
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("JSON".to_string()));
        }

        let sub_str = extract_string(&args[0])?.to_ascii_lowercase();
        let command_args = &args[1..];

        // Delegate parsing to the specific subcommand's implementation.
        let subcommand = match sub_str.as_str() {
            "set" => JsonSubcommand::Set(JsonSet::parse(command_args)?),
            "get" => JsonSubcommand::Get(JsonGet::parse(command_args)?),
            "del" | "forget" => JsonSubcommand::Del(JsonDel::parse(command_args)?),
            "arrappend" => JsonSubcommand::ArrAppend(JsonArrAppend::parse(command_args)?),
            "type" => JsonSubcommand::Type(JsonType::parse(command_args)?),
            "objlen" => JsonSubcommand::ObjLen(JsonObjLen::parse(command_args)?),
            "arrlen" => JsonSubcommand::ArrLen(JsonArrLen::parse(command_args)?),
            "numincrby" => JsonSubcommand::NumIncrBy(JsonNumIncrBy::parse(command_args)?),
            "objkeys" => JsonSubcommand::ObjKeys(JsonObjKeys::parse(command_args)?),
            "arrinsert" => JsonSubcommand::ArrInsert(JsonArrInsert::parse(command_args)?),
            "arrpop" => JsonSubcommand::ArrPop(JsonArrPop::parse(command_args)?),
            "strlen" => JsonSubcommand::StrLen(JsonStrLen::parse(command_args)?),
            "arrtrim" => JsonSubcommand::ArrTrim(JsonArrTrim::parse(command_args)?),
            "arrindex" => JsonSubcommand::ArrIndex(JsonArrIndex::parse(command_args)?),
            "mget" => JsonSubcommand::MGet(JsonMGet::parse(command_args)?),
            "strappend" => JsonSubcommand::StrAppend(JsonStrAppend::parse(command_args)?),
            "toggle" => JsonSubcommand::Toggle(JsonToggle::parse(command_args)?),
            "nummultby" => JsonSubcommand::NumMultBy(JsonNumMultBy::parse(command_args)?),
            "clear" => JsonSubcommand::Clear(JsonClear::parse(command_args)?), // ADDED THIS LINE
            "merge" => JsonSubcommand::Merge(JsonMerge::parse(command_args)?), // ADDED THIS LINE
            _ => return Err(SpinelDBError::UnknownCommand(format!("JSON {sub_str}"))),
        };

        Ok(Json { subcommand })
    }
}

#[async_trait]
impl ExecutableCommand for Json {
    /// Dispatches execution to the specific subcommand's implementation.
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            JsonSubcommand::Set(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::Get(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::Del(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ArrAppend(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::Type(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ObjLen(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ArrLen(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::NumIncrBy(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ObjKeys(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ArrInsert(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ArrPop(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::StrLen(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ArrTrim(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::ArrIndex(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::MGet(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::StrAppend(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::Toggle(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::NumMultBy(cmd) => cmd.execute(ctx).await,
            JsonSubcommand::Clear(cmd) => cmd.execute(ctx).await, // ADDED THIS LINE
            JsonSubcommand::Merge(cmd) => cmd.execute(ctx).await, // ADDED THIS LINE
        }
    }
}

impl CommandSpec for Json {
    fn name(&self) -> &'static str {
        "json"
    }

    fn arity(&self) -> i64 {
        // Arity is variable; delegate to the specific subcommand.
        match &self.subcommand {
            JsonSubcommand::Set(cmd) => cmd.arity(),
            JsonSubcommand::Get(cmd) => cmd.arity(),
            JsonSubcommand::Del(cmd) => cmd.arity(),
            JsonSubcommand::ArrAppend(cmd) => cmd.arity(),
            JsonSubcommand::Type(cmd) => cmd.arity(),
            JsonSubcommand::ObjLen(cmd) => cmd.arity(),
            JsonSubcommand::ArrLen(cmd) => cmd.arity(),
            JsonSubcommand::NumIncrBy(cmd) => cmd.arity(),
            JsonSubcommand::ObjKeys(cmd) => cmd.arity(),
            JsonSubcommand::ArrInsert(cmd) => cmd.arity(),
            JsonSubcommand::ArrPop(cmd) => cmd.arity(),
            JsonSubcommand::StrLen(cmd) => cmd.arity(),
            JsonSubcommand::ArrTrim(cmd) => cmd.arity(),
            JsonSubcommand::ArrIndex(cmd) => cmd.arity(),
            JsonSubcommand::MGet(cmd) => cmd.arity(),
            JsonSubcommand::StrAppend(cmd) => cmd.arity(),
            JsonSubcommand::Toggle(cmd) => cmd.arity(),
            JsonSubcommand::NumMultBy(cmd) => cmd.arity(),
            JsonSubcommand::Clear(cmd) => cmd.arity(), // ADDED THIS LINE
            JsonSubcommand::Merge(cmd) => cmd.arity(), // ADDED THIS LINE
        }
    }

    fn flags(&self) -> CommandFlags {
        // Inherit flags from the specific subcommand.
        match &self.subcommand {
            JsonSubcommand::Set(cmd) => cmd.flags(),
            JsonSubcommand::Get(cmd) => cmd.flags(),
            JsonSubcommand::Del(cmd) => cmd.flags(),
            JsonSubcommand::ArrAppend(cmd) => cmd.flags(),
            JsonSubcommand::Type(cmd) => cmd.flags(),
            JsonSubcommand::ObjLen(cmd) => cmd.flags(),
            JsonSubcommand::ArrLen(cmd) => cmd.flags(),
            JsonSubcommand::NumIncrBy(cmd) => cmd.flags(),
            JsonSubcommand::ObjKeys(cmd) => cmd.flags(),
            JsonSubcommand::ArrInsert(cmd) => cmd.flags(),
            JsonSubcommand::ArrPop(cmd) => cmd.flags(),
            JsonSubcommand::StrLen(cmd) => cmd.flags(),
            JsonSubcommand::ArrTrim(cmd) => cmd.flags(),
            JsonSubcommand::ArrIndex(cmd) => cmd.flags(),
            JsonSubcommand::MGet(cmd) => cmd.flags(),
            JsonSubcommand::StrAppend(cmd) => cmd.flags(),
            JsonSubcommand::Toggle(cmd) => cmd.flags(),
            JsonSubcommand::NumMultBy(cmd) => cmd.flags(),
            JsonSubcommand::Clear(cmd) => cmd.flags(), // ADDED THIS LINE
            JsonSubcommand::Merge(cmd) => cmd.flags(), // ADDED THIS LINE
        }
    }

    fn first_key(&self) -> i64 {
        match &self.subcommand {
            JsonSubcommand::MGet(cmd) => cmd.first_key(),
            _ => 1, // Key is the first argument after subcommand name for others
        }
    }

    fn last_key(&self) -> i64 {
        match &self.subcommand {
            JsonSubcommand::MGet(cmd) => cmd.last_key(),
            _ => 1,
        }
    }

    fn step(&self) -> i64 {
        match &self.subcommand {
            JsonSubcommand::MGet(cmd) => cmd.step(),
            _ => 1,
        }
    }

    fn get_keys(&self) -> Vec<Bytes> {
        // Delegate key extraction to the subcommand.
        match &self.subcommand {
            JsonSubcommand::Set(cmd) => cmd.get_keys(),
            JsonSubcommand::Get(cmd) => cmd.get_keys(),
            JsonSubcommand::Del(cmd) => cmd.get_keys(),
            JsonSubcommand::ArrAppend(cmd) => cmd.get_keys(),
            JsonSubcommand::Type(cmd) => cmd.get_keys(),
            JsonSubcommand::ObjLen(cmd) => cmd.get_keys(),
            JsonSubcommand::ArrLen(cmd) => cmd.get_keys(),
            JsonSubcommand::NumIncrBy(cmd) => cmd.get_keys(),
            JsonSubcommand::ObjKeys(cmd) => cmd.get_keys(),
            JsonSubcommand::ArrInsert(cmd) => cmd.get_keys(),
            JsonSubcommand::ArrPop(cmd) => cmd.get_keys(),
            JsonSubcommand::StrLen(cmd) => cmd.get_keys(),
            JsonSubcommand::ArrTrim(cmd) => cmd.get_keys(),
            JsonSubcommand::ArrIndex(cmd) => cmd.get_keys(),
            JsonSubcommand::MGet(cmd) => cmd.get_keys(),
            JsonSubcommand::StrAppend(cmd) => cmd.get_keys(),
            JsonSubcommand::Toggle(cmd) => cmd.get_keys(),
            JsonSubcommand::NumMultBy(cmd) => cmd.get_keys(),
            JsonSubcommand::Clear(cmd) => cmd.get_keys(), // ADDED THIS LINE
            JsonSubcommand::Merge(cmd) => cmd.get_keys(), // ADDED THIS LINE
        }
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        // Prepend the subcommand name to the subcommand's arguments for replication/AOF.
        match &self.subcommand {
            JsonSubcommand::Set(cmd) => {
                let mut args = vec![Bytes::from_static(b"SET")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::Get(cmd) => {
                let mut args = vec![Bytes::from_static(b"GET")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::Del(cmd) => {
                let mut args = vec![Bytes::from_static(b"DEL")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ArrAppend(cmd) => {
                let mut args = vec![Bytes::from_static(b"ARRAPPEND")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::Type(cmd) => {
                let mut args = vec![Bytes::from_static(b"TYPE")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ObjLen(cmd) => {
                let mut args = vec![Bytes::from_static(b"OBJLEN")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ArrLen(cmd) => {
                let mut args = vec![Bytes::from_static(b"ARRLEN")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::NumIncrBy(cmd) => {
                let mut args = vec![Bytes::from_static(b"NUMINCRBY")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ObjKeys(cmd) => {
                let mut args = vec![Bytes::from_static(b"OBJKEYS")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ArrInsert(cmd) => {
                let mut args = vec![Bytes::from_static(b"ARRINSERT")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ArrPop(cmd) => {
                let mut args = vec![Bytes::from_static(b"ARRPOP")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::StrLen(cmd) => {
                let mut args = vec![Bytes::from_static(b"STRLEN")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ArrTrim(cmd) => {
                let mut args = vec![Bytes::from_static(b"ARRTRIM")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::ArrIndex(cmd) => {
                let mut args = vec![Bytes::from_static(b"ARRINDEX")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::MGet(cmd) => {
                let mut args = vec![Bytes::from_static(b"MGET")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::StrAppend(cmd) => {
                let mut args = vec![Bytes::from_static(b"STRAPPEND")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::Toggle(cmd) => {
                let mut args = vec![Bytes::from_static(b"TOGGLE")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::NumMultBy(cmd) => {
                let mut args = vec![Bytes::from_static(b"NUMMULTBY")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::Clear(cmd) => {
                let mut args = vec![Bytes::from_static(b"CLEAR")];
                args.extend(cmd.to_resp_args());
                args
            }
            JsonSubcommand::Merge(cmd) => {
                let mut args = vec![Bytes::from_static(b"MERGE")];
                args.extend(cmd.to_resp_args());
                args
            }
        }
    }
}
