// src/core/commands/vector/command.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

use super::vs_add::VsAdd;
use super::vs_card::VsCard;
use super::vs_del::VsDel;
use super::vs_exists::VsExists;
use super::vs_expire::VsExpire;
use super::vs_federatedsearch::VsFederatedSearch;
use super::vs_get::VsGet;
use super::vs_hybridsearch::VsHybridSearch;
use super::vs_info::VsInfo;
use super::vs_madd::VsMAdd;
use super::vs_msearch::VsMSearch;
use super::vs_optimize::VsOptimize;
use super::vs_quantize::VsQuantize;
use super::vs_rebuild::VsRebuild;
use super::vs_reserve::VsReserve;
use super::vs_search::VsSearch;
use super::vs_stats::VsStats;
use super::vs_trainpq::VsTrainPq;
use super::vs_ttl::VsTtl;
use super::vs_update::VsUpdate;

/// Represents the specific SpinelVector subcommand being executed.
#[derive(Debug, Clone)]
pub enum VectorSubcommand {
    Reserve(VsReserve),
    Add(VsAdd),
    MAdd(VsMAdd),
    Get(VsGet),
    Del(VsDel),
    Search(VsSearch),
    MSearch(VsMSearch),
    Update(VsUpdate),
    Info(VsInfo),
    Card(VsCard),
    Exists(VsExists),
    Rebuild(VsRebuild),
    Optimize(VsOptimize),
    Stats(VsStats),
    Expire(VsExpire),
    Ttl(VsTtl),
    Quantize(VsQuantize),
    HybridSearch(VsHybridSearch),
    TrainPq(VsTrainPq),
    FederatedSearch(VsFederatedSearch),
}

/// Implements the top-level `VS` command, acting as a dispatcher for its subcommands.
#[derive(Debug, Clone, Default)]
pub struct Vector {
    pub subcommand: Option<VectorSubcommand>,
}

impl ParseCommand for Vector {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("VS".to_string()));
        }
        let subcommand_name = match &args[0] {
            RespFrame::BulkString(bs) => String::from_utf8_lossy(bs).to_ascii_lowercase(),
            _ => return Err(SpinelDBError::SyntaxError),
        };
        let subcommand_args = &args[1..];

        let subcommand = match subcommand_name.as_str() {
            "reserve" => VectorSubcommand::Reserve(VsReserve::parse(subcommand_args)?),
            "add" => VectorSubcommand::Add(VsAdd::parse(subcommand_args)?),
            "madd" => VectorSubcommand::MAdd(VsMAdd::parse(subcommand_args)?),
            "get" => VectorSubcommand::Get(VsGet::parse(subcommand_args)?),
            "del" => VectorSubcommand::Del(VsDel::parse(subcommand_args)?),
            "search" => VectorSubcommand::Search(VsSearch::parse(subcommand_args)?),
            "msearch" => VectorSubcommand::MSearch(VsMSearch::parse(subcommand_args)?),
            "update" => VectorSubcommand::Update(VsUpdate::parse(subcommand_args)?),
            "info" => VectorSubcommand::Info(VsInfo::parse(subcommand_args)?),
            "card" => VectorSubcommand::Card(VsCard::parse(subcommand_args)?),
            "exists" => VectorSubcommand::Exists(VsExists::parse(subcommand_args)?),
            "rebuild" => VectorSubcommand::Rebuild(VsRebuild::parse(subcommand_args)?),
            "optimize" => VectorSubcommand::Optimize(VsOptimize::parse(subcommand_args)?),
            "stats" => VectorSubcommand::Stats(VsStats::parse(subcommand_args)?),
            "expire" => VectorSubcommand::Expire(VsExpire::parse(subcommand_args)?),
            "ttl" => VectorSubcommand::Ttl(VsTtl::parse(subcommand_args)?),
            "quantize" => VectorSubcommand::Quantize(VsQuantize::parse(subcommand_args)?),
            "hybridsearch" => {
                VectorSubcommand::HybridSearch(VsHybridSearch::parse(subcommand_args)?)
            }
            "trainpq" => VectorSubcommand::TrainPq(VsTrainPq::parse(subcommand_args)?),
            "federatedsearch" => {
                VectorSubcommand::FederatedSearch(VsFederatedSearch::parse(subcommand_args)?)
            }
            _ => {
                return Err(SpinelDBError::UnknownCommand(format!(
                    "VS.{}",
                    subcommand_name.to_uppercase()
                )));
            }
        };

        Ok(Vector {
            subcommand: Some(subcommand),
        })
    }
}

#[async_trait]
impl ExecutableCommand for Vector {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        match &self.subcommand {
            Some(VectorSubcommand::Reserve(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Add(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::MAdd(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Get(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Del(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Search(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::MSearch(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Update(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Info(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Card(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Exists(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Rebuild(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Optimize(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Stats(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Expire(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Ttl(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::Quantize(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::HybridSearch(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::TrainPq(cmd)) => cmd.execute(ctx).await,
            Some(VectorSubcommand::FederatedSearch(cmd)) => cmd.execute(ctx).await,
            None => Err(SpinelDBError::Internal("VS command not parsed".into())),
        }
    }
}

impl CommandSpec for Vector {
    fn name(&self) -> &'static str {
        "vs"
    }
    fn arity(&self) -> i64 {
        -2
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM
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
        match &self.subcommand {
            Some(VectorSubcommand::Reserve(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Add(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::MAdd(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Get(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Del(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Search(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::MSearch(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Update(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Info(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Card(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Exists(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Rebuild(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Optimize(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Stats(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Expire(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Ttl(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::Quantize(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::HybridSearch(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::TrainPq(cmd)) => cmd.get_keys(),
            Some(VectorSubcommand::FederatedSearch(cmd)) => cmd.get_keys(),
            None => vec![],
        }
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        match &self.subcommand {
            Some(VectorSubcommand::Reserve(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Add(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::MAdd(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Get(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Del(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Search(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::MSearch(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Update(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Info(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Card(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Exists(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Rebuild(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Optimize(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Stats(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Expire(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Ttl(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::Quantize(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::HybridSearch(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::TrainPq(cmd)) => cmd.to_resp_args(),
            Some(VectorSubcommand::FederatedSearch(cmd)) => cmd.to_resp_args(),
            None => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bs(s: &str) -> RespFrame {
        RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes()))
    }

    #[test]
    fn test_vector_default_has_no_subcommand() {
        let v = Vector::default();
        assert!(v.subcommand.is_none());
    }

    #[test]
    fn test_vector_parse_no_args_is_error() {
        let r = Vector::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_vector_parse_reserve() {
        let v = Vector::parse(&[bs("RESERVE"), bs("k"), bs("3"), bs("L2")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Reserve(_))));
    }

    #[test]
    fn test_vector_parse_add() {
        let v = Vector::parse(&[bs("ADD"), bs("k"), bs("v1"), bs("1.0"), bs("2.0")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Add(_))));
    }

    #[test]
    fn test_vector_parse_madd() {
        let v = Vector::parse(&[bs("MADD"), bs("k"), bs("v1"), bs("1.0"), bs("2.0")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::MAdd(_))));
    }

    #[test]
    fn test_vector_parse_get() {
        let v = Vector::parse(&[bs("GET"), bs("k"), bs("v1")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Get(_))));
    }

    #[test]
    fn test_vector_parse_del() {
        let v = Vector::parse(&[bs("DEL"), bs("k"), bs("v1")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Del(_))));
    }

    #[test]
    fn test_vector_parse_search() {
        let v = Vector::parse(&[
            bs("SEARCH"),
            bs("k"),
            bs("1.0"),
            bs("2.0"),
            bs("COUNT"),
            bs("5"),
        ])
        .unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Search(_))));
    }

    #[test]
    fn test_vector_parse_msearch() {
        let v = Vector::parse(&[
            bs("MSEARCH"),
            bs("k"),
            bs("1.0"),
            bs("2.0"),
            bs("3.0"),
            bs("4.0"),
            bs("5.0"),
            bs("6.0"),
        ])
        .unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::MSearch(_))));
    }

    #[test]
    fn test_vector_parse_update() {
        let v = Vector::parse(&[
            bs("UPDATE"),
            bs("k"),
            bs("v1"),
            bs("VECTOR"),
            bs("1.0"),
            bs("2.0"),
        ])
        .unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Update(_))));
    }

    #[test]
    fn test_vector_parse_info() {
        let v = Vector::parse(&[bs("INFO"), bs("k")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Info(_))));
    }

    #[test]
    fn test_vector_parse_card() {
        let v = Vector::parse(&[bs("CARD"), bs("k")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Card(_))));
    }

    #[test]
    fn test_vector_parse_unknown_subcommand_is_error() {
        let r = Vector::parse(&[bs("FOO"), bs("k")]);
        assert!(matches!(r, Err(SpinelDBError::UnknownCommand(_))));
    }

    #[test]
    fn test_vector_parse_case_insensitive() {
        let v = Vector::parse(&[bs("add"), bs("k"), bs("v1"), bs("1.0")]).unwrap();
        assert!(matches!(v.subcommand, Some(VectorSubcommand::Add(_))));
    }

    #[test]
    fn test_vector_command_spec_metadata() {
        let v = Vector::default();
        assert_eq!(v.name(), "vs");
        assert_eq!(v.arity(), -2);
        assert!(v.flags().contains(CommandFlags::WRITE));
        assert!(v.flags().contains(CommandFlags::DENY_OOM));
    }
}
