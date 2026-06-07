// src/core/commands/zset/zadd.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::commands::zset::zpop_logic::PopSide;
use crate::core::commands::zset::{ZIncrBy, ZPopMax, ZPopMin};
use crate::core::database::ExecutionContext;
use crate::core::database::zset::SortedSet;
use crate::core::events::{TransactionData, UnitOfWork};
use crate::core::protocol::RespFrame;
use crate::core::storage::data_types::{DataValue, StoredValue};
use crate::core::{Command, RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

/// Defines the condition for `ZADD` execution (`NX` or `XX`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ZaddCondition {
    #[default]
    None,
    IfNotExists, // NX
    IfExists,    // XX
}

/// Defines the update rule for `ZADD` when a member already exists (`GT` or `LT`).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum ZaddUpdateRule {
    #[default]
    None,
    LessThan,    // LT
    GreaterThan, // GT
}

/// Represents the full `ZADD` command with all its options.
#[derive(Debug, Clone, Default)]
pub struct Zadd {
    pub key: Bytes,
    pub members: Vec<(f64, Bytes)>,
    pub condition: ZaddCondition,
    pub update_rule: ZaddUpdateRule,
    pub ch: bool,
    pub incr: bool,
}

impl ParseCommand for Zadd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount("ZADD".to_string()));
        }

        let key = extract_bytes(&args[0])?;
        let mut i = 1;
        let mut condition = ZaddCondition::None;
        let mut update_rule = ZaddUpdateRule::None;
        let mut ch = false;
        let mut incr = false;

        while i < args.len() {
            if let Ok(flag) = extract_string(&args[i]) {
                match flag.to_ascii_lowercase().as_str() {
                    "nx" => condition = ZaddCondition::IfNotExists,
                    "xx" => condition = ZaddCondition::IfExists,
                    "gt" => update_rule = ZaddUpdateRule::GreaterThan,
                    "lt" => update_rule = ZaddUpdateRule::LessThan,
                    "ch" => ch = true,
                    "incr" => incr = true,
                    _ => break,
                }
                i += 1;
            } else {
                break;
            }
        }

        if condition != ZaddCondition::None && update_rule != ZaddUpdateRule::None {
            return Err(SpinelDBError::SyntaxError);
        }
        if incr && (condition != ZaddCondition::None || update_rule != ZaddUpdateRule::None) {
            return Err(SpinelDBError::SyntaxError);
        }

        if incr && args.len() - i != 2 {
            return Err(SpinelDBError::InvalidState(
                "INCR option supports a single increment-element pair".into(),
            ));
        }

        if (i >= args.len()) || !(args.len() - i).is_multiple_of(2) {
            return Err(SpinelDBError::WrongArgumentCount("ZADD".to_string()));
        }

        let members = args[i..]
            .chunks_exact(2)
            .map(|chunk| -> Result<(f64, Bytes), SpinelDBError> {
                let score = extract_string(&chunk[0])?
                    .parse::<f64>()
                    .map_err(|_| SpinelDBError::NotAFloat)?;
                let member = extract_bytes(&chunk[1])?;
                Ok((score, member))
            })
            .collect::<Result<_, _>>()?;

        Ok(Zadd {
            key,
            members,
            condition,
            update_rule,
            ch,
            incr,
        })
    }
}

/// Returns `true` if the operator has chosen to reject NaN scores for `ZADD`.
async fn reject_nan_scores(ctx: &ExecutionContext<'_>) -> bool {
    ctx.state.config.lock().await.safety.reject_nan_scores
}

#[async_trait]
impl ExecutableCommand for Zadd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        // Reject NaN scores up-front. NaN is technically a valid `f64` but
        // would break all ordering guarantees in the sorted set (Redis
        // itself stores NaN, but every comparison collapses to "equal").
        // Operators can opt back into the legacy behavior via config.
        if reject_nan_scores(ctx).await {
            for (score, _) in &self.members {
                if score.is_nan() {
                    return Err(SpinelDBError::NotAFloat);
                }
            }
        }

        if self.incr {
            if self.members.len() != 1 {
                return Err(SpinelDBError::SyntaxError);
            }
            let (increment, member) = self.members[0].clone();
            let zincrby_cmd = ZIncrBy {
                key: self.key.clone(),
                increment,
                member,
            };
            return zincrby_cmd.execute(ctx).await;
        }

        if self.members.is_empty() {
            return Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite));
        }

        let state_clone = ctx.state.clone();
        let (shard, guard) = ctx.get_single_shard_context_mut()?;

        let mut changed_count = 0;
        let mut added_count = 0;

        let entry = guard.get_or_insert_with_mut(self.key.clone(), || {
            StoredValue::new(DataValue::SortedSet(SortedSet::new()))
        });

        if let DataValue::SortedSet(zset) = &mut entry.data {
            let old_mem = zset.memory_usage();

            for (score, member) in &self.members {
                let old_score = zset.get_score(member);

                if (self.condition == ZaddCondition::IfNotExists && old_score.is_some())
                    || (self.condition == ZaddCondition::IfExists && old_score.is_none())
                {
                    continue;
                }

                if let Some(old_s) = old_score
                    && ((self.update_rule == ZaddUpdateRule::GreaterThan && *score <= old_s)
                        || (self.update_rule == ZaddUpdateRule::LessThan && *score >= old_s))
                {
                    continue;
                }

                if zset.add(*score, member.clone()) {
                    if old_score.is_none() {
                        added_count += 1;
                    }
                    changed_count += 1;
                }
            }

            if changed_count > 0 {
                // After adding elements, try to satisfy a blocked client. The notifier
                // will atomically pop the element if a waiter exists.
                let popped_side = state_clone
                    .blocker_manager
                    .notify_and_pop_zset_waiter(zset, &self.key, PopSide::Min)
                    .or_else(|| {
                        state_clone.blocker_manager.notify_and_pop_zset_waiter(
                            zset,
                            &self.key,
                            PopSide::Max,
                        )
                    });

                // If an atomic handoff occurred, we must manually propagate the state change.
                if let Some(side) = popped_side {
                    let zadd_cmd_for_tx = Command::Zadd(self.clone());
                    let zpop_cmd_for_tx = match side {
                        PopSide::Min => Command::ZPopMin(ZPopMin {
                            pop_cmd: super::zpop_logic::ZPop::new(self.key.clone(), side, Some(1)),
                        }),
                        PopSide::Max => Command::ZPopMax(ZPopMax {
                            pop_cmd: super::zpop_logic::ZPop::new(self.key.clone(), side, Some(1)),
                        }),
                    };

                    let tx_data = TransactionData {
                        all_commands: vec![zadd_cmd_for_tx.clone(), zpop_cmd_for_tx.clone()],
                        write_commands: vec![zadd_cmd_for_tx, zpop_cmd_for_tx],
                    };

                    ctx.state
                        .event_bus
                        .publish(UnitOfWork::Transaction(Box::new(tx_data)), &ctx.state);

                    // Since propagation is handled, return DidNotWrite.
                    return Ok((
                        RespValue::Integer(if self.ch { changed_count } else { added_count }),
                        WriteOutcome::DidNotWrite,
                    ));
                }

                // Standard write path (no handoff).
                let new_mem = zset.memory_usage();
                entry.version = entry.version.wrapping_add(1);
                entry.size = new_mem;
                if new_mem > old_mem {
                    shard.update_memory((new_mem - old_mem) as isize);
                } else {
                    shard.update_memory(-((old_mem - new_mem) as isize));
                }

                let result_count = if self.ch { changed_count } else { added_count };

                Ok((
                    RespValue::Integer(result_count),
                    WriteOutcome::Write { keys_modified: 1 },
                ))
            } else {
                Ok((RespValue::Integer(0), WriteOutcome::DidNotWrite))
            }
        } else {
            Err(SpinelDBError::WrongType)
        }
    }
}

impl CommandSpec for Zadd {
    fn name(&self) -> &'static str {
        "zadd"
    }
    fn arity(&self) -> i64 {
        -4
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE | CommandFlags::DENY_OOM | CommandFlags::MOVABLEKEYS
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
        if self.condition == ZaddCondition::IfNotExists {
            args.push("NX".into());
        }
        if self.condition == ZaddCondition::IfExists {
            args.push("XX".into());
        }
        if self.update_rule == ZaddUpdateRule::GreaterThan {
            args.push("GT".into());
        }
        if self.update_rule == ZaddUpdateRule::LessThan {
            args.push("LT".into());
        }
        if self.ch {
            args.push("CH".into());
        }
        if self.incr {
            args.push("INCR".into());
        }
        args.extend(
            self.members
                .iter()
                .flat_map(|(s, m)| [s.to_string().into(), m.clone()]),
        );
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
    fn test_zadd_parses_key_and_single_member() {
        let c = Zadd::parse(&[bs("z"), bs("1"), bs("m1")]).unwrap();
        assert_eq!(c.key, Bytes::from_static(b"z"));
        assert_eq!(c.members.len(), 1);
        assert_eq!(c.members[0].0, 1.0);
        assert_eq!(c.members[0].1, Bytes::from_static(b"m1"));
        assert_eq!(c.condition, ZaddCondition::None);
    }

    #[test]
    fn test_zadd_parses_multiple_members() {
        let c = Zadd::parse(&[bs("z"), bs("1"), bs("a"), bs("2"), bs("b")]).unwrap();
        assert_eq!(c.members.len(), 2);
    }

    #[test]
    fn test_zadd_parses_negative_score() {
        let c = Zadd::parse(&[bs("z"), bs("-3.5"), bs("a")]).unwrap();
        assert_eq!(c.members[0].0, -3.5);
    }

    #[test]
    fn test_zadd_parses_nx_flag() {
        let c = Zadd::parse(&[bs("z"), bs("NX"), bs("1"), bs("a")]).unwrap();
        assert_eq!(c.condition, ZaddCondition::IfNotExists);
    }

    #[test]
    fn test_zadd_parses_xx_flag() {
        let c = Zadd::parse(&[bs("z"), bs("XX"), bs("1"), bs("a")]).unwrap();
        assert_eq!(c.condition, ZaddCondition::IfExists);
    }

    #[test]
    fn test_zadd_parses_gt_flag() {
        let c = Zadd::parse(&[bs("z"), bs("GT"), bs("1"), bs("a")]).unwrap();
        assert_eq!(c.update_rule, ZaddUpdateRule::GreaterThan);
    }

    #[test]
    fn test_zadd_parses_lt_flag() {
        let c = Zadd::parse(&[bs("z"), bs("LT"), bs("1"), bs("a")]).unwrap();
        assert_eq!(c.update_rule, ZaddUpdateRule::LessThan);
    }

    #[test]
    fn test_zadd_parses_ch_flag() {
        let c = Zadd::parse(&[bs("z"), bs("CH"), bs("1"), bs("a")]).unwrap();
        assert!(c.ch);
    }

    #[test]
    fn test_zadd_parses_incr_flag_with_one_pair() {
        let c = Zadd::parse(&[bs("z"), bs("INCR"), bs("1"), bs("a")]).unwrap();
        assert!(c.incr);
        assert_eq!(c.members.len(), 1);
    }

    #[test]
    fn test_zadd_flags_case_insensitive() {
        let c = Zadd::parse(&[bs("z"), bs("nx"), bs("1"), bs("a")]).unwrap();
        assert_eq!(c.condition, ZaddCondition::IfNotExists);
    }

    #[test]
    fn test_zadd_with_nx_and_gt_is_syntax_error() {
        let r = Zadd::parse(&[bs("z"), bs("NX"), bs("GT"), bs("1"), bs("a")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_zadd_with_incr_and_nx_is_syntax_error() {
        let r = Zadd::parse(&[bs("z"), bs("INCR"), bs("NX"), bs("1"), bs("a")]);
        assert!(matches!(r, Err(SpinelDBError::SyntaxError)));
    }

    #[test]
    fn test_zadd_incr_with_multiple_pairs_is_error() {
        let r = Zadd::parse(&[bs("z"), bs("INCR"), bs("1"), bs("a"), bs("2"), bs("b")]);
        assert!(matches!(
            r,
            Err(SpinelDBError::InvalidState(_)) | Err(SpinelDBError::SyntaxError)
        ));
    }

    #[test]
    fn test_zadd_with_odd_member_args_is_error() {
        let r = Zadd::parse(&[bs("z"), bs("1")]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_zadd_with_no_args_is_error() {
        let r = Zadd::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_zadd_with_non_float_score_is_error() {
        let r = Zadd::parse(&[bs("z"), bs("abc"), bs("a")]);
        assert!(matches!(r, Err(SpinelDBError::NotAFloat)));
    }

    #[test]
    fn test_zadd_to_resp_args_with_all_flags() {
        let c = Zadd::parse(&[bs("z"), bs("GT"), bs("CH"), bs("1"), bs("a")]).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args[0], Bytes::from_static(b"z"));
        assert_eq!(args[1], Bytes::from_static(b"GT"));
        assert_eq!(args[2], Bytes::from_static(b"CH"));
        assert_eq!(args[3], Bytes::from_static(b"1"));
        assert_eq!(args[4], Bytes::from_static(b"a"));
    }
}
