// src/core/commands/generic/migrate.rs

use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::persistence::spldb;
use crate::core::protocol::{RespFrame, RespFrameCodec, RespValue};
use crate::core::storage::data_types::DataValue;
use crate::core::storage::db::ExecutionContext;
use crate::core::{Command, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

#[derive(Debug, Clone, Default)]
pub struct Migrate {
    pub host: String,
    pub port: u16,
    pub key: Bytes,
    pub db_index: usize,
    pub timeout_ms: u64,
    pub copy: bool,
    pub replace: bool,
}

impl ParseCommand for Migrate {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 5 {
            return Err(SpinelDBError::WrongArgumentCount("MIGRATE".to_string()));
        }
        let mut cmd = Migrate {
            host: extract_string(&args[0])?,
            port: extract_string(&args[1])?.parse()?,
            key: extract_bytes(&args[2])?,
            db_index: extract_string(&args[3])?.parse()?,
            timeout_ms: extract_string(&args[4])?.parse()?,
            ..Default::default()
        };

        let mut i = 5;
        while i < args.len() {
            let option = extract_string(&args[i])?.to_ascii_lowercase();
            match option.as_str() {
                "copy" => cmd.copy = true,
                "replace" => cmd.replace = true,
                _ => return Err(SpinelDBError::SyntaxError),
            }
            i += 1;
        }

        Ok(cmd)
    }
}

#[async_trait]
impl ExecutableCommand for Migrate {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let data_type_discriminant = {
            let (_, guard) = ctx.get_single_shard_context_mut()?;
            let Some(stored_value) = guard.peek(&self.key) else {
                return Ok((
                    RespValue::SimpleString("NOKEY".into()),
                    WriteOutcome::DidNotWrite,
                ));
            };

            if stored_value.is_expired() {
                guard.pop(&self.key);
                return Ok((
                    RespValue::SimpleString("NOKEY".into()),
                    WriteOutcome::DidNotWrite,
                ));
            }
            std::mem::discriminant(&stored_value.data)
        };

        match data_type_discriminant {
            d if d == std::mem::discriminant(&DataValue::List(Default::default()))
                || d == std::mem::discriminant(&DataValue::Set(Default::default()))
                || d == std::mem::discriminant(&DataValue::Hash(Default::default()))
                || d == std::mem::discriminant(&DataValue::SortedSet(Default::default())) =>
            {
                self.migrate_collection_iteratively(ctx).await
            }
            _ => self.migrate_simple_value(ctx).await,
        }
    }
}

impl Migrate {
    async fn connect_and_select(
        &self,
        framed: &mut Framed<TcpStream, RespFrameCodec>,
    ) -> Result<(), SpinelDBError> {
        let select_cmd: RespFrame = Command::Select(crate::core::commands::generic::Select {
            db_index: self.db_index,
        })
        .into();

        framed.send(select_cmd).await?;
        match framed.next().await {
            Some(Ok(RespFrame::SimpleString(s))) if s == "OK" => Ok(()),
            _ => Err(SpinelDBError::MigrationError(
                "IOERR target did not acknowledge SELECT".into(),
            )),
        }
    }

    async fn migrate_simple_value<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let (serialized_value, ttl_ms) = {
            let (_, guard) = ctx.get_single_shard_context_mut()?;
            let stored_value = guard.peek(&self.key).unwrap();
            let serialized = spldb::serialize_value(&stored_value.data).map_err(|e| {
                SpinelDBError::MigrationError(format!("Failed to serialize key: {e}"))
            })?;
            (
                serialized,
                stored_value.remaining_ttl_ms().unwrap_or(0).max(0) as u64,
            )
        };

        let target_addr = format!("{}:{}", self.host, self.port);
        let timeout = Duration::from_millis(self.timeout_ms);
        let socket = tokio::time::timeout(timeout, TcpStream::connect(&target_addr))
            .await
            .map_err(|_| SpinelDBError::MigrationError("IOERR connection timed out".into()))?
            .map_err(|e| SpinelDBError::MigrationError(format!("IOERR failed to connect: {e}")))?;
        let mut framed = Framed::new(socket, RespFrameCodec);

        self.connect_and_select(&mut framed).await?;

        let restore_cmd = Command::Restore(crate::core::commands::generic::Restore {
            key: self.key.clone(),
            ttl_ms,
            serialized_value,
            replace: self.replace,
        });
        framed.send(restore_cmd.into()).await?;

        match framed.next().await {
            Some(Ok(RespFrame::SimpleString(s))) if s == "OK" => {}
            Some(Ok(RespFrame::Error(e))) => return Err(SpinelDBError::MigrationError(e)),
            _ => {
                return Err(SpinelDBError::MigrationError(
                    "IOERR unexpected response from target".into(),
                ));
            }
        }

        if self.copy {
            return Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ));
        }

        let (_, guard) = ctx.get_single_shard_context_mut()?;
        if guard.pop(&self.key).is_some() {
            Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::Delete { keys_deleted: 1 },
            ))
        } else {
            Ok((
                RespValue::SimpleString("OK".into()),
                WriteOutcome::DidNotWrite,
            ))
        }
    }

    async fn migrate_collection_iteratively<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        const BATCH_SIZE: usize = 100;

        let (source_data_clone, ttl_ms) = {
            let (_, guard) = ctx.get_single_shard_context_mut()?;
            let entry = guard.peek(&self.key).unwrap();
            (
                entry.data.clone(),
                entry.remaining_ttl_ms().unwrap_or(0).max(0) as u64,
            )
        };

        let target_addr = format!("{}:{}", self.host, self.port);
        let timeout = Duration::from_millis(self.timeout_ms);
        let socket = tokio::time::timeout(timeout, TcpStream::connect(&target_addr))
            .await
            .map_err(|_| SpinelDBError::MigrationError("IOERR connection timed out".into()))?
            .map_err(|e| SpinelDBError::MigrationError(format!("IOERR failed to connect: {e}")))?;
        let mut framed = Framed::new(socket, RespFrameCodec);

        self.connect_and_select(&mut framed).await?;

        if self.replace {
            let del_cmd = Command::Del(crate::core::commands::generic::Del {
                keys: vec![self.key.clone()],
            });
            framed.send(del_cmd.into()).await?;
            let _ = framed.next().await;
        }

        let batch_commands_iter: Box<dyn Iterator<Item = Command> + Send> = match source_data_clone
        {
            DataValue::List(items) => {
                let items_vec: Vec<Bytes> = items.into_iter().collect();
                Box::new(
                    items_vec
                        .chunks(BATCH_SIZE)
                        .map({
                            let key = self.key.clone();
                            move |chunk| {
                                Command::RPush(crate::core::commands::list::RPush {
                                    key: key.clone(),
                                    values: chunk.to_vec(),
                                })
                            }
                        })
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
            DataValue::Set(items) => {
                let items_vec: Vec<Bytes> = items.into_iter().collect();
                Box::new(
                    items_vec
                        .chunks(BATCH_SIZE)
                        .map({
                            let key = self.key.clone();
                            move |chunk| {
                                Command::Sadd(crate::core::commands::set::Sadd {
                                    key: key.clone(),
                                    members: chunk.to_vec(),
                                })
                            }
                        })
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
            DataValue::Hash(items) => {
                let items_vec: Vec<(Bytes, Bytes)> = items.into_iter().collect();
                Box::new(
                    items_vec
                        .chunks(BATCH_SIZE)
                        .map({
                            let key = self.key.clone();
                            move |chunk| {
                                Command::HSet(crate::core::commands::hash::HSet {
                                    key: key.clone(),
                                    fields: chunk.to_vec(),
                                })
                            }
                        })
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
            DataValue::SortedSet(items) => {
                let items_vec = items.get_range(0, -1);
                Box::new(
                    items_vec
                        .chunks(BATCH_SIZE)
                        .map({
                            let key = self.key.clone();
                            move |chunk| {
                                Command::Zadd(crate::core::commands::zset::Zadd {
                                    key: key.clone(),
                                    members: chunk
                                        .iter()
                                        .map(|e| (e.score, e.member.clone()))
                                        .collect(),
                                    ..Default::default()
                                })
                            }
                        })
                        .collect::<Vec<_>>()
                        .into_iter(),
                )
            }
            _ => unreachable!(),
        };

        for cmd in batch_commands_iter {
            framed.send(cmd.into()).await?;
            if let Some(Ok(RespFrame::Error(e))) = framed.next().await {
                return Err(SpinelDBError::MigrationError(format!("TARGETERR {e}")));
            }
        }

        if ttl_ms > 0 {
            let pexpire_cmd = Command::PExpire(crate::core::commands::generic::PExpire {
                key: self.key.clone(),
                milliseconds: ttl_ms,
            });
            framed.send(pexpire_cmd.into()).await?;
            let _ = framed.next().await;
        }

        let write_outcome = if !self.copy {
            let (_, guard) = ctx.get_single_shard_context_mut()?;
            if guard.pop(&self.key).is_some() {
                WriteOutcome::Delete { keys_deleted: 1 }
            } else {
                WriteOutcome::DidNotWrite
            }
        } else {
            WriteOutcome::DidNotWrite
        };

        Ok((RespValue::SimpleString("OK".into()), write_outcome))
    }
}

impl CommandSpec for Migrate {
    fn name(&self) -> &'static str {
        "migrate"
    }
    fn arity(&self) -> i64 {
        -6
    }
    fn flags(&self) -> CommandFlags {
        CommandFlags::WRITE
            | CommandFlags::ADMIN
            | CommandFlags::NO_PROPAGATE
            | CommandFlags::MOVABLEKEYS
    }
    fn first_key(&self) -> i64 {
        3
    }
    fn last_key(&self) -> i64 {
        3
    }
    fn step(&self) -> i64 {
        1
    }
    fn get_keys(&self) -> Vec<Bytes> {
        vec![self.key.clone()]
    }
    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![
            self.host.clone().into(),
            self.port.to_string().into(),
            self.key.clone(),
            self.db_index.to_string().into(),
            self.timeout_ms.to_string().into(),
        ];
        if self.copy {
            args.push("COPY".into());
        }
        if self.replace {
            args.push("REPLACE".into());
        }
        args
    }
}
