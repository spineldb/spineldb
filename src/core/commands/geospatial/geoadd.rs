use super::helpers::coordinates_to_score;
use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{
    CommandFlags, ExecutableCommand, ParseCommand, WriteOutcome,
};
use crate::core::commands::helpers::{extract_bytes, extract_string};
use crate::core::commands::zset::Zadd;
use crate::core::database::ExecutionContext;
use crate::core::protocol::RespFrame;
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
pub struct GeoAdd {
    pub key: Bytes,
    pub members: Vec<(f64, f64, Bytes)>, // lon, lat, member
}

impl ParseCommand for GeoAdd {
    fn parse(args: &[RespFrame]) -> Result<Self, SpinelDBError> {
        if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
            return Err(SpinelDBError::WrongArgumentCount("GEOADD".to_string()));
        }
        let key = extract_bytes(&args[0])?;
        let members = args[1..]
            .chunks_exact(3)
            .map(|chunk| -> Result<(f64, f64, Bytes), SpinelDBError> {
                let lon = extract_string(&chunk[0])?
                    .parse()
                    .map_err(|_| SpinelDBError::NotAFloat)?;
                let lat = extract_string(&chunk[1])?
                    .parse()
                    .map_err(|_| SpinelDBError::NotAFloat)?;
                let member = extract_bytes(&chunk[2])?;
                Ok((lon, lat, member))
            })
            .collect::<Result<_, _>>()?;
        Ok(GeoAdd { key, members })
    }
}

#[async_trait]
impl ExecutableCommand for GeoAdd {
    async fn execute<'a>(
        &self,
        ctx: &mut ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let mut zadd_members = Vec::with_capacity(self.members.len());
        for (lon, lat, member) in &self.members {
            let score = coordinates_to_score(*lon, *lat)?;
            zadd_members.push((score, member.clone()));
        }

        // Delegasikan ke logika ZADD
        let zadd_cmd = Zadd {
            key: self.key.clone(),
            members: zadd_members,
            ..Default::default()
        };
        zadd_cmd.execute(ctx).await
    }
}

impl CommandSpec for GeoAdd {
    fn name(&self) -> &'static str {
        "geoadd"
    }
    fn arity(&self) -> i64 {
        -5
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
        for (lon, lat, member) in &self.members {
            args.push(lon.to_string().into());
            args.push(lat.to_string().into());
            args.push(member.clone());
        }
        args
    }
}
