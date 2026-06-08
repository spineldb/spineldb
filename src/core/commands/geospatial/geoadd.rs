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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame(parts: &[&str]) -> Vec<RespFrame> {
        parts
            .iter()
            .map(|s| RespFrame::BulkString(Bytes::copy_from_slice(s.as_bytes())))
            .collect()
    }

    #[test]
    fn test_geoadd_parse_valid() {
        let frames = make_frame(&["key", "10.0", "20.0", "member1"]);
        let cmd = GeoAdd::parse(&frames).unwrap();
        assert_eq!(cmd.key, Bytes::copy_from_slice(b"key"));
        assert_eq!(cmd.members.len(), 1);
    }

    #[test]
    fn test_geoadd_parse_invalid_arity() {
        let frames = make_frame(&["key"]);
        assert!(GeoAdd::parse(&frames).is_err());
    }

    #[test]
    fn test_geoadd_parses_multiple_members() {
        let frames = make_frame(&["key", "10.0", "20.0", "m1", "30.0", "40.0", "m2"]);
        let c = GeoAdd::parse(&frames).unwrap();
        assert_eq!(c.members.len(), 2);
        assert_eq!(c.members[0].2, Bytes::from_static(b"m1"));
        assert_eq!(c.members[1].2, Bytes::from_static(b"m2"));
    }

    #[test]
    fn test_geoadd_with_negative_coords() {
        let frames = make_frame(&["key", "-122.41", "37.77", "sf"]);
        let c = GeoAdd::parse(&frames).unwrap();
        assert!((c.members[0].0 - -122.41).abs() < f64::EPSILON);
        assert!((c.members[0].1 - 37.77).abs() < f64::EPSILON);
    }

    #[test]
    fn test_geoadd_odd_member_count_is_error() {
        // After the key, we need multiples of 3 (lon, lat, member).
        let frames = make_frame(&["key", "10.0", "20.0", "m1", "30.0"]);
        let r = GeoAdd::parse(&frames);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_geoadd_invalid_lon_is_error() {
        let frames = make_frame(&["key", "not_a_float", "20.0", "m1"]);
        let r = GeoAdd::parse(&frames);
        assert!(matches!(r, Err(SpinelDBError::NotAFloat)));
    }

    #[test]
    fn test_geoadd_invalid_lat_is_error() {
        let frames = make_frame(&["key", "10.0", "not_a_float", "m1"]);
        let r = GeoAdd::parse(&frames);
        assert!(matches!(r, Err(SpinelDBError::NotAFloat)));
    }

    #[test]
    fn test_geoadd_no_args_is_error() {
        let r = GeoAdd::parse(&[]);
        assert!(matches!(r, Err(SpinelDBError::WrongArgumentCount(_))));
    }

    #[test]
    fn test_geoadd_to_resp_args_round_trips() {
        let frames = make_frame(&["key", "10.0", "20.0", "m1"]);
        let c = GeoAdd::parse(&frames).unwrap();
        let args = c.to_resp_args();
        assert_eq!(args.len(), 4);
        assert_eq!(args[0], Bytes::from_static(b"key"));
        assert_eq!(args[3], Bytes::from_static(b"m1"));
    }
}
