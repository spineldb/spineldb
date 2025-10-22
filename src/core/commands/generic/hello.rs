use crate::core::protocol::RespFrame;
use crate::core::SpinelDBError;
use crate::core::commands::command_trait::{CommandFlags, CommandExt};
use bytes::Bytes;

/// The `HELLO` command is used to negotiate the protocol version with the client.
/// It can also be used to provide client name and authentication details.
#[derive(Debug, Clone)]
pub struct Hello {
    pub version: u8,
    // Additional fields for client name, auth, etc., can be added here if needed.
}

impl CommandExt for Hello {
    const NAME: &'static str = "HELLO";
    const FLAGS: CommandFlags = CommandFlags::ADMIN | CommandFlags::FAST; // HELLO is fast and admin-like

    fn parse_frames(frames: Vec<RespFrame>) -> Result<Self, SpinelDBError> {
        if frames.is_empty() {
            return Err(SpinelDBError::SyntaxError);
        }

        // Expecting: HELLO [version]
        // If no version is provided, default to 2 (RESP2)
        let version = if frames.len() > 1 {
            match &frames[1] {
                RespFrame::BulkString(b) | RespFrame::SimpleString(s) => {
                    String::from_utf8_lossy(b).parse::<u8>().map_err(|_| SpinelDBError::SyntaxError)?
                }
                RespFrame::Integer(i) => *i as u8,
                _ => return Err(SpinelDBError::SyntaxError),
            }
        } else {
            2 // Default to RESP2 if no version is specified
        };

        Ok(Hello { version })
    }

    fn to_frame(self) -> RespFrame {
        // This command is typically not sent from server to client as a direct command.
        // It's used for negotiation.
        // If needed for AOF or replication, it would be encoded as an array.
        RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from_static(Self::NAME.as_bytes())),
            RespFrame::Integer(self.version as i64),
        ])
    }

    fn into_args(self) -> Vec<Bytes> {
        vec![Bytes::from(self.version.to_string())]
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![]
    }
}
