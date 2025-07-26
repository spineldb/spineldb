use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::memory::{Memory, MemorySubcommand};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_memory_parse_no_subcommand() {
    let args = [];
    let err = Memory::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_memory_usage_parse_no_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"usage"))];
    let err = Memory::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_memory_usage_parse_valid_key() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"usage")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
    ];
    let memory_command = Memory::parse(&args).unwrap();
    match memory_command.subcommand {
        MemorySubcommand::Usage(key) => {
            assert_eq!(key, Bytes::from_static(b"mykey"));
        }
    }
}

#[tokio::test]
async fn test_memory_usage_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"usage")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Memory::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_memory_usage_parse_non_bulk_string_key() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"usage")),
        RespFrame::Integer(123),
    ];
    let result = Memory::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongType)
    ));
}

#[tokio::test]
async fn test_memory_parse_unknown_subcommand() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"unknown"))];
    let err = Memory::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownCommand"));
}
