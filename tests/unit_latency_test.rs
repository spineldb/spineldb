use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::latency::{Latency, LatencySubcommand};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_latency_parse_no_subcommand() {
    let args = [];
    let err = Latency::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_latency_doctor_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"doctor"))];
    let latency_command = Latency::parse(&args).unwrap();
    match latency_command.subcommand {
        LatencySubcommand::Doctor => {
            // Success
        }
        _ => panic!("Expected Doctor subcommand"),
    }
}

#[tokio::test]
async fn test_latency_doctor_parse_with_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"doctor")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Latency::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_latency_history_parse_no_event_name() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"history"))];
    let err = Latency::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_latency_history_parse_valid_event_name() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"history")),
        RespFrame::BulkString(Bytes::from_static(b"command")),
    ];
    let latency_command = Latency::parse(&args).unwrap();
    match latency_command.subcommand {
        LatencySubcommand::History(event) => {
            assert_eq!(event, "command");
        }
        _ => panic!("Expected History subcommand"),
    }
}

#[tokio::test]
async fn test_latency_history_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"history")),
        RespFrame::BulkString(Bytes::from_static(b"command")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Latency::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_latency_history_parse_non_bulk_string_event_name() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"history")),
        RespFrame::Integer(123),
    ];
    let err = Latency::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_latency_parse_unknown_subcommand() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"unknown"))];
    let err = Latency::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownCommand"));
}
