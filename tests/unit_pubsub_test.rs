use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::pubsub::{PubSubInfo, PubSubSubcommand};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_pubsub_parse_no_subcommand() {
    let args = [];
    let err = PubSubInfo::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pubsub_channels_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"channels"))];
    let pubsub_command = PubSubInfo::parse(&args).unwrap();
    match pubsub_command.subcommand {
        PubSubSubcommand::Channels(pattern) => {
            assert!(pattern.is_none());
        }
        _ => panic!("Expected Channels subcommand"),
    }
}

#[tokio::test]
async fn test_pubsub_channels_parse_with_pattern() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"channels")),
        RespFrame::BulkString(Bytes::from_static(b"mychannel*")),
    ];
    let pubsub_command = PubSubInfo::parse(&args).unwrap();
    match pubsub_command.subcommand {
        PubSubSubcommand::Channels(pattern) => {
            assert_eq!(pattern, Some(Bytes::from_static(b"mychannel*")));
        }
        _ => panic!("Expected Channels subcommand"),
    }
}

#[tokio::test]
async fn test_pubsub_channels_parse_non_bulk_string_pattern() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"channels")),
        RespFrame::Integer(123),
    ];
    let err = PubSubInfo::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_pubsub_numsub_parse_no_channels() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"numsub"))];
    let pubsub_command = PubSubInfo::parse(&args).unwrap();
    match pubsub_command.subcommand {
        PubSubSubcommand::NumSub(channels) => {
            assert!(channels.is_empty());
        }
        _ => panic!("Expected NumSub subcommand"),
    }
}

#[tokio::test]
async fn test_pubsub_numsub_parse_with_channels() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"numsub")),
        RespFrame::BulkString(Bytes::from_static(b"channel1")),
        RespFrame::BulkString(Bytes::from_static(b"channel2")),
    ];
    let pubsub_command = PubSubInfo::parse(&args).unwrap();
    match pubsub_command.subcommand {
        PubSubSubcommand::NumSub(channels) => {
            assert_eq!(
                channels,
                vec![
                    Bytes::from_static(b"channel1"),
                    Bytes::from_static(b"channel2")
                ]
            );
        }
        _ => panic!("Expected NumSub subcommand"),
    }
}

#[tokio::test]
async fn test_pubsub_numsub_parse_non_bulk_string_channel() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"numsub")),
        RespFrame::Integer(123),
    ];
    let err = PubSubInfo::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_pubsub_numpat_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"numpat"))];
    let pubsub_command = PubSubInfo::parse(&args).unwrap();
    match pubsub_command.subcommand {
        PubSubSubcommand::NumPat => {
            // Success
        }
        _ => panic!("Expected NumPat subcommand"),
    }
}

#[tokio::test]
async fn test_pubsub_numpat_parse_with_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"numpat")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = PubSubInfo::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pubsub_parse_unknown_subcommand() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"unknown"))];
    let err = PubSubInfo::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownCommand"));
}
