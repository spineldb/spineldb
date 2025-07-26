use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::publish::Publish;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_publish_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mychannel")),
        RespFrame::BulkString(Bytes::from_static(b"mymessage")),
    ];
    let publish_command = Publish::parse(&args).unwrap();
    assert_eq!(publish_command.channel, Bytes::from_static(b"mychannel"));
    assert_eq!(publish_command.message, Bytes::from_static(b"mymessage"));
}

#[tokio::test]
async fn test_publish_parse_no_args() {
    let args = [];
    let err = Publish::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_publish_parse_missing_one_arg() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mychannel"))];
    let err = Publish::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_publish_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mychannel")),
        RespFrame::BulkString(Bytes::from_static(b"mymessage")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Publish::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_publish_parse_non_bulk_string_channel() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"mymessage")),
    ];
    let err = Publish::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_publish_parse_non_bulk_string_message() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mychannel")),
        RespFrame::Integer(123),
    ];
    let err = Publish::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
