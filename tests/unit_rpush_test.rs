use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::list::rpush::RPush;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_rpush_parse_single_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
    ];
    let rpush_command = RPush::parse(&args).unwrap();
    assert_eq!(rpush_command.key, Bytes::from_static(b"mylist"));
    assert_eq!(rpush_command.values, vec![Bytes::from_static(b"value1")]);
}

#[tokio::test]
async fn test_rpush_parse_multiple_values() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
        RespFrame::BulkString(Bytes::from_static(b"value2")),
        RespFrame::BulkString(Bytes::from_static(b"value3")),
    ];
    let rpush_command = RPush::parse(&args).unwrap();
    assert_eq!(rpush_command.key, Bytes::from_static(b"mylist"));
    assert_eq!(
        rpush_command.values,
        vec![
            Bytes::from_static(b"value1"),
            Bytes::from_static(b"value2"),
            Bytes::from_static(b"value3"),
        ]
    );
}

#[tokio::test]
async fn test_rpush_parse_no_args() {
    let args = [];
    let err = RPush::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_rpush_parse_only_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mylist"))];
    let err = RPush::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_rpush_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
    ];
    let err = RPush::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_rpush_parse_non_bulk_string_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::Integer(456),
    ];
    let err = RPush::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
