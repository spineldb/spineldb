use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::incrby::IncrBy;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_incrby_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let incrby_command = IncrBy::parse(&args).unwrap();
    assert_eq!(incrby_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(incrby_command.increment, 10);
}

#[tokio::test]
async fn test_incrby_parse_no_args() {
    let args = [];
    let err = IncrBy::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incrby_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = IncrBy::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incrby_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = IncrBy::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incrby_parse_non_integer_increment() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-integer")),
    ];
    let err = IncrBy::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_incrby_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let err = IncrBy::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
