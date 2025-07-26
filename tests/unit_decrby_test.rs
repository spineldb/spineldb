use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::decrby::DecrBy;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_decrby_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let decrby_command = DecrBy::parse(&args).unwrap();
    assert_eq!(decrby_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(decrby_command.decrement, 10);
}

#[tokio::test]
async fn test_decrby_parse_no_args() {
    let args = [];
    let err = DecrBy::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_decrby_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = DecrBy::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_decrby_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = DecrBy::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_decrby_parse_non_integer_decrement() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-integer")),
    ];
    let err = DecrBy::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_decrby_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"10")),
    ];
    let err = DecrBy::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
