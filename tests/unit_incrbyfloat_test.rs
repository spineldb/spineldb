use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::incrbyfloat::IncrByFloat;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_incrbyfloat_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"10.5")),
    ];
    let incrbyfloat_command = IncrByFloat::parse(&args).unwrap();
    assert_eq!(incrbyfloat_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(incrbyfloat_command.increment, 10.5);
}

#[tokio::test]
async fn test_incrbyfloat_parse_negative_float() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"-3.14")),
    ];
    let incrbyfloat_command = IncrByFloat::parse(&args).unwrap();
    assert_eq!(incrbyfloat_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(incrbyfloat_command.increment, -3.14);
}

#[tokio::test]
async fn test_incrbyfloat_parse_no_args() {
    let args = [];
    let err = IncrByFloat::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incrbyfloat_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = IncrByFloat::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incrbyfloat_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1.0")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = IncrByFloat::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_incrbyfloat_parse_non_float_increment() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not-a-float")),
    ];
    let err = IncrByFloat::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAFloat));
}

#[tokio::test]
async fn test_incrbyfloat_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"1.0")),
    ];
    let err = IncrByFloat::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
