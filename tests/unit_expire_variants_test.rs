use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::expire_variants::{ExpireAt, PExpire, PExpireAt};
use spineldb::core::protocol::RespFrame;

// --- PEXPIRE Tests ---

#[tokio::test]
async fn test_pexpire_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1000")),
    ];
    let pexpire_command = PExpire::parse(&args).unwrap();
    assert_eq!(pexpire_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(pexpire_command.milliseconds, 1000);
}

#[tokio::test]
async fn test_pexpire_parse_no_args() {
    let args = [];
    let err = PExpire::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pexpire_parse_only_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = PExpire::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pexpire_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1000")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = PExpire::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pexpire_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"1000")),
    ];
    let err = PExpire::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_pexpire_parse_non_integer_milliseconds() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not_an_int")),
    ];
    let err = PExpire::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

// --- EXPIREAT Tests ---

#[tokio::test]
async fn test_expireat_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1678886400")),
    ];
    let expireat_command = ExpireAt::parse(&args).unwrap();
    assert_eq!(expireat_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(expireat_command.unix_seconds, 1678886400);
}

#[tokio::test]
async fn test_expireat_parse_no_args() {
    let args = [];
    let err = ExpireAt::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_expireat_parse_only_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = ExpireAt::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_expireat_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1678886400")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = ExpireAt::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_expireat_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"1678886400")),
    ];
    let err = ExpireAt::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_expireat_parse_non_integer_unix_seconds() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not_an_int")),
    ];
    let err = ExpireAt::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

// --- PEXPIREAT Tests ---

#[tokio::test]
async fn test_pexpireat_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1678886400000")),
    ];
    let pexpireat_command = PExpireAt::parse(&args).unwrap();
    assert_eq!(pexpireat_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(pexpireat_command.unix_milliseconds, 1678886400000);
}

#[tokio::test]
async fn test_pexpireat_parse_no_args() {
    let args = [];
    let err = PExpireAt::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pexpireat_parse_only_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = PExpireAt::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pexpireat_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"1678886400000")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = PExpireAt::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_pexpireat_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"1678886400000")),
    ];
    let err = PExpireAt::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_pexpireat_parse_non_integer_unix_milliseconds() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not_an_int")),
    ];
    let err = PExpireAt::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}
