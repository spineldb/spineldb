use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::migrate::Migrate;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_migrate_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
    ];
    let migrate_command = Migrate::parse(&args).unwrap();
    assert_eq!(migrate_command.host, "127.0.0.1");
    assert_eq!(migrate_command.port, 6379);
    assert_eq!(migrate_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(migrate_command.db_index, 0);
    assert_eq!(migrate_command.timeout_ms, 5000);
    assert!(!migrate_command.copy);
    assert!(!migrate_command.replace);
}

#[tokio::test]
async fn test_migrate_parse_valid_args_with_copy() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
        RespFrame::BulkString(Bytes::from_static(b"copy")),
    ];
    let migrate_command = Migrate::parse(&args).unwrap();
    assert_eq!(migrate_command.host, "127.0.0.1");
    assert_eq!(migrate_command.port, 6379);
    assert_eq!(migrate_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(migrate_command.db_index, 0);
    assert_eq!(migrate_command.timeout_ms, 5000);
    assert!(migrate_command.copy);
    assert!(!migrate_command.replace);
}

#[tokio::test]
async fn test_migrate_parse_valid_args_with_replace() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
        RespFrame::BulkString(Bytes::from_static(b"replace")),
    ];
    let migrate_command = Migrate::parse(&args).unwrap();
    assert_eq!(migrate_command.host, "127.0.0.1");
    assert_eq!(migrate_command.port, 6379);
    assert_eq!(migrate_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(migrate_command.db_index, 0);
    assert_eq!(migrate_command.timeout_ms, 5000);
    assert!(!migrate_command.copy);
    assert!(migrate_command.replace);
}

#[tokio::test]
async fn test_migrate_parse_valid_args_with_copy_and_replace() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
        RespFrame::BulkString(Bytes::from_static(b"copy")),
        RespFrame::BulkString(Bytes::from_static(b"replace")),
    ];
    let migrate_command = Migrate::parse(&args).unwrap();
    assert_eq!(migrate_command.host, "127.0.0.1");
    assert_eq!(migrate_command.port, 6379);
    assert_eq!(migrate_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(migrate_command.db_index, 0);
    assert_eq!(migrate_command.timeout_ms, 5000);
    assert!(migrate_command.copy);
    assert!(migrate_command.replace);
}

#[tokio::test]
async fn test_migrate_parse_missing_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
    ];
    let err = Migrate::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_migrate_parse_non_bulk_string_host() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
    ];
    let err = Migrate::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_migrate_parse_non_integer_port() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"not_a_port")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
    ];
    let err = Migrate::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_migrate_parse_non_bulk_string_key() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
    ];
    let err = Migrate::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_migrate_parse_non_integer_db_index() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"not_an_int")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
    ];
    let err = Migrate::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_migrate_parse_non_integer_timeout() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"not_an_int")),
    ];
    let err = Migrate::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_migrate_parse_unknown_option() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"127.0.0.1")),
        RespFrame::BulkString(Bytes::from_static(b"6379")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"5000")),
        RespFrame::BulkString(Bytes::from_static(b"unknown_option")),
    ];
    let err = Migrate::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::SyntaxError));
}
