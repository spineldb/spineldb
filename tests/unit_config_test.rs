use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::config::{ConfigGetSet, ConfigSubcommand};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_config_parse_no_subcommand() {
    let args = [];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_config_get_parse_no_param() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"get"))];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_config_get_parse_valid_param() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"get")),
        RespFrame::BulkString(Bytes::from_static(b"databases")),
    ];
    let config_command = ConfigGetSet::parse(&args).unwrap();
    match config_command.subcommand {
        ConfigSubcommand::Get(param) => {
            assert_eq!(param, "databases");
        }
        _ => panic!("Expected Get subcommand"),
    }
}

#[tokio::test]
async fn test_config_get_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"get")),
        RespFrame::BulkString(Bytes::from_static(b"databases")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_config_get_parse_non_bulk_string_param() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"get")),
        RespFrame::Integer(123),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_config_set_parse_no_param_or_value() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"set"))];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_config_set_parse_missing_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"set")),
        RespFrame::BulkString(Bytes::from_static(b"maxmemory")),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_config_set_parse_valid_param_and_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"set")),
        RespFrame::BulkString(Bytes::from_static(b"maxmemory")),
        RespFrame::BulkString(Bytes::from_static(b"1024")),
    ];
    let config_command = ConfigGetSet::parse(&args).unwrap();
    match config_command.subcommand {
        ConfigSubcommand::Set(param, value) => {
            assert_eq!(param, "maxmemory");
            assert_eq!(value, "1024");
        }
        _ => panic!("Expected Set subcommand"),
    }
}

#[tokio::test]
async fn test_config_set_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"set")),
        RespFrame::BulkString(Bytes::from_static(b"maxmemory")),
        RespFrame::BulkString(Bytes::from_static(b"1024")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_config_set_parse_non_bulk_string_param() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"set")),
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"1024")),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_config_set_parse_non_bulk_string_value() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"set")),
        RespFrame::BulkString(Bytes::from_static(b"maxmemory")),
        RespFrame::Integer(123),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_config_rewrite_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"rewrite"))];
    let config_command = ConfigGetSet::parse(&args).unwrap();
    match config_command.subcommand {
        ConfigSubcommand::Rewrite => {
            // Success
        }
        _ => panic!("Expected Rewrite subcommand"),
    }
}

#[tokio::test]
async fn test_config_rewrite_parse_with_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"rewrite")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_config_set_parse_unsupported_param() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"set")),
        RespFrame::BulkString(Bytes::from_static(b"unsupported_param")),
        RespFrame::BulkString(Bytes::from_static(b"value")),
    ];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_config_parse_unknown_subcommand() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"unknown"))];
    let err = ConfigGetSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownCommand"));
}
