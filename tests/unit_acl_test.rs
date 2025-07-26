use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::acl::{Acl, AclSubcommand};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_acl_parse_no_subcommand() {
    let args = [];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_setuser_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"setuser"))];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_setuser_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"setuser")),
        RespFrame::BulkString(Bytes::from_static(b"testuser")),
        RespFrame::BulkString(Bytes::from_static(b">password")),
        RespFrame::BulkString(Bytes::from_static(b"~*")),
    ];
    let acl_command = Acl::parse(&args).unwrap();
    match acl_command.subcommand {
        AclSubcommand::SetUser { username, rules } => {
            assert_eq!(username, "testuser");
            assert_eq!(rules, vec![">password", "~*"]);
        }
        _ => panic!("Expected SetUser subcommand"),
    }
}

#[tokio::test]
async fn test_acl_setuser_parse_invalid_rule_type() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"setuser")),
        RespFrame::BulkString(Bytes::from_static(b"testuser")),
        RespFrame::Integer(123),
    ];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_acl_setuser_parse_non_bulk_string_rule() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"setuser")),
        RespFrame::BulkString(Bytes::from_static(b"testuser")),
        RespFrame::Integer(123),
    ];
    let result = Acl::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::InvalidState(_))
    ));
}

#[tokio::test]
async fn test_acl_getuser_parse_no_username() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"getuser"))];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_getuser_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"getuser")),
        RespFrame::BulkString(Bytes::from_static(b"testuser")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_getuser_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"getuser")),
        RespFrame::BulkString(Bytes::from_static(b"testuser")),
    ];
    let acl_command = Acl::parse(&args).unwrap();
    match acl_command.subcommand {
        AclSubcommand::GetUser(username) => {
            assert_eq!(username, "testuser");
        }
        _ => panic!("Expected GetUser subcommand"),
    }
}

#[tokio::test]
async fn test_acl_getuser_parse_non_bulk_string_username() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"getuser")),
        RespFrame::Integer(123),
    ];
    let result = Acl::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongType)
    ));
}

#[tokio::test]
async fn test_acl_deluser_parse_no_username() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"deluser"))];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_deluser_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"deluser")),
        RespFrame::BulkString(Bytes::from_static(b"testuser")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_deluser_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"deluser")),
        RespFrame::BulkString(Bytes::from_static(b"testuser")),
    ];
    let acl_command = Acl::parse(&args).unwrap();
    match acl_command.subcommand {
        AclSubcommand::DelUser(username) => {
            assert_eq!(username, "testuser");
        }
        _ => panic!("Expected DelUser subcommand"),
    }
}

#[tokio::test]
async fn test_acl_list_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"list"))];
    let acl_command = Acl::parse(&args).unwrap();
    match acl_command.subcommand {
        AclSubcommand::List => {
            // Success
        }
        _ => panic!("Expected List subcommand"),
    }
}

#[tokio::test]
async fn test_acl_list_parse_with_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"list")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_save_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"save"))];
    let acl_command = Acl::parse(&args).unwrap();
    match acl_command.subcommand {
        AclSubcommand::Save => {
            // Success
        }
        _ => panic!("Expected Save subcommand"),
    }
}

#[tokio::test]
async fn test_acl_save_parse_with_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"save")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_acl_parse_unknown_subcommand() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"unknown"))];
    let err = Acl::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownCommand"));
}
