use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::client::{Client, ClientSubcommand};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_client_parse_no_subcommand() {
    let args = [];
    let err = Client::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_client_list_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"list"))];
    let client_command = Client::parse(&args).unwrap();
    match client_command.subcommand {
        ClientSubcommand::List => {
            // Success
        }
        _ => panic!("Expected List subcommand"),
    }
}

#[tokio::test]
async fn test_client_list_parse_with_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"list")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Client::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_client_setname_parse_no_name() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"setname"))];
    let err = Client::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_client_setname_parse_valid_name() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"setname")),
        RespFrame::BulkString(Bytes::from_static(b"myclient")),
    ];
    let client_command = Client::parse(&args).unwrap();
    match client_command.subcommand {
        ClientSubcommand::SetName(name) => {
            assert_eq!(name, Bytes::from_static(b"myclient"));
        }
        _ => panic!("Expected SetName subcommand"),
    }
}

#[tokio::test]
async fn test_client_setname_parse_non_bulk_string_arg() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"setname")),
        RespFrame::SimpleString("myclient".to_string()),
    ];
    let err = Client::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_client_getname_parse_no_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"getname"))];
    let client_command = Client::parse(&args).unwrap();
    match client_command.subcommand {
        ClientSubcommand::GetName => {
            // Success
        }
        _ => panic!("Expected GetName subcommand"),
    }
}

#[tokio::test]
async fn test_client_getname_parse_with_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"getname")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Client::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_client_kill_parse_no_id() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"kill"))];
    let err = Client::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_client_kill_parse_valid_id() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"kill")),
        RespFrame::BulkString(Bytes::from_static(b"123")),
    ];
    let client_command = Client::parse(&args).unwrap();
    match client_command.subcommand {
        ClientSubcommand::Kill(id) => {
            assert_eq!(id, 123);
        }
        _ => panic!("Expected Kill subcommand"),
    }
}

#[tokio::test]
async fn test_client_kill_parse_invalid_id() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"kill")),
        RespFrame::BulkString(Bytes::from_static(b"not_an_id")),
    ];
    let err = Client::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_client_kill_parse_non_bulk_string_id() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"kill")),
        RespFrame::Integer(123),
    ];
    let err = Client::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_client_parse_unknown_subcommand() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"unknown"))];
    let err = Client::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("UnknownCommand"));
}
