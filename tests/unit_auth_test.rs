use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::auth::Auth;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_auth_parse_valid_password() {
    let args = [RespFrame::BulkString(Bytes::from_static(
        b"my_secret_password",
    ))];
    let auth_command = Auth::parse(&args).unwrap();
    assert_eq!(auth_command.password, "my_secret_password");
}

#[tokio::test]
async fn test_auth_parse_no_args() {
    let args = [];
    let err = Auth::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_auth_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"user")),
        RespFrame::BulkString(Bytes::from_static(b"password")),
    ];
    let err = Auth::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_auth_parse_non_bulk_string_arg() {
    let args = [RespFrame::SimpleString("password".to_string())];
    let err = Auth::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
