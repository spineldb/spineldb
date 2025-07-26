use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::getdel::GetDel;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_getdel_parse_valid() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let getdel_command = GetDel::parse(&args).unwrap();
    assert_eq!(getdel_command.key, Bytes::from_static(b"mykey"));
}

#[tokio::test]
async fn test_getdel_parse_no_args() {
    let args = [];
    let err = GetDel::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_getdel_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"another_arg")),
    ];
    let err = GetDel::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_getdel_parse_non_bulk_string_key() {
    let args = [RespFrame::Integer(123)];
    let err = GetDel::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
