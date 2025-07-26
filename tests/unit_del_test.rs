use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::del::Del;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_del_parse_single_key() {
    let args = [RespFrame::BulkString(Bytes::from("key1"))];
    let del_command = Del::parse(&args).unwrap();
    assert_eq!(del_command.keys, vec![Bytes::from("key1")]);
}

#[tokio::test]
async fn test_del_parse_multiple_keys() {
    let args = [
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("key2")),
        RespFrame::BulkString(Bytes::from("key3")),
    ];
    let del_command = Del::parse(&args).unwrap();
    assert_eq!(
        del_command.keys,
        vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3")
        ]
    );
}

#[tokio::test]
async fn test_del_parse_no_args() {
    let args = [];
    let err = Del::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_del_parse_non_bulk_string_arg() {
    let args = [RespFrame::SimpleString("key1".to_string())];
    let err = Del::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
