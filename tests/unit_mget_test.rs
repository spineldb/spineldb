use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::mget::MGet;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_mget_parse_single_key() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"key1"))];
    let mget_command = MGet::parse(&args).unwrap();
    assert_eq!(mget_command.keys, vec![Bytes::from_static(b"key1")]);
}

#[tokio::test]
async fn test_mget_parse_multiple_keys() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"key2")),
        RespFrame::BulkString(Bytes::from_static(b"key3")),
    ];
    let mget_command = MGet::parse(&args).unwrap();
    assert_eq!(
        mget_command.keys,
        vec![
            Bytes::from_static(b"key1"),
            Bytes::from_static(b"key2"),
            Bytes::from_static(b"key3"),
        ]
    );
}

#[tokio::test]
async fn test_mget_parse_no_args() {
    let args = [];
    let err = MGet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_mget_parse_non_bulk_string_key() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"key3")),
    ];
    let err = MGet::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
