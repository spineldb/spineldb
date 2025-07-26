use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::msetnx::MSetNx;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_msetnx_parse_single_pair() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
    ];
    let msetnx_command = MSetNx::parse(&args).unwrap();
    assert_eq!(msetnx_command.pairs.len(), 1);
    assert_eq!(msetnx_command.pairs[0].0, Bytes::from_static(b"key1"));
    assert_eq!(msetnx_command.pairs[0].1, Bytes::from_static(b"value1"));
}

#[tokio::test]
async fn test_msetnx_parse_multiple_pairs() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
        RespFrame::BulkString(Bytes::from_static(b"key2")),
        RespFrame::BulkString(Bytes::from_static(b"value2")),
        RespFrame::BulkString(Bytes::from_static(b"key3")),
        RespFrame::BulkString(Bytes::from_static(b"value3")),
    ];
    let msetnx_command = MSetNx::parse(&args).unwrap();
    assert_eq!(msetnx_command.pairs.len(), 3);
    assert_eq!(msetnx_command.pairs[1].0, Bytes::from_static(b"key2"));
    assert_eq!(msetnx_command.pairs[1].1, Bytes::from_static(b"value2"));
}

#[tokio::test]
async fn test_msetnx_parse_no_args() {
    let args = [];
    let err = MSetNx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_msetnx_parse_odd_number_of_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
        RespFrame::BulkString(Bytes::from_static(b"key2")),
    ];
    let err = MSetNx::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_msetnx_parse_non_bulk_string_arg() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::Integer(123),
    ];
    let err = MSetNx::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
