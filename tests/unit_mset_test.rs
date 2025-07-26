use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::mset::MSet;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_mset_parse_single_pair() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
    ];
    let mset_command = MSet::parse(&args).unwrap();
    assert_eq!(mset_command.pairs.len(), 2);
    assert_eq!(
        mset_command.pairs[0],
        RespFrame::BulkString(Bytes::from_static(b"key1"))
    );
    assert_eq!(
        mset_command.pairs[1],
        RespFrame::BulkString(Bytes::from_static(b"value1"))
    );
}

#[tokio::test]
async fn test_mset_parse_multiple_pairs() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
        RespFrame::BulkString(Bytes::from_static(b"key2")),
        RespFrame::BulkString(Bytes::from_static(b"value2")),
        RespFrame::BulkString(Bytes::from_static(b"key3")),
        RespFrame::BulkString(Bytes::from_static(b"value3")),
    ];
    let mset_command = MSet::parse(&args).unwrap();
    assert_eq!(mset_command.pairs.len(), 6);
    // Just check a couple to ensure parsing worked for multiple pairs
    assert_eq!(
        mset_command.pairs[2],
        RespFrame::BulkString(Bytes::from_static(b"key2"))
    );
    assert_eq!(
        mset_command.pairs[3],
        RespFrame::BulkString(Bytes::from_static(b"value2"))
    );
}

#[tokio::test]
async fn test_mset_parse_no_args() {
    let args = [];
    let err = MSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_mset_parse_odd_number_of_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"value1")),
        RespFrame::BulkString(Bytes::from_static(b"key2")),
    ];
    let err = MSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_mset_parse_non_bulk_string_arg() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::Integer(123),
    ];
    let err = MSet::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
