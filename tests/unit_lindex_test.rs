use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::list::lindex::LIndex;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_lindex_parse_valid() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let lindex_command = LIndex::parse(&args).unwrap();
    assert_eq!(lindex_command.key, Bytes::from_static(b"mylist"));
    assert_eq!(lindex_command.index, 0);
}

#[tokio::test]
async fn test_lindex_parse_negative_index() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
    ];
    let lindex_command = LIndex::parse(&args).unwrap();
    assert_eq!(lindex_command.key, Bytes::from_static(b"mylist"));
    assert_eq!(lindex_command.index, -1);
}

#[tokio::test]
async fn test_lindex_parse_no_args() {
    let args = [];
    let err = LIndex::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_lindex_parse_too_few_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mylist"))];
    let err = LIndex::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_lindex_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = LIndex::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_lindex_parse_non_integer_index() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"not-an-int")),
    ];
    let err = LIndex::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_lindex_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let err = LIndex::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
