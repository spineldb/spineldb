use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::list::linsert::{InsertPosition, LInsert};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_linsert_parse_valid_before() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"BEFORE")),
        RespFrame::BulkString(Bytes::from_static(b"pivot_value")),
        RespFrame::BulkString(Bytes::from_static(b"new_element")),
    ];
    let linsert_command = LInsert::parse(&args).unwrap();
    assert_eq!(linsert_command.key, Bytes::from_static(b"mylist"));
    assert_eq!(linsert_command.position, InsertPosition::Before);
    assert_eq!(linsert_command.pivot, Bytes::from_static(b"pivot_value"));
    assert_eq!(linsert_command.element, Bytes::from_static(b"new_element"));
}

#[tokio::test]
async fn test_linsert_parse_valid_after() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"AFTER")),
        RespFrame::BulkString(Bytes::from_static(b"pivot_value")),
        RespFrame::BulkString(Bytes::from_static(b"new_element")),
    ];
    let linsert_command = LInsert::parse(&args).unwrap();
    assert_eq!(linsert_command.key, Bytes::from_static(b"mylist"));
    assert_eq!(linsert_command.position, InsertPosition::After);
    assert_eq!(linsert_command.pivot, Bytes::from_static(b"pivot_value"));
    assert_eq!(linsert_command.element, Bytes::from_static(b"new_element"));
}

#[tokio::test]
async fn test_linsert_parse_no_args() {
    let args = [];
    let err = LInsert::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_linsert_parse_too_few_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"BEFORE")),
        RespFrame::BulkString(Bytes::from_static(b"pivot_value")),
    ];
    let err = LInsert::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_linsert_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"BEFORE")),
        RespFrame::BulkString(Bytes::from_static(b"pivot_value")),
        RespFrame::BulkString(Bytes::from_static(b"new_element")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = LInsert::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_linsert_parse_invalid_position() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"MIDDLE")),
        RespFrame::BulkString(Bytes::from_static(b"pivot_value")),
        RespFrame::BulkString(Bytes::from_static(b"new_element")),
    ];
    let err = LInsert::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("SyntaxError"));
}

#[tokio::test]
async fn test_linsert_parse_non_bulk_string_key() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"BEFORE")),
        RespFrame::BulkString(Bytes::from_static(b"pivot_value")),
        RespFrame::BulkString(Bytes::from_static(b"new_element")),
    ];
    let err = LInsert::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_linsert_parse_non_bulk_string_pivot() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"BEFORE")),
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"new_element")),
    ];
    let err = LInsert::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_linsert_parse_non_bulk_string_element() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mylist")),
        RespFrame::BulkString(Bytes::from_static(b"BEFORE")),
        RespFrame::BulkString(Bytes::from_static(b"pivot_value")),
        RespFrame::Integer(456),
    ];
    let err = LInsert::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
