use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::psync::Psync;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_psync_parse_valid_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"?")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
    ];
    let psync_command = Psync::parse(&args).unwrap();
    assert_eq!(psync_command.replication_id, "?");
    assert_eq!(psync_command.offset, "-1");
}
/*
#[tokio::test]
async fn test_psync_parse_no_args() {
    let args = [];
    let err = Psync::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_psync_parse_missing_one_arg() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"?"))];
    let err = Psync::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_psync_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"?")),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Psync::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_psync_parse_non_bulk_string_replication_id() {
    let args = [
        RespFrame::Integer(123),
        RespFrame::BulkString(Bytes::from_static(b"-1")),
    ];
    let err = Psync::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_psync_parse_non_bulk_string_offset() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"?")),
        RespFrame::Integer(123),
    ];
    let err = Psync::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
*/
