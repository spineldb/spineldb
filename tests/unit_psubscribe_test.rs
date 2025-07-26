use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::psubscribe::PSubscribe;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_psubscribe_parse_valid_patterns() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"channel1.*")),
        RespFrame::BulkString(Bytes::from_static(b"news.*")),
    ];
    let psubscribe_command = PSubscribe::parse(&args).unwrap();
    assert_eq!(
        psubscribe_command.patterns,
        vec![
            Bytes::from_static(b"channel1.*"),
            Bytes::from_static(b"news.*")
        ]
    );
}

#[tokio::test]
async fn test_psubscribe_parse_no_args() {
    let args = [];
    let err = PSubscribe::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_psubscribe_parse_non_bulk_string_arg() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"channel1.*")),
        RespFrame::Integer(123),
    ];
    let err = PSubscribe::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
