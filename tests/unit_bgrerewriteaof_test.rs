use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::bgrerewriteaof::BgRewriteAof;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_bgrewriteaof_parse_no_args() {
    let args = [];
    let bgrewriteaof_command_result = BgRewriteAof::parse(&args);
    assert!(bgrewriteaof_command_result.is_ok());
}

#[tokio::test]
async fn test_bgrewriteaof_parse_with_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"extra_arg"))];
    let err = BgRewriteAof::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_bgrewriteaof_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = BgRewriteAof::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongArgumentCount(_))
    ));
}
