use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::json::json_set::{JsonSet, SetCondition};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_json_set_parse_basic() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"$.a")),
        RespFrame::BulkString(Bytes::from_static(b"{\"x\": 1}")),
    ];
    let cmd = JsonSet::parse(&args).unwrap();
    assert_eq!(cmd.key, Bytes::from_static(b"mykey"));
    assert_eq!(cmd.path, "$.a");
    assert_eq!(cmd.value_json_str, Bytes::from_static(b"{\"x\": 1}"));
    assert_eq!(cmd.condition, SetCondition::None);
}

#[tokio::test]
async fn test_json_set_parse_with_nx() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"$.a")),
        RespFrame::BulkString(Bytes::from_static(b"123")),
        RespFrame::BulkString(Bytes::from_static(b"NX")),
    ];
    let cmd = JsonSet::parse(&args).unwrap();
    assert_eq!(cmd.condition, SetCondition::IfNotExists);
}

#[tokio::test]
async fn test_json_set_parse_with_xx() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"$.a")),
        RespFrame::BulkString(Bytes::from_static(b"true")),
        RespFrame::BulkString(Bytes::from_static(b"XX")),
    ];
    let cmd = JsonSet::parse(&args).unwrap();
    assert_eq!(cmd.condition, SetCondition::IfExists);
}

#[tokio::test]
async fn test_json_set_parse_lowercase_condition() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"$.a")),
        RespFrame::BulkString(Bytes::from_static(b"\"hello\"")),
        RespFrame::BulkString(Bytes::from_static(b"nx")),
    ];
    let cmd = JsonSet::parse(&args).unwrap();
    assert_eq!(cmd.condition, SetCondition::IfNotExists);
}

#[tokio::test]
async fn test_json_set_parse_invalid_condition() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"$.a")),
        RespFrame::BulkString(Bytes::from_static(b"[]")),
        RespFrame::BulkString(Bytes::from_static(b"ZZ")),
    ];
    let err = JsonSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("SyntaxError"));
}

#[tokio::test]
async fn test_json_set_parse_too_few_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"$.a")),
    ];
    let err = JsonSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_json_set_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"$.a")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"NX")),
        RespFrame::BulkString(Bytes::from_static(b"EXTRA")),
    ];
    let err = JsonSet::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}
