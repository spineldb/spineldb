use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::string::set::{Set, SetCondition, TtlOption};
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_set_parse_basic() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
    ];
    let set_command = Set::parse(&args).unwrap();
    assert_eq!(set_command.key, Bytes::from_static(b"mykey"));
    assert_eq!(set_command.value, Bytes::from_static(b"myvalue"));
    assert_eq!(set_command.condition, SetCondition::None);
    assert!(matches!(set_command.ttl, TtlOption::None));
}

#[tokio::test]
async fn test_set_parse_with_nx() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"NX")),
    ];
    let set_command = Set::parse(&args).unwrap();
    assert_eq!(set_command.condition, SetCondition::IfNotExists);
}

#[tokio::test]
async fn test_set_parse_with_xx() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"XX")),
    ];
    let set_command = Set::parse(&args).unwrap();
    assert_eq!(set_command.condition, SetCondition::IfExists);
}

#[tokio::test]
async fn test_set_parse_with_ex() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"EX")),
        RespFrame::BulkString(Bytes::from_static(b"3600")),
    ];
    let set_command = Set::parse(&args).unwrap();
    assert!(matches!(set_command.ttl, TtlOption::Seconds(3600)));
}

#[tokio::test]
async fn test_set_parse_with_px() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"PX")),
        RespFrame::BulkString(Bytes::from_static(b"90000")),
    ];
    let set_command = Set::parse(&args).unwrap();
    assert!(matches!(set_command.ttl, TtlOption::Milliseconds(90000)));
}

#[tokio::test]
async fn test_set_parse_with_get() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ];
    let set_command = Set::parse(&args).unwrap();
    assert!(set_command.get);
}

#[tokio::test]
async fn test_set_parse_all_options() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"NX")),
        RespFrame::BulkString(Bytes::from_static(b"PX")),
        RespFrame::BulkString(Bytes::from_static(b"12345")),
        RespFrame::BulkString(Bytes::from_static(b"GET")),
    ];
    let set_command = Set::parse(&args).unwrap();
    assert_eq!(set_command.condition, SetCondition::IfNotExists);
    assert!(matches!(set_command.ttl, TtlOption::Milliseconds(12345)));
    assert!(set_command.get);
}

#[tokio::test]
async fn test_set_parse_invalid_option() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"ZZ")),
    ];
    let err = Set::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("SyntaxError"));
}

#[tokio::test]
async fn test_set_parse_missing_value_for_ex() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"EX")),
    ];
    let err = Set::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("SyntaxError"));
}

#[tokio::test]
async fn test_set_parse_not_enough_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"mykey"))];
    let err = Set::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_set_parse_conflicting_conditions() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"NX")),
        RespFrame::BulkString(Bytes::from_static(b"XX")),
    ];
    let err = Set::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("SyntaxError"));
}

#[tokio::test]
async fn test_set_parse_conflicting_ttl() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
        RespFrame::BulkString(Bytes::from_static(b"myvalue")),
        RespFrame::BulkString(Bytes::from_static(b"EX")),
        RespFrame::BulkString(Bytes::from_static(b"10")),
        RespFrame::BulkString(Bytes::from_static(b"PX")),
        RespFrame::BulkString(Bytes::from_static(b"20000")),
    ];
    let err = Set::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("SyntaxError"));
}
