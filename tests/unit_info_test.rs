use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::info::Info;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_info_parse_no_args() {
    let args = [];
    let info_command = Info::parse(&args).unwrap();
    assert!(info_command.section.is_none());
}

#[tokio::test]
async fn test_info_parse_with_section() {
    let section = "server";
    let args = [RespFrame::BulkString(Bytes::from_static(
        section.as_bytes(),
    ))];
    let info_command = Info::parse(&args).unwrap();
    assert_eq!(info_command.section, Some(section.to_string()));
}

#[tokio::test]
async fn test_info_parse_with_section_uppercase() {
    let section = "SERVER";
    let args = [RespFrame::BulkString(Bytes::from_static(
        section.as_bytes(),
    ))];
    let info_command = Info::parse(&args).unwrap();
    assert_eq!(info_command.section, Some(section.to_lowercase()));
}

#[tokio::test]
async fn test_info_parse_too_many_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"server")),
        RespFrame::BulkString(Bytes::from_static(b"extra")),
    ];
    let err = Info::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_info_parse_non_bulk_string_arg() {
    let args = [RespFrame::Integer(123)];
    let result = Info::parse(&args);
    assert!(matches!(
        result,
        Err(spineldb::core::SpinelDBError::WrongType)
    ));
}
