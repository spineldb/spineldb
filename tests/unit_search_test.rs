// tests/unit_search_test.rs

use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::search::{
    FtCreateCommand, FtDropCommand, FtInfoCommand, FtSearchCommand,
};
use spineldb::core::protocol::RespFrame;
use spineldb::core::types::SpinelString;

#[test]
fn test_parse_ft_create_valid() {
    let args = [
        RespFrame::BulkString("products-idx".into()),
        RespFrame::BulkString("ON".into()),
        RespFrame::BulkString("HASH".into()),
        RespFrame::BulkString("PREFIX".into()),
        RespFrame::BulkString("1".into()),
        RespFrame::BulkString("product:".into()),
        RespFrame::BulkString("SCHEMA".into()),
        RespFrame::BulkString("name".into()),
        RespFrame::BulkString("TEXT".into()),
        RespFrame::BulkString("price".into()),
        RespFrame::BulkString("NUMERIC".into()),
    ];
    let spinel_args: Vec<SpinelString> =
        args.iter().map(|f| f.as_bytes().unwrap().into()).collect();
    let cmd = FtCreateCommand::parse(&spinel_args).unwrap();
    assert_eq!(cmd.index_name, "products-idx");
    assert_eq!(cmd.schema.fields.len(), 2);
}

#[test]
fn test_parse_ft_create_invalid() {
    let args = [RespFrame::BulkString("products-idx".into())];
    let spinel_args: Vec<SpinelString> =
        args.iter().map(|f| f.as_bytes().unwrap().into()).collect();
    let err = FtCreateCommand::parse(&spinel_args).unwrap_err();
    assert!(matches!(
        err,
        spineldb::core::SpinelDBError::WrongArgumentCount(_)
    ));
}

#[test]
fn test_parse_ft_drop_valid() {
    let args = [RespFrame::BulkString("products-idx".into())];
    let spinel_args: Vec<SpinelString> =
        args.iter().map(|f| f.as_bytes().unwrap().into()).collect();
    let cmd = FtDropCommand::parse(&spinel_args).unwrap();
    assert_eq!(cmd.index_name, "products-idx");
}

#[test]
fn test_parse_ft_drop_invalid() {
    let args = [];
    let spinel_args: Vec<SpinelString> =
        args.iter().map(|f| f.as_bytes().unwrap().into()).collect();
    let err = FtDropCommand::parse(&spinel_args).unwrap_err();
    assert!(matches!(
        err,
        spineldb::core::SpinelDBError::WrongArgumentCount(_)
    ));
}

#[test]
fn test_parse_ft_info_valid() {
    let args = [RespFrame::BulkString("products-idx".into())];
    let spinel_args: Vec<SpinelString> =
        args.iter().map(|f| f.as_bytes().unwrap().into()).collect();
    let cmd = FtInfoCommand::parse(&spinel_args).unwrap();
    assert_eq!(cmd.index_name, "products-idx");
}

#[test]
fn test_parse_ft_search_valid() {
    let args = [
        RespFrame::BulkString("products-idx".into()),
        RespFrame::BulkString("laptop @price:[100 200]".into()),
    ];
    let spinel_args: Vec<SpinelString> =
        args.iter().map(|f| f.as_bytes().unwrap().into()).collect();
    let cmd = FtSearchCommand::parse(&spinel_args).unwrap();
    assert_eq!(cmd.index_name, "products-idx");
    assert_eq!(cmd.query, "laptop @price:[100 200]");
}

#[test]
fn test_parse_ft_search_invalid() {
    let args = [RespFrame::BulkString("products-idx".into())];
    let spinel_args: Vec<SpinelString> =
        args.iter().map(|f| f.as_bytes().unwrap().into()).collect();
    let err = FtSearchCommand::parse(&spinel_args).unwrap_err();
    assert!(matches!(
        err,
        spineldb::core::SpinelDBError::WrongArgumentCount(_)
    ));
}
