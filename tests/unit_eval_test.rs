use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::eval::Eval;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_eval_parse_no_keys_no_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"return 1")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let eval_command = Eval::parse(&args).unwrap();
    assert_eq!(eval_command.script, Bytes::from_static(b"return 1"));
    assert_eq!(eval_command.num_keys, 0);
    assert!(eval_command.keys.is_empty());
    assert!(eval_command.args.is_empty());
}

#[tokio::test]
async fn test_eval_parse_some_keys_no_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"return KEYS[1]")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
    ];
    let eval_command = Eval::parse(&args).unwrap();
    assert_eq!(eval_command.script, Bytes::from_static(b"return KEYS[1]"));
    assert_eq!(eval_command.num_keys, 1);
    assert_eq!(eval_command.keys, vec![Bytes::from_static(b"mykey")]);
    assert!(eval_command.args.is_empty());
}

#[tokio::test]
async fn test_eval_parse_no_keys_some_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"return ARGV[1]")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"myarg")),
    ];
    let eval_command = Eval::parse(&args).unwrap();
    assert_eq!(eval_command.script, Bytes::from_static(b"return ARGV[1]"));
    assert_eq!(eval_command.num_keys, 0);
    assert!(eval_command.keys.is_empty());
    assert_eq!(eval_command.args, vec![Bytes::from_static(b"myarg")]);
}

#[tokio::test]
async fn test_eval_parse_some_keys_some_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"return KEYS[1] .. ARGV[1]")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"arg1")),
        RespFrame::BulkString(Bytes::from_static(b"arg2")),
    ];
    let eval_command = Eval::parse(&args).unwrap();
    assert_eq!(
        eval_command.script,
        Bytes::from_static(b"return KEYS[1] .. ARGV[1]")
    );
    assert_eq!(eval_command.num_keys, 1);
    assert_eq!(eval_command.keys, vec![Bytes::from_static(b"key1")]);
    assert_eq!(
        eval_command.args,
        vec![Bytes::from_static(b"arg1"), Bytes::from_static(b"arg2")]
    );
}

#[tokio::test]
async fn test_eval_parse_less_than_two_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(b"script"))];
    let err = Eval::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_eval_parse_num_keys_greater_than_available_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"script")),
        RespFrame::BulkString(Bytes::from_static(b"2")),
        RespFrame::BulkString(Bytes::from_static(b"key1")),
    ];
    let err = Eval::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_eval_parse_script_not_bulk_string() {
    let args = [
        RespFrame::SimpleString("script".to_string()),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let err = Eval::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_eval_parse_num_keys_not_integer() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"script")),
        RespFrame::BulkString(Bytes::from_static(b"not_an_int")),
    ];
    let err = Eval::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_eval_parse_key_not_bulk_string() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"script")),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::Integer(123),
    ];
    let err = Eval::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_eval_parse_arg_not_bulk_string() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"script")),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::Integer(123),
    ];
    let err = Eval::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
