use bytes::Bytes;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::commands::generic::evalsha::EvalSha;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_evalsha_parse_no_keys_no_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let evalsha_command = EvalSha::parse(&args).unwrap();
    assert_eq!(
        evalsha_command.sha1,
        "abcdef0123456789abcdef0123456789abcdef01"
    );
    assert_eq!(evalsha_command.num_keys, 0);
    assert!(evalsha_command.keys.is_empty());
    assert!(evalsha_command.args.is_empty());
}

#[tokio::test]
async fn test_evalsha_parse_some_keys_no_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"mykey")),
    ];
    let evalsha_command = EvalSha::parse(&args).unwrap();
    assert_eq!(
        evalsha_command.sha1,
        "abcdef0123456789abcdef0123456789abcdef01"
    );
    assert_eq!(evalsha_command.num_keys, 1);
    assert_eq!(evalsha_command.keys, vec![Bytes::from_static(b"mykey")]);
    assert!(evalsha_command.args.is_empty());
}

#[tokio::test]
async fn test_evalsha_parse_no_keys_some_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::BulkString(Bytes::from_static(b"myarg")),
    ];
    let evalsha_command = EvalSha::parse(&args).unwrap();
    assert_eq!(
        evalsha_command.sha1,
        "abcdef0123456789abcdef0123456789abcdef01"
    );
    assert_eq!(evalsha_command.num_keys, 0);
    assert!(evalsha_command.keys.is_empty());
    assert_eq!(evalsha_command.args, vec![Bytes::from_static(b"myarg")]);
}

#[tokio::test]
async fn test_evalsha_parse_some_keys_some_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::BulkString(Bytes::from_static(b"key1")),
        RespFrame::BulkString(Bytes::from_static(b"arg1")),
        RespFrame::BulkString(Bytes::from_static(b"arg2")),
    ];
    let evalsha_command = EvalSha::parse(&args).unwrap();
    assert_eq!(
        evalsha_command.sha1,
        "abcdef0123456789abcdef0123456789abcdef01"
    );
    assert_eq!(evalsha_command.num_keys, 1);
    assert_eq!(evalsha_command.keys, vec![Bytes::from_static(b"key1")]);
    assert_eq!(
        evalsha_command.args,
        vec![Bytes::from_static(b"arg1"), Bytes::from_static(b"arg2")]
    );
}

#[tokio::test]
async fn test_evalsha_parse_less_than_two_args() {
    let args = [RespFrame::BulkString(Bytes::from_static(
        b"abcdef0123456789abcdef0123456789abcdef01",
    ))];
    let err = EvalSha::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("WrongArgumentCount"));
}

#[tokio::test]
async fn test_evalsha_parse_num_keys_greater_than_available_args() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"2")),
        RespFrame::BulkString(Bytes::from_static(b"key1")),
    ];
    let err = EvalSha::parse(&args).unwrap_err();
    assert!(format!("{:?}", err).contains("InvalidState"));
}

#[tokio::test]
async fn test_evalsha_parse_sha1_not_bulk_string() {
    let args = [
        RespFrame::SimpleString("abcdef0123456789abcdef0123456789abcdef01".to_string()),
        RespFrame::BulkString(Bytes::from_static(b"0")),
    ];
    let err = EvalSha::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_evalsha_parse_num_keys_not_integer() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"not_an_int")),
    ];
    let err = EvalSha::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::NotAnInteger));
}

#[tokio::test]
async fn test_evalsha_parse_key_not_bulk_string() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"1")),
        RespFrame::Integer(123),
    ];
    let err = EvalSha::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}

#[tokio::test]
async fn test_evalsha_parse_arg_not_bulk_string() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(
            b"abcdef0123456789abcdef0123456789abcdef01",
        )),
        RespFrame::BulkString(Bytes::from_static(b"0")),
        RespFrame::Integer(123),
    ];
    let err = EvalSha::parse(&args).unwrap_err();
    assert!(matches!(err, spineldb::core::SpinelDBError::WrongType));
}
