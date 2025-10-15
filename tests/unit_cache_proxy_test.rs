use bytes::Bytes;
use spineldb::core::commands::cache::cache_proxy::CacheProxy;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::protocol::RespFrame;

#[tokio::test]
async fn test_proxy_parse_simple_url_as_key() {
    let url = "http://example.com/data";
    let args = [
        RespFrame::BulkString(Bytes::from(url)),
        RespFrame::BulkString(Bytes::from_static(b"TTL")),
        RespFrame::BulkString(Bytes::from_static(b"60")),
    ];
    let proxy_command = CacheProxy::parse(&args).unwrap();

    // The key should be the URL itself
    assert_eq!(proxy_command.key, Bytes::from(url));

    // The URL should be automatically inferred from the key
    assert_eq!(proxy_command.url, Some(url.to_string()));

    // The TTL should be parsed correctly
    assert_eq!(proxy_command.ttl, Some(60));
}

#[tokio::test]
async fn test_proxy_parse_separate_key_and_url() {
    let key = "my-key";
    let url = "http://example.com/data";
    let args = [
        RespFrame::BulkString(Bytes::from(key)),
        RespFrame::BulkString(Bytes::from(url)),
        RespFrame::BulkString(Bytes::from_static(b"TTL")),
        RespFrame::BulkString(Bytes::from_static(b"120")),
    ];
    let proxy_command = CacheProxy::parse(&args).unwrap();

    // The key should be "my-key"
    assert_eq!(proxy_command.key, Bytes::from(key));

    // The URL should be explicitly set
    assert_eq!(proxy_command.url, Some(url.to_string()));

    // The TTL should be parsed correctly
    assert_eq!(proxy_command.ttl, Some(120));
}

#[tokio::test]
async fn test_proxy_parse_key_not_a_url() {
    let key = "not-a-url";
    let args = [
        RespFrame::BulkString(Bytes::from(key)),
        RespFrame::BulkString(Bytes::from_static(b"TTL")),
        RespFrame::BulkString(Bytes::from_static(b"60")),
    ];
    let proxy_command = CacheProxy::parse(&args).unwrap();

    // The key should be "not-a-url"
    assert_eq!(proxy_command.key, Bytes::from(key));

    // The URL should be None because the key is not a URL and no explicit URL was provided
    assert_eq!(proxy_command.url, None);

    // The TTL should be parsed correctly
    assert_eq!(proxy_command.ttl, Some(60));
}
