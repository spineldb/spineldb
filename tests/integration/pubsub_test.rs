// tests/integration/pubsub_test.rs

//! Integration tests for Pub/Sub commands
//! Tests: PUBLISH, PUBSUB (CHANNELS, NUMSUB, NUMPAT)

use super::test_helpers::TestContext;
use bytes::Bytes;
use spineldb::core::Command;
use spineldb::core::RespValue;
use spineldb::core::protocol::RespFrame;

// ===== PUBLISH Command Tests =====

#[tokio::test]
async fn test_publish_to_empty_channel() {
    let ctx = TestContext::new().await;

    // Publish to a channel with no subscribers
    let result = ctx.publish("news:sports", "Team A wins!").await.unwrap();

    // Should return 0 (no subscribers)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 0),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_basic() {
    let ctx = TestContext::new().await;

    // Subscribe to a channel (using pubsub manager directly)
    let channel = Bytes::from("test:channel");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    // Publish a message
    let result = ctx.publish("test:channel", "Hello, World!").await.unwrap();

    // Should return 1 (one subscriber)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 1),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_multiple_subscribers() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("news:updates");

    // Create multiple subscribers
    let _receiver1 = ctx.state.pubsub.subscribe(&channel);
    let _receiver2 = ctx.state.pubsub.subscribe(&channel);
    let _receiver3 = ctx.state.pubsub.subscribe(&channel);

    // Publish a message
    let result = ctx.publish("news:updates", "Breaking news!").await.unwrap();

    // Should return 3 (three subscribers)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 3),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_empty_message() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("empty:channel");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    // Publish an empty message
    let result = ctx.publish("empty:channel", "").await.unwrap();

    match result {
        RespValue::Integer(count) => assert_eq!(count, 1),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_large_message() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("large:channel");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    // Create a large message (1KB)
    let large_message = "x".repeat(1024);
    let result = ctx.publish("large:channel", &large_message).await.unwrap();

    match result {
        RespValue::Integer(count) => assert_eq!(count, 1),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_unicode_message() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("unicode:channel");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    // Publish a message with unicode characters
    let unicode_message = "Hello 世界 🌍";
    let result = ctx
        .publish("unicode:channel", unicode_message)
        .await
        .unwrap();

    match result {
        RespValue::Integer(count) => assert_eq!(count, 1),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_binary_message() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("binary:channel");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    // Publish binary data
    let binary_data = vec![0x00, 0x01, 0xFF, 0xAB, 0xCD];
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"PUBLISH")),
        RespFrame::BulkString(Bytes::from("binary:channel")),
        RespFrame::BulkString(Bytes::from(binary_data.clone())),
    ]))
    .unwrap();

    let result = ctx.execute(command).await.unwrap();

    match result {
        RespValue::Integer(count) => assert_eq!(count, 1),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_multiple_channels() {
    let ctx = TestContext::new().await;

    let channel1 = Bytes::from("channel:1");
    let channel2 = Bytes::from("channel:2");
    let _channel3 = Bytes::from("channel:3");

    let _receiver1 = ctx.state.pubsub.subscribe(&channel1);
    let _receiver2 = ctx.state.pubsub.subscribe(&channel2);
    // channel3 has no subscribers

    // Publish to each channel
    let result1 = ctx.publish("channel:1", "Message 1").await.unwrap();
    let result2 = ctx.publish("channel:2", "Message 2").await.unwrap();
    let result3 = ctx.publish("channel:3", "Message 3").await.unwrap();

    match (result1, result2, result3) {
        (RespValue::Integer(c1), RespValue::Integer(c2), RespValue::Integer(c3)) => {
            assert_eq!(c1, 1);
            assert_eq!(c2, 1);
            assert_eq!(c3, 0);
        }
        _ => panic!("Expected integer responses"),
    }
}

// ===== Pattern Subscription Tests =====

#[tokio::test]
async fn test_publish_with_pattern_subscriber() {
    let ctx = TestContext::new().await;

    // Subscribe to a pattern
    let pattern = Bytes::from("news:*");
    let _pattern_receiver = ctx.state.pubsub.subscribe_pattern(&pattern);

    // Publish to a channel matching the pattern
    let result = ctx.publish("news:sports", "Sports update").await.unwrap();

    // Should return 1 (pattern subscriber)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 1),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_with_both_direct_and_pattern_subscribers() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("news:sports");
    let pattern = Bytes::from("news:*");

    // Create both direct and pattern subscribers
    let _direct_receiver = ctx.state.pubsub.subscribe(&channel);
    let _pattern_receiver = ctx.state.pubsub.subscribe_pattern(&pattern);

    // Publish a message
    let result = ctx.publish("news:sports", "Breaking news!").await.unwrap();

    // Should return 2 (one direct + one pattern subscriber)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 2),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_pattern_multiple_matches() {
    let ctx = TestContext::new().await;

    let pattern = Bytes::from("news:*");
    let _pattern_receiver = ctx.state.pubsub.subscribe_pattern(&pattern);

    // Publish to multiple channels matching the pattern
    let result1 = ctx.publish("news:sports", "Sports news").await.unwrap();
    let result2 = ctx.publish("news:finance", "Finance news").await.unwrap();
    let result3 = ctx.publish("news:tech", "Tech news").await.unwrap();

    match (result1, result2, result3) {
        (RespValue::Integer(c1), RespValue::Integer(c2), RespValue::Integer(c3)) => {
            assert_eq!(c1, 1);
            assert_eq!(c2, 1);
            assert_eq!(c3, 1);
        }
        _ => panic!("Expected integer responses"),
    }
}

#[tokio::test]
async fn test_publish_pattern_no_match() {
    let ctx = TestContext::new().await;

    let pattern = Bytes::from("news:*");
    let _pattern_receiver = ctx.state.pubsub.subscribe_pattern(&pattern);

    // Publish to a channel that doesn't match the pattern
    let result = ctx.publish("updates:sports", "Update").await.unwrap();

    // Should return 0 (pattern doesn't match)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 0),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_publish_multiple_patterns() {
    let ctx = TestContext::new().await;

    let pattern1 = Bytes::from("news:*");
    let pattern2 = Bytes::from("*:sports");

    let _receiver1 = ctx.state.pubsub.subscribe_pattern(&pattern1);
    let _receiver2 = ctx.state.pubsub.subscribe_pattern(&pattern2);

    // Publish to a channel matching both patterns
    let result = ctx
        .publish("news:sports", "Match both patterns")
        .await
        .unwrap();

    // Should return 2 (both patterns match)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 2),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

// ===== PUBSUB CHANNELS Command Tests =====

#[tokio::test]
async fn test_pubsub_channels_empty() {
    let ctx = TestContext::new().await;

    // No channels subscribed yet
    let result = ctx.pubsub_channels(None).await.unwrap();

    match result {
        RespValue::Array(channels) => assert_eq!(channels.len(), 0),
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_channels_single() {
    let ctx = TestContext::new().await;

    // Subscribe to a channel
    let channel = Bytes::from("test:channel");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    let result = ctx.pubsub_channels(None).await.unwrap();

    match result {
        RespValue::Array(channels) => {
            assert_eq!(channels.len(), 1);
            assert_eq!(
                channels[0],
                RespValue::BulkString(Bytes::from("test:channel"))
            );
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_channels_multiple() {
    let ctx = TestContext::new().await;

    // Subscribe to multiple channels
    let channels = vec![
        Bytes::from("news:sports"),
        Bytes::from("news:finance"),
        Bytes::from("updates:tech"),
    ];

    for channel in &channels {
        let _receiver = ctx.state.pubsub.subscribe(channel);
    }

    let result = ctx.pubsub_channels(None).await.unwrap();

    match result {
        RespValue::Array(channels_array) => {
            assert_eq!(channels_array.len(), 3);
            // Verify all channels are present (order may vary)
            let channel_names: Vec<Bytes> = channels_array
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(bs) = v {
                        Some(bs.clone())
                    } else {
                        None
                    }
                })
                .collect();

            assert!(channel_names.contains(&Bytes::from("news:sports")));
            assert!(channel_names.contains(&Bytes::from("news:finance")));
            assert!(channel_names.contains(&Bytes::from("updates:tech")));
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_channels_with_pattern() {
    let ctx = TestContext::new().await;

    // Subscribe to multiple channels
    let channels = vec![
        Bytes::from("news:sports"),
        Bytes::from("news:finance"),
        Bytes::from("updates:tech"),
    ];

    for channel in &channels {
        let _receiver = ctx.state.pubsub.subscribe(channel);
    }

    // Filter by pattern
    let result = ctx.pubsub_channels(Some("news:*")).await.unwrap();

    match result {
        RespValue::Array(channels_array) => {
            assert_eq!(channels_array.len(), 2);
            let channel_names: Vec<Bytes> = channels_array
                .iter()
                .filter_map(|v| {
                    if let RespValue::BulkString(bs) = v {
                        Some(bs.clone())
                    } else {
                        None
                    }
                })
                .collect();

            assert!(channel_names.contains(&Bytes::from("news:sports")));
            assert!(channel_names.contains(&Bytes::from("news:finance")));
            assert!(!channel_names.contains(&Bytes::from("updates:tech")));
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_channels_pattern_no_match() {
    let ctx = TestContext::new().await;

    // Subscribe to channels
    let channel = Bytes::from("news:sports");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    // Filter by pattern that doesn't match
    let result = ctx.pubsub_channels(Some("updates:*")).await.unwrap();

    match result {
        RespValue::Array(channels_array) => {
            assert_eq!(channels_array.len(), 0);
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

// ===== PUBSUB NUMSUB Command Tests =====

#[tokio::test]
async fn test_pubsub_numsub_empty() {
    let ctx = TestContext::new().await;

    // No channels provided
    let result = ctx.pubsub_numsub(&[]).await.unwrap();

    match result {
        RespValue::Array(arr) => assert_eq!(arr.len(), 0),
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_numsub_single_channel() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("test:channel");
    let _receiver = ctx.state.pubsub.subscribe(&channel);

    let result = ctx.pubsub_numsub(&["test:channel"]).await.unwrap();

    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2); // [channel_name, count]
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("test:channel")));
            assert_eq!(arr[1], RespValue::Integer(1));
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_numsub_multiple_subscribers() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("news:updates");

    // Create multiple subscribers
    let _receiver1 = ctx.state.pubsub.subscribe(&channel);
    let _receiver2 = ctx.state.pubsub.subscribe(&channel);
    let _receiver3 = ctx.state.pubsub.subscribe(&channel);

    let result = ctx.pubsub_numsub(&["news:updates"]).await.unwrap();

    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("news:updates")));
            assert_eq!(arr[1], RespValue::Integer(3));
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_numsub_multiple_channels() {
    let ctx = TestContext::new().await;

    let channel1 = Bytes::from("channel:1");
    let channel2 = Bytes::from("channel:2");
    let _channel3 = Bytes::from("channel:3");

    // Subscribe to channels with different counts
    let _r1 = ctx.state.pubsub.subscribe(&channel1);
    let _r2 = ctx.state.pubsub.subscribe(&channel1); // 2 subscribers
    let _r3 = ctx.state.pubsub.subscribe(&channel2); // 1 subscriber
    // channel3 has 0 subscribers

    let result = ctx
        .pubsub_numsub(&["channel:1", "channel:2", "channel:3"])
        .await
        .unwrap();

    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 6); // 3 channels * 2 (name + count)
            assert_eq!(arr[0], RespValue::BulkString(Bytes::from("channel:1")));
            assert_eq!(arr[1], RespValue::Integer(2));
            assert_eq!(arr[2], RespValue::BulkString(Bytes::from("channel:2")));
            assert_eq!(arr[3], RespValue::Integer(1));
            assert_eq!(arr[4], RespValue::BulkString(Bytes::from("channel:3")));
            assert_eq!(arr[5], RespValue::Integer(0));
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_numsub_nonexistent_channel() {
    let ctx = TestContext::new().await;

    // Query a channel that doesn't exist
    let result = ctx.pubsub_numsub(&["nonexistent:channel"]).await.unwrap();

    match result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 2);
            assert_eq!(
                arr[0],
                RespValue::BulkString(Bytes::from("nonexistent:channel"))
            );
            assert_eq!(arr[1], RespValue::Integer(0));
        }
        _ => panic!("Expected array response, got {:?}", result),
    }
}

// ===== PUBSUB NUMPAT Command Tests =====

#[tokio::test]
async fn test_pubsub_numpat_empty() {
    let ctx = TestContext::new().await;

    // No pattern subscriptions
    let result = ctx.pubsub_numpat().await.unwrap();

    match result {
        RespValue::Integer(count) => assert_eq!(count, 0),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_numpat_single() {
    let ctx = TestContext::new().await;

    let pattern = Bytes::from("news:*");
    let _receiver = ctx.state.pubsub.subscribe_pattern(&pattern);

    let result = ctx.pubsub_numpat().await.unwrap();

    match result {
        RespValue::Integer(count) => assert_eq!(count, 1),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

#[tokio::test]
async fn test_pubsub_numpat_multiple() {
    let ctx = TestContext::new().await;

    let patterns = vec![
        Bytes::from("news:*"),
        Bytes::from("*:sports"),
        Bytes::from("updates:*"),
    ];

    for pattern in &patterns {
        let _receiver = ctx.state.pubsub.subscribe_pattern(pattern);
    }

    let result = ctx.pubsub_numpat().await.unwrap();

    match result {
        RespValue::Integer(count) => assert_eq!(count, 3),
        _ => panic!("Expected integer response, got {:?}", result),
    }
}

// ===== Edge Cases and Error Handling =====

#[tokio::test]
async fn test_publish_invalid_args() {
    let _ctx = TestContext::new().await;

    // PUBLISH with no arguments should fail
    let command = Command::try_from(RespFrame::Array(vec![RespFrame::BulkString(
        Bytes::from_static(b"PUBLISH"),
    )]));

    assert!(command.is_err());
}

#[tokio::test]
async fn test_publish_missing_message() {
    let _ctx = TestContext::new().await;

    // PUBLISH with only channel, no message
    let command = Command::try_from(RespFrame::Array(vec![
        RespFrame::BulkString(Bytes::from_static(b"PUBLISH")),
        RespFrame::BulkString(Bytes::from("channel")),
    ]));

    assert!(command.is_err());
}

#[tokio::test]
async fn test_pubsub_channels_after_unsubscribe() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("temp:channel");
    let receiver = ctx.state.pubsub.subscribe(&channel);

    // Verify channel exists
    let result = ctx.pubsub_channels(None).await.unwrap();
    match result {
        RespValue::Array(channels) => assert_eq!(channels.len(), 1),
        _ => panic!("Expected array"),
    }

    // Drop the receiver (simulating unsubscribe)
    drop(receiver);

    // Channel should still be listed until purged
    // Note: purge_empty_channels is called by background task, so channel may still exist
    let result = ctx.pubsub_channels(None).await.unwrap();
    match result {
        RespValue::Array(channels) => {
            // Channel may or may not be purged yet depending on timing
            assert!(channels.len() <= 1);
        }
        _ => panic!("Expected array"),
    }
}

#[tokio::test]
async fn test_pubsub_complex_scenario() {
    let ctx = TestContext::new().await;

    // Create a complex scenario with multiple channels and patterns
    let channels = vec![
        Bytes::from("news:sports"),
        Bytes::from("news:finance"),
        Bytes::from("updates:tech"),
    ];

    let patterns = vec![Bytes::from("news:*"), Bytes::from("*:tech")];

    // Subscribe to channels - keep receivers alive
    let mut channel_receivers = Vec::new();
    for channel in &channels {
        channel_receivers.push(ctx.state.pubsub.subscribe(channel));
    }

    // Subscribe to patterns - keep receivers alive
    let mut pattern_receivers = Vec::new();
    for pattern in &patterns {
        pattern_receivers.push(ctx.state.pubsub.subscribe_pattern(pattern));
    }

    // Publish to a channel that matches both a direct subscription and a pattern
    let result = ctx.publish("news:sports", "Complex message").await.unwrap();

    // Should have 1 direct subscriber + 1 pattern subscriber (news:*)
    match result {
        RespValue::Integer(count) => assert_eq!(count, 2),
        _ => panic!("Expected integer"),
    }

    // Check channels (while receivers are still alive)
    let channels_result = ctx.pubsub_channels(None).await.unwrap();
    match channels_result {
        RespValue::Array(arr) => assert_eq!(arr.len(), 3),
        _ => panic!("Expected array"),
    }

    // Check pattern count (while receivers are still alive)
    let numpat_result = ctx.pubsub_numpat().await.unwrap();
    match numpat_result {
        RespValue::Integer(count) => assert_eq!(count, 2),
        _ => panic!("Expected integer"),
    }

    // Check subscriber counts (while receivers are still alive)
    let numsub_result = ctx
        .pubsub_numsub(&["news:sports", "news:finance", "updates:tech"])
        .await
        .unwrap();
    match numsub_result {
        RespValue::Array(arr) => {
            assert_eq!(arr.len(), 6);
            // news:sports should have 1 subscriber
            assert_eq!(arr[1], RespValue::Integer(1));
            // news:finance should have 1 subscriber
            assert_eq!(arr[3], RespValue::Integer(1));
            // updates:tech should have 1 subscriber
            assert_eq!(arr[5], RespValue::Integer(1));
        }
        _ => panic!("Expected array"),
    }

    // Keep receivers alive until end of test
    drop(channel_receivers);
    drop(pattern_receivers);
}

// ===== PubSub Manager Direct Tests =====

#[tokio::test]
async fn test_pubsub_manager_new() {
    let _ctx = TestContext::new().await;

    // Test that we can create a new pubsub manager
    let manager = spineldb::core::pubsub::PubSubManager::new();
    assert_eq!(manager.get_all_channels().len(), 0);
    assert_eq!(manager.get_pattern_subscriber_count(), 0);
}

#[tokio::test]
async fn test_pubsub_purge_empty_channels() {
    let ctx = TestContext::new().await;

    // Create subscriptions
    let channel1 = Bytes::from("temp:channel1");
    let channel2 = Bytes::from("temp:channel2");
    let pattern = Bytes::from("temp:*");

    let receiver1 = ctx.state.pubsub.subscribe(&channel1);
    let receiver2 = ctx.state.pubsub.subscribe(&channel2);
    let pattern_receiver = ctx.state.pubsub.subscribe_pattern(&pattern);

    // Verify channels exist
    let channels = ctx.state.pubsub.get_all_channels();
    assert!(channels.contains(&channel1));
    assert!(channels.contains(&channel2));
    assert_eq!(ctx.state.pubsub.get_pattern_subscriber_count(), 1);

    // Drop receivers to make channels empty
    drop(receiver1);
    drop(receiver2);
    drop(pattern_receiver);

    // Purge empty channels
    let purged = ctx.state.pubsub.purge_empty_channels();

    // Should have purged 2 channels + 1 pattern = 3
    assert_eq!(purged, 3);

    // Verify channels are gone
    let channels_after = ctx.state.pubsub.get_all_channels();
    assert!(!channels_after.contains(&channel1));
    assert!(!channels_after.contains(&channel2));
    assert_eq!(ctx.state.pubsub.get_pattern_subscriber_count(), 0);
}

#[tokio::test]
async fn test_pubsub_purge_partial() {
    let ctx = TestContext::new().await;

    let channel1 = Bytes::from("keep:channel1");
    let channel2 = Bytes::from("remove:channel2");

    let receiver1 = ctx.state.pubsub.subscribe(&channel1);
    let receiver2 = ctx.state.pubsub.subscribe(&channel2);

    // Drop one receiver
    drop(receiver2);

    // Purge - should only remove channel2
    let purged = ctx.state.pubsub.purge_empty_channels();
    assert_eq!(purged, 1);

    // channel1 should still exist
    let channels = ctx.state.pubsub.get_all_channels();
    assert!(channels.contains(&channel1));
    assert!(!channels.contains(&channel2));

    drop(receiver1);
}

#[tokio::test]
async fn test_pubsub_purge_nothing() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("active:channel");
    let receiver = ctx.state.pubsub.subscribe(&channel);

    // Purge with active subscribers - should purge nothing
    let purged = ctx.state.pubsub.purge_empty_channels();
    assert_eq!(purged, 0);

    // Channel should still exist
    let channels = ctx.state.pubsub.get_all_channels();
    assert!(channels.contains(&channel));

    drop(receiver);
}

#[tokio::test]
async fn test_pubsub_get_subscriber_count() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("count:channel");

    // No subscribers initially
    assert_eq!(ctx.state.pubsub.get_subscriber_count(&channel), 0);

    // Add subscribers
    let receiver1 = ctx.state.pubsub.subscribe(&channel);
    assert_eq!(ctx.state.pubsub.get_subscriber_count(&channel), 1);

    let receiver2 = ctx.state.pubsub.subscribe(&channel);
    assert_eq!(ctx.state.pubsub.get_subscriber_count(&channel), 2);

    let receiver3 = ctx.state.pubsub.subscribe(&channel);
    assert_eq!(ctx.state.pubsub.get_subscriber_count(&channel), 3);

    // Drop one
    drop(receiver1);
    assert_eq!(ctx.state.pubsub.get_subscriber_count(&channel), 2);

    drop(receiver2);
    drop(receiver3);
}

#[tokio::test]
async fn test_pubsub_get_pattern_subscriber_count() {
    let ctx = TestContext::new().await;

    // No patterns initially
    assert_eq!(ctx.state.pubsub.get_pattern_subscriber_count(), 0);

    let pattern1 = Bytes::from("pattern:*");
    let _receiver1 = ctx.state.pubsub.subscribe_pattern(&pattern1);
    assert_eq!(ctx.state.pubsub.get_pattern_subscriber_count(), 1);

    let pattern2 = Bytes::from("*:test");
    let _receiver2 = ctx.state.pubsub.subscribe_pattern(&pattern2);
    assert_eq!(ctx.state.pubsub.get_pattern_subscriber_count(), 2);

    // Note: pattern count is based on unique patterns, not receivers
    let _receiver3 = ctx.state.pubsub.subscribe_pattern(&pattern1);
    assert_eq!(ctx.state.pubsub.get_pattern_subscriber_count(), 2);
}

#[tokio::test]
async fn test_pubsub_resubscribe_same_channel() {
    let ctx = TestContext::new().await;

    let channel = Bytes::from("resub:channel");

    // Subscribe multiple times to same channel
    let receiver1 = ctx.state.pubsub.subscribe(&channel);
    let receiver2 = ctx.state.pubsub.subscribe(&channel);
    let receiver3 = ctx.state.pubsub.subscribe(&channel);

    // All should be counted
    assert_eq!(ctx.state.pubsub.get_subscriber_count(&channel), 3);

    // Publish should reach all
    let result = ctx.publish("resub:channel", "Message").await.unwrap();
    match result {
        RespValue::Integer(count) => assert_eq!(count, 3),
        _ => panic!("Expected integer"),
    }

    drop(receiver1);
    drop(receiver2);
    drop(receiver3);
}

#[tokio::test]
async fn test_pubsub_resubscribe_same_pattern() {
    let ctx = TestContext::new().await;

    let pattern = Bytes::from("resub:*");

    // Subscribe multiple times to same pattern
    let receiver1 = ctx.state.pubsub.subscribe_pattern(&pattern);
    let receiver2 = ctx.state.pubsub.subscribe_pattern(&pattern);
    let receiver3 = ctx.state.pubsub.subscribe_pattern(&pattern);

    // Pattern count should be 1 (unique patterns)
    assert_eq!(ctx.state.pubsub.get_pattern_subscriber_count(), 1);

    // Publish should reach all pattern subscribers
    let result = ctx.publish("resub:test", "Message").await.unwrap();
    match result {
        RespValue::Integer(count) => assert_eq!(count, 3),
        _ => panic!("Expected integer"),
    }

    drop(receiver1);
    drop(receiver2);
    drop(receiver3);
}
