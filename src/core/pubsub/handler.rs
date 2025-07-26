// src/core/pubsub/handler.rs

//! Handles a client connection that has entered Pub/Sub mode.

use crate::connection::{SessionState, SubscriptionReceiver};
use crate::core::SpinelDBError;
use crate::core::protocol::{RespFrame, RespFrameCodec, RespValue};
use crate::core::state::ServerState;
use futures::{SinkExt, future::FutureExt};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::broadcast;
use tokio_util::codec::Framed;
use tracing::{debug, warn};

/// Manages a connection that is in Pub/Sub mode.
/// In this mode, the connection can only receive messages and a limited
/// set of commands (`(P)UNSUBSCRIBE`, `QUIT`).
pub struct PubSubModeHandler<'a, S: AsyncRead + AsyncWrite + Unpin> {
    framed: &'a mut Framed<S, RespFrameCodec>,
    shutdown_rx: &'a mut broadcast::Receiver<()>,
    session: &'a mut SessionState,
    state: Arc<ServerState>,
}

impl<'a, S: AsyncRead + AsyncWrite + Unpin> PubSubModeHandler<'a, S> {
    pub fn new(
        framed: &'a mut Framed<S, RespFrameCodec>,
        shutdown_rx: &'a mut broadcast::Receiver<()>,
        session: &'a mut SessionState,
        state: Arc<ServerState>,
    ) -> Self {
        Self {
            framed,
            shutdown_rx,
            session,
            state,
        }
    }

    /// Runs a loop that exclusively listens for broadcast messages from subscribed
    /// channels/patterns and shutdown signals.
    pub async fn run(&mut self) -> Result<(), SpinelDBError> {
        debug!("Connection entering Pub/Sub mode loop.");
        loop {
            // If the client unsubscribes from all channels/patterns, exit Pub/Sub mode.
            if self.session.subscribed_channels.is_empty()
                && self.session.subscribed_patterns.is_empty()
            {
                debug!("No more subscriptions, exiting Pub/Sub mode.");
                self.session.is_subscribed = false;
                self.session.is_pattern_subscribed = false;
                return Ok(());
            }

            tokio::select! {
                biased;
                // Prioritize shutdown signals.
                _ = self.shutdown_rx.recv() => { return Ok(()); }
                // Wait for a message from any of the subscribed receivers.
                maybe_msg = receive_pubsub_message_static(&mut self.session.pubsub_receivers) => {
                    match maybe_msg {
                        Some(Ok(frame)) => {
                            // Forward the message to the client.
                            if self.framed.send(frame).await.is_err() {
                                warn!("Failed to send pubsub message to client. Connection likely closed.");
                                return Ok(());
                            }
                        }

                        Some(Err(broadcast::error::RecvError::Lagged(num_lagged))) => {
                            // The client's receiver is too slow and missed messages.
                            // To recover, we re-subscribe to all channels to get a fresh receiver.
                            warn!("Pub/Sub receiver lagged for client, missed {} messages. Re-subscribing to continue.", num_lagged);
                            self.resubscribe_all();
                        }

                        Some(Err(broadcast::error::RecvError::Closed)) => {
                            // This should not happen unless the EventBus is dropped, which means a server shutdown.
                            warn!("A Pub/Sub broadcast channel was closed. Exiting pub/sub mode.");
                            return Ok(());
                        }
                        None => {
                            // This case can happen if the receivers list is empty but the loop hasn't exited yet.
                            // A small sleep prevents a tight loop.
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }
    }

    /// Re-subscribes to all of the session's channels and patterns.
    /// This is a recovery mechanism for when a `broadcast::Receiver` lags.
    fn resubscribe_all(&mut self) {
        let old_receivers = std::mem::take(&mut self.session.pubsub_receivers);

        for sub_receiver in old_receivers {
            match sub_receiver {
                SubscriptionReceiver::Channel(name, _) => {
                    let new_rx = self.state.pubsub.subscribe(&name);
                    self.session
                        .pubsub_receivers
                        .push(SubscriptionReceiver::Channel(name, new_rx));
                }
                SubscriptionReceiver::Pattern(pattern, _) => {
                    let new_rx = self.state.pubsub.subscribe_pattern(&pattern);
                    self.session
                        .pubsub_receivers
                        .push(SubscriptionReceiver::Pattern(pattern, new_rx));
                }
            }
        }
        debug!(
            "Re-subscribed to {} channels/patterns after receiver lag.",
            self.session.pubsub_receivers.len()
        );
    }
}

/// A static async function that uses `select_all` to wait for a message from any
/// of the provided `SubscriptionReceiver`s.
async fn receive_pubsub_message_static(
    pubsub_receivers: &mut [SubscriptionReceiver],
) -> Option<Result<RespFrame, broadcast::error::RecvError>> {
    if pubsub_receivers.is_empty() {
        return None;
    }

    // `select_all` polls a list of futures and returns the first one that completes.
    let select_all = futures::future::select_all(pubsub_receivers.iter_mut().map(|sub_receiver| {
        async move {
            match sub_receiver {
                // For channel subscriptions, format the message as `(message, channel_name, message_body)`.
                SubscriptionReceiver::Channel(name, rx) => rx.recv().await.map(|msg| {
                    RespValue::Array(vec![
                        RespValue::BulkString("message".into()),
                        RespValue::BulkString(name.clone()),
                        RespValue::BulkString(msg),
                    ])
                }),
                // For pattern subscriptions, format as `(pmessage, pattern, channel_name, message_body)`.
                SubscriptionReceiver::Pattern(pattern, rx) => {
                    // Correctly handle the Result before destructuring the tuple.
                    rx.recv().await.map(|pmsg_result| {
                        let (_p, chan, msg) = pmsg_result;
                        RespValue::Array(vec![
                            RespValue::BulkString("pmessage".into()),
                            RespValue::BulkString(pattern.clone()),
                            RespValue::BulkString(chan),
                            RespValue::BulkString(msg),
                        ])
                    })
                }
            }
        }
        .boxed() // Box the future to create a homogenous type for `select_all`.
    }));

    let (recv_result, _index, _remaining) = select_all.await;

    // Convert the `RespValue` to a `RespFrame`.
    match recv_result {
        Ok(resp_value) => Some(Ok(resp_value.into())),
        Err(e) => Some(Err(e)),
    }
}
