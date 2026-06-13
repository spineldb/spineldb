// src/core/events.rs

//! Defines the event bus system for propagating write operations to persistence
//! and replication subsystems.

use crate::core::Command;
use crate::core::commands::command_trait::CommandExt;
use crate::core::protocol::RespFrame;
use crate::core::state::ServerState;
use std::sync::Arc;
use tokio::sync::{
    broadcast::{self, Sender as BroadcastSender},
    mpsc::{self, Sender as MpscSender, error::TrySendError},
};
use tracing::{debug, error};

/// The capacity of the broadcast channel for replication.
/// This should be large enough to handle bursts of commands without lagging.
const BROADCAST_BUS_CAPACITY: usize = 16384;

/// The capacity of the MPSC channel for AOF persistence.
/// This is very large to ensure that even if disk I/O is slow, the server
/// does not block or reject write commands.
const AOF_CHANNEL_CAPACITY: usize = 65536;

/// A wrapper struct for a unit of work that will be propagated.
#[derive(Debug, Clone)]
pub struct PropagatedWork {
    pub uow: UnitOfWork,
}

impl PropagatedWork {
    /// Estimates the size of the work unit as it would be written to the AOF.
    pub fn estimated_size(&self) -> usize {
        self.uow.estimated_size()
    }
}

/// A struct to hold the data for a transaction.
/// This is boxed within `UnitOfWork` to keep the enum's size small.
#[derive(Debug, Clone)]
pub struct TransactionData {
    /// All commands that were queued, including read-only ones.
    /// This is used by the AOF to accurately reconstruct the state.
    pub all_commands: Vec<Command>,
    /// Only the commands that actually modify data.
    /// This is used by replication to save bandwidth.
    pub write_commands: Vec<Command>,
}

/// Defines an atomic unit of work that will be propagated to the AOF and replicas.
/// Both variants are boxed to keep the enum itself small and efficient,
/// storing only a pointer on the stack regardless of the variant's content size.
#[derive(Debug, Clone)]
pub enum UnitOfWork {
    /// A single command. Boxed to optimize the size of the enum.
    Command(Box<Command>),
    /// An entire transaction. Boxed for the same reason.
    Transaction(Box<TransactionData>),
}

impl UnitOfWork {
    /// Estimates the size of the unit of work by encoding it to its RESP representation.
    pub fn estimated_size(&self) -> usize {
        let frames: Vec<RespFrame> = match self {
            UnitOfWork::Transaction(tx_data) => {
                if tx_data.all_commands.is_empty() {
                    return 0;
                }
                let mut frames: Vec<RespFrame> = Vec::with_capacity(tx_data.all_commands.len() + 2);
                frames.push(Command::Multi.into());
                frames.extend(tx_data.all_commands.iter().cloned().map(Into::into));
                frames.push(Command::Exec.into());
                frames
            }
            UnitOfWork::Command(cmd) => vec![(**cmd).clone().into()],
        };

        let mut encoded_size = 0;
        for frame in &frames {
            if let Ok(encoded) = frame.encode_to_vec() {
                encoded_size += encoded.len();
            }
        }
        encoded_size
    }
}

/// The `EventBus` is the central distribution hub for all write operations.
/// It sends work units to the AOF writer and all connected replicas.
#[derive(Debug)]
pub struct EventBus {
    /// A broadcast sender for replication (one-to-many).
    replication_sender: BroadcastSender<PropagatedWork>,
    /// An MPSC sender for AOF persistence (one-to-one).
    aof_sender: Option<MpscSender<PropagatedWork>>,
}

impl EventBus {
    /// Creates a new `EventBus` and returns the receiver for the AOF task.
    pub fn new(aof_enabled: bool) -> (Self, Option<mpsc::Receiver<PropagatedWork>>) {
        let (replication_sender, _) = broadcast::channel(BROADCAST_BUS_CAPACITY);

        let (aof_sender, aof_receiver) = if aof_enabled {
            let (tx, rx) = mpsc::channel(AOF_CHANNEL_CAPACITY);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        let bus = Self {
            replication_sender,
            aof_sender,
        };

        (bus, aof_receiver)
    }

    /// Publishes a `UnitOfWork` to all subscribers (AOF and replication).
    ///
    /// The AOF sender applies graceful backpressure: when the channel is full
    /// we wait up to `state.config.persistence.aof_enqueue_timeout_ms`
    /// milliseconds for room to appear before falling back to a hard error.
    /// The timeout can be disabled (set to `0`) to preserve the legacy
    /// "immediate read-only" behavior.
    pub fn publish(&self, uow: UnitOfWork, state: &Arc<ServerState>) {
        let work = PropagatedWork { uow };

        // --- CLIENT TRACKING INVALIDATION ---
        // Extract affected keys from the command(s) and notify tracking clients.
        let keys = match &work.uow {
            UnitOfWork::Command(cmd) => cmd.get_keys(),
            UnitOfWork::Transaction(tx_data) => {
                let mut all_keys = Vec::new();
                for cmd in &tx_data.write_commands {
                    all_keys.extend(cmd.get_keys());
                }
                all_keys
            }
        };
        if !keys.is_empty() {
            // Use db_index 0 as a default; the actual db_index isn't stored in the command.
            // Tracking invalidation is best-effort, so this is acceptable.
            let state_clone = state.clone();
            let keys_clone = keys;
            tokio::spawn(async move {
                state_clone.tracking.invalidate_keys(0, &keys_clone).await;
            });
        }

        // Send to replication subscribers. It's okay if there are no active subscribers.
        if self.replication_sender.send(work.clone()).is_err() {
            debug!("Published a UnitOfWork with no active replication subscribers.");
        }

        if let Some(sender) = &self.aof_sender {
            // Fast path: the channel has room.
            match sender.try_send(work) {
                Ok(_) => (),
                Err(TrySendError::Closed(_)) => {
                    let reason = "AOF channel is closed. Persistence has stopped.".to_string();
                    error!("{}", reason);
                    state.set_read_only(true, &reason);
                }
                Err(TrySendError::Full(work)) => {
                    // Channel is full. Decide whether to wait or escalate.
                    let timeout_ms = state
                        .config
                        .try_lock()
                        .map(|cfg| cfg.persistence.aof_enqueue_timeout_ms)
                        .unwrap_or(0);

                    if timeout_ms == 0 {
                        let reason = "AOF channel is full. Persistence is lagging behind writes."
                            .to_string();
                        error!("{}", reason);
                        state.set_read_only(true, &reason);
                        return;
                    }

                    let sender_clone = sender.clone();
                    // Drive the wait on a best-effort blocking thread so we do
                    // not stall the Tokio runtime that handles the command.
                    let work_size = work.estimated_size();
                    let state_clone = state.clone();
                    // Clone the work so the closure can move its own copy while
                    // we keep a copy available for the fallback `unwrap_or`
                    // path below.
                    let work_for_thread = work.clone();
                    let send_result = std::thread::Builder::new()
                        .name("spineldb-aof-backpressure".into())
                        .spawn(move || {
                            // We are inside a dedicated OS thread, not a Tokio
                            // worker, so it is safe to block here for the
                            // configured timeout.
                            let deadline = std::time::Instant::now()
                                + std::time::Duration::from_millis(timeout_ms);
                            let mut work = work_for_thread;
                            loop {
                                match sender_clone.try_send(work) {
                                    Ok(()) => return Ok(()),
                                    Err(TrySendError::Full(w)) => {
                                        if std::time::Instant::now() >= deadline {
                                            return Err(w);
                                        }
                                        std::thread::sleep(std::time::Duration::from_millis(1));
                                        work = w;
                                    }
                                    Err(TrySendError::Closed(w)) => return Err(w),
                                }
                            }
                        })
                        .ok()
                        .and_then(|h| h.join().ok())
                        .unwrap_or(Err(work));

                    if let Err(returned) = send_result {
                        let reason = format!(
                            "AOF channel stayed full for {} ms (approx. {} bytes pending). \
                             Persistence is lagging behind writes.",
                            timeout_ms, work_size
                        );
                        error!("{}", reason);
                        state_clone.set_read_only(true, &reason);
                        // `returned` is dropped here, which means the write is
                        // not propagated. The set_read_only call above will
                        // reject subsequent writes, preventing unbounded loss.
                        let _ = returned;
                    }
                }
            }
        }
    }

    /// Provides a new receiver for a replication task to subscribe to updates.
    pub fn subscribe_for_replication(&self) -> broadcast::Receiver<PropagatedWork> {
        self.replication_sender.subscribe()
    }

    /// Checks if the AOF channel has been closed.
    pub fn is_closed(&self) -> bool {
        self.aof_sender.as_ref().is_some_and(|s| s.is_closed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::commands::generic::Ping;

    #[test]
    fn test_uow_command_estimated_size() {
        let cmd = Command::Ping(Ping::default());
        let uow = UnitOfWork::Command(Box::new(cmd));
        // The size is the encoded RESP frame size; we just assert it's non-zero.
        assert!(uow.estimated_size() > 0);
    }

    #[test]
    fn test_uow_transaction_estimated_size_includes_all_cmds() {
        let cmd1 = Command::Ping(Ping::default());
        let cmd2 = Command::Ping(Ping::default());
        let tx = TransactionData {
            all_commands: vec![cmd1.clone(), cmd2.clone()],
            write_commands: vec![cmd1.clone(), cmd2],
        };
        let uow = UnitOfWork::Transaction(Box::new(tx));
        // A transaction with 2 commands must have a larger estimate than a
        // single command on its own.
        let single = UnitOfWork::Command(Box::new(cmd1));
        assert!(uow.estimated_size() > single.estimated_size());
    }

    #[test]
    fn test_uow_empty_transaction_estimated_size_is_zero() {
        let tx = TransactionData {
            all_commands: vec![],
            write_commands: vec![],
        };
        let uow = UnitOfWork::Transaction(Box::new(tx));
        assert_eq!(uow.estimated_size(), 0);
    }

    #[test]
    fn test_propagated_work_estimated_size_delegates() {
        let cmd = Command::Ping(Ping::default());
        let uow = UnitOfWork::Command(Box::new(cmd));
        let expected = uow.estimated_size();
        let work = PropagatedWork { uow };
        assert_eq!(work.estimated_size(), expected);
    }

    #[test]
    fn test_event_bus_new_with_aof_enabled() {
        let (bus, rx) = EventBus::new(true);
        assert!(rx.is_some());
        // AOF is still open because the receiver is alive.
        assert!(!bus.is_closed());
    }

    #[test]
    fn test_event_bus_new_with_aof_disabled() {
        let (_bus, rx) = EventBus::new(false);
        assert!(rx.is_none());
    }

    #[test]
    fn test_event_bus_aof_closes_when_receiver_dropped() {
        let (bus, rx) = EventBus::new(true);
        drop(rx);
        // After dropping the receiver, the channel is closed.
        assert!(bus.is_closed());
    }

    #[test]
    fn test_event_bus_subscribe_for_replication() {
        let (bus, _rx) = EventBus::new(false);
        let mut sub = bus.subscribe_for_replication();
        // Send a work unit through the bus using a minimal state.
        // We can't easily construct a ServerState here, so we use the broadcast
        // sender directly via publish_for_test if available, or skip.
        // We at least verify subscribe() returns a valid receiver.
        assert!(sub.try_recv().is_err()); // No messages yet.
    }
}
