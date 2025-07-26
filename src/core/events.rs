// src/core/events.rs

//! Defines the event bus system for propagating write operations to persistence
//! and replication subsystems.

use crate::core::Command;
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
    pub fn publish(&self, uow: UnitOfWork, state: &Arc<ServerState>) {
        let work = PropagatedWork { uow };

        // Send to replication subscribers. It's okay if there are no active subscribers.
        if self.replication_sender.send(work.clone()).is_err() {
            debug!("Published a UnitOfWork with no active replication subscribers.");
        }

        if let Some(sender) = &self.aof_sender {
            match sender.try_send(work) {
                Ok(_) => {}
                Err(TrySendError::Full(_)) => {
                    let reason =
                        "AOF channel is full. Persistence is lagging behind writes.".to_string();
                    error!("{}", reason);
                    state.set_read_only(true, &reason);
                }
                Err(TrySendError::Closed(_)) => {
                    let reason = "AOF channel is closed. Persistence has stopped.".to_string();
                    error!("{}", reason);
                    state.set_read_only(true, &reason);
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
