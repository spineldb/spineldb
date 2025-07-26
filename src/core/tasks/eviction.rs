// src/core/tasks/eviction.rs

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::config::EvictionPolicy;
use crate::core::state::ServerState;

/// A task responsible for proactive memory eviction.
pub struct EvictionManager {
    state: Arc<ServerState>,
}

impl EvictionManager {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }

    /// Runs the main loop for the eviction manager, using a Redis-like active eviction algorithm.
    pub async fn run(self, mut shutdown_rx: broadcast::Receiver<()>) {
        let (maxmemory, policy) = {
            let config = self.state.config.lock().await;
            (config.maxmemory, config.maxmemory_policy)
        };

        let maxmemory = match maxmemory {
            Some(m) if m > 0 && policy != EvictionPolicy::NoEviction => m,
            _ => {
                info!(
                    "Eviction manager will not run (maxmemory is 0, not set, or policy is 'noeviction')."
                );
                return;
            }
        };

        info!(
            "Proactive eviction manager started. Policy: {:?}. Max memory: {} bytes.",
            policy, maxmemory
        );
        let mut interval = tokio::time::interval(Duration::from_millis(100));

        let mut unproductive_eviction_attempts = 0u64;
        const MAX_UNPRODUCTIVE_ATTEMPTS: u64 = 600;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let total_memory: usize = self.state.dbs.iter().map(|db| db.get_current_memory()).sum();

                    if total_memory > maxmemory {
                        if unproductive_eviction_attempts >= MAX_UNPRODUCTIVE_ATTEMPTS {
                            if unproductive_eviction_attempts == MAX_UNPRODUCTIVE_ATTEMPTS {
                                warn!(
                                    "Memory usage is still above maxmemory, but eviction has been unproductive for {} attempts. \
                                    Pausing proactive eviction until memory usage drops or suitable keys are added.",
                                    MAX_UNPRODUCTIVE_ATTEMPTS
                                );
                                unproductive_eviction_attempts += 1;
                            }
                            continue;
                        }

                        if unproductive_eviction_attempts > 5 {
                            tokio::time::sleep(Duration::from_millis(100 * unproductive_eviction_attempts.min(50))).await;
                        }

                        let memory_freed = self.perform_eviction_cycle(maxmemory).await;

                        if memory_freed == 0 {
                            unproductive_eviction_attempts += 1;
                            debug!("Eviction cycle was unproductive. Increasing backoff counter to {}.", unproductive_eviction_attempts);
                        } else {
                            unproductive_eviction_attempts = 0;
                        }
                    } else if unproductive_eviction_attempts > 0 {

                        info!("Memory usage is now below maxmemory. Resuming normal eviction checks.");
                        unproductive_eviction_attempts = 0;
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Eviction manager shutting down.");
                    return;
                }
            }
        }
    }

    /// Performs a single, time-boxed eviction cycle, similar to Redis.
    async fn perform_eviction_cycle(&self, maxmemory: usize) -> usize {
        const TIME_LIMIT: Duration = Duration::from_millis(1);
        let start_time = Instant::now();
        let memory_before: usize = self
            .state
            .dbs
            .iter()
            .map(|db| db.get_current_memory())
            .sum();

        loop {
            let total_memory: usize = self
                .state
                .dbs
                .iter()
                .map(|db| db.get_current_memory())
                .sum();

            if total_memory <= maxmemory {
                break;
            }
            if start_time.elapsed() > TIME_LIMIT {
                break;
            }

            let mut evicted_in_pass = false;
            for db in &self.state.dbs {
                if db.get_key_count() > 0 && db.evict_one_key(&self.state).await {
                    evicted_in_pass = true;
                }
            }

            if !evicted_in_pass {
                break;
            }
        }

        let memory_after: usize = self
            .state
            .dbs
            .iter()
            .map(|db| db.get_current_memory())
            .sum();
        memory_before.saturating_sub(memory_after)
    }
}
