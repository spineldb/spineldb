// src/server/spawner.rs

//! Spawns all of the server's long-running background tasks.

use super::context::ServerContext;
use super::metrics_server;
use crate::config::AppendFsync;
use crate::core::cluster;
use crate::core::persistence::AofWriterTask;
use crate::core::persistence::spldb_saver::SpldbSaverTask;
use crate::core::pubsub::channel_purger::ChannelPurgerTask;
use crate::core::replication;
use crate::core::storage::ttl::TtlManager;
use crate::core::tasks::{
    cache_gc::OnDiskCacheGCTask,
    cache_purger::CachePurgerTask,
    cache_revalidator::{CacheRevalidationWorker, CacheRevalidator},
    cache_tag_validator::CacheTagValidatorTask,
    eviction::EvictionManager,
    lazy_free::LazyFreeManager,
    persistence::AofRewriteManager,
};
use anyhow::{Result, anyhow};
use std::time::Duration;
use tracing::info;

/// Spawns all critical background tasks into the provided JoinSet.
pub async fn spawn_all(ctx: &mut ServerContext) -> Result<()> {
    let server_state = &ctx.state;
    let shutdown_tx = &ctx.shutdown_tx;
    let background_tasks = &mut ctx.background_tasks;

    let server_init = std::mem::replace(
        &mut ctx.init_channels,
        crate::core::state::ServerInit {
            state: server_state.clone(),
            aof_event_rx: None,
            aof_fsync_request_rx: tokio::sync::mpsc::channel(1).1,
            aof_rewrite_complete_rx: tokio::sync::watch::channel(()).1,
            lazy_free_rx: tokio::sync::mpsc::channel(1).1,
            cluster_gossip_rx: tokio::sync::mpsc::channel(1).1,
            replication_reconfigure_rx: tokio::sync::broadcast::channel(1).1,
            cache_revalidation_rx: tokio::sync::mpsc::channel(1).1,
        },
    );

    let config_clone = server_state.config.lock().await.clone();

    // --- Metrics Server ---
    if config_clone.metrics.enabled {
        let metrics_state = server_state.clone();
        let shutdown_rx_metrics = shutdown_tx.subscribe();
        background_tasks.spawn(async move {
            metrics_server::run_metrics_server(metrics_state, shutdown_rx_metrics).await;
            Ok(())
        });
    } else {
        info!("Prometheus metrics server is disabled in the configuration.");
    }

    // --- Core Maintenance Tasks ---
    let ttl_manager = TtlManager::new(server_state.dbs.clone());
    let shutdown_rx_ttl = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        ttl_manager.run(shutdown_rx_ttl).await;
        Ok(())
    });

    let eviction_manager = EvictionManager::new(server_state.clone());
    let shutdown_rx_evict = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        eviction_manager.run(shutdown_rx_evict).await;
        Ok(())
    });

    let lazy_free_manager = LazyFreeManager {
        state: server_state.clone(),
        rx: server_init.lazy_free_rx,
    };
    let shutdown_rx_lazy = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        lazy_free_manager.run(shutdown_rx_lazy).await;
        Ok(())
    });

    let purger = ChannelPurgerTask::new(server_state.clone());
    let shutdown_rx_purge = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        purger.run(shutdown_rx_purge).await;
        Ok(())
    });

    // --- Intelligent Caching Tasks ---
    let revalidation_worker = CacheRevalidationWorker {
        state: server_state.clone(),
        rx: server_init.cache_revalidation_rx,
    };
    let shutdown_rx_reval_worker = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        revalidation_worker.run(shutdown_rx_reval_worker).await;
        Ok(())
    });

    let revalidator = CacheRevalidator::new(server_state.clone());
    let shutdown_rx_revalidator = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        revalidator.run(shutdown_rx_revalidator).await;
        Ok(())
    });

    if !config_clone.cache.on_disk_path.is_empty() {
        let gc_task = OnDiskCacheGCTask::new(server_state.clone());
        let shutdown_rx_gc = shutdown_tx.subscribe();
        background_tasks.spawn(async move {
            gc_task.run(shutdown_rx_gc).await;
            Ok(())
        });
    }

    let cache_purger = CachePurgerTask::new(server_state.clone());
    let shutdown_rx_cache_purge = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        cache_purger.run(shutdown_rx_cache_purge).await;
        Ok(())
    });

    // Spawn the new cache tag validator task for cluster mode.
    let cache_validator = CacheTagValidatorTask::new(server_state.clone());
    let shutdown_rx_cache_validate = shutdown_tx.subscribe();
    background_tasks.spawn(async move {
        cache_validator.run(shutdown_rx_cache_validate).await;
        Ok(())
    });

    // --- Persistence Tasks ---
    if config_clone.persistence.aof_enabled {
        let aof_rewrite_manager = AofRewriteManager::new(server_state.clone());
        let shutdown_rx_aof_manager = shutdown_tx.subscribe();
        background_tasks.spawn(async move {
            aof_rewrite_manager.run(shutdown_rx_aof_manager).await;
            Ok(())
        });

        let aof_rx = server_init
            .aof_event_rx
            .expect("AOF receiver must exist when AOF is enabled");

        let writer = AofWriterTask::new(
            server_state.clone(),
            aof_rx,
            server_init.aof_fsync_request_rx,
            server_init.aof_rewrite_complete_rx,
        )
        .await?;

        if config_clone.persistence.appendfsync == AppendFsync::EverySec {
            let fsync_state = server_state.clone();
            let mut fsync_shutdown = shutdown_tx.subscribe();
            background_tasks.spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            if fsync_state.persistence.aof_fsync_request_tx.send(()).await.is_err() {
                                break;
                            }
                        },
                        _ = fsync_shutdown.recv() => {
                            break;
                        }
                    }
                }
                Ok(())
            });
        }

        let shutdown_rx_aof = shutdown_tx.subscribe();
        background_tasks.spawn(async move {
            writer.run(shutdown_rx_aof).await?;
            Ok(())
        });
    }

    if config_clone.persistence.spldb_enabled && !config_clone.persistence.save_rules.is_empty() {
        let spldb_saver = SpldbSaverTask::new(server_state.clone());
        let shutdown_rx_spldb = shutdown_tx.subscribe();
        background_tasks.spawn(async move {
            spldb_saver.run(shutdown_rx_spldb).await;
            Ok(())
        });
    }

    // --- Cluster / Replication Task ---
    if config_clone.cluster.enabled {
        let state_clone = server_state.clone();
        let shutdown_rx_cluster = shutdown_tx.subscribe();
        background_tasks.spawn(async move {
            let bus_port = {
                let config_guard = state_clone.config.lock().await;
                let cluster_config = &config_guard.cluster;
                cluster_config
                    .announce_bus_port
                    .unwrap_or(config_guard.port + cluster_config.bus_port_offset)
            };
            cluster::gossip::run(
                state_clone,
                bus_port,
                shutdown_rx_cluster,
                server_init.cluster_gossip_rx,
            )
            .await;
            Ok(())
        });
    } else {
        let repl_state = server_state.clone();
        let shutdown_rx_repl = shutdown_tx.subscribe();
        background_tasks.spawn(async move {
            replication::setup_replication(
                repl_state,
                shutdown_rx_repl,
                server_init.replication_reconfigure_rx,
            )
            .await?
            .await
            .map_err(|e| anyhow!("Replication task panicked: {:?}", e))
        });
    }

    info!("All background tasks have been spawned.");
    Ok(())
}
