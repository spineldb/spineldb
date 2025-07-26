// src/core/commands/cluster/nodes.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx.state.cluster.as_ref().unwrap();
    let mut output = String::new();

    for entry in cluster.nodes.iter() {
        let runtime_state = entry.value();
        let node = &runtime_state.node_info;

        let flags_str = node
            .get_flags()
            .iter_names()
            .map(|(name, _)| name.to_lowercase())
            .collect::<Vec<_>>()
            .join(",");

        let slots_str = if node.slots.is_empty() {
            "".to_string()
        } else {
            let mut ranges = vec![];
            let mut sorted_slots: Vec<_> = node.slots.iter().cloned().collect();
            sorted_slots.sort_unstable();

            if !sorted_slots.is_empty() {
                let mut iter = sorted_slots.into_iter();
                let mut start = iter.next().unwrap();
                let mut end = start;
                for slot in iter {
                    if slot == end + 1 {
                        end = slot;
                    } else {
                        ranges.push(if start == end {
                            format!("{start}")
                        } else {
                            format!("{start}-{end}")
                        });
                        start = slot;
                        end = slot;
                    }
                }
                ranges.push(if start == end {
                    format!("{start}")
                } else {
                    format!("{start}-{end}")
                });
            }
            ranges.join(" ")
        };

        let replica_of_str = node.replica_of.as_deref().unwrap_or("-");

        let last_pong = runtime_state
            .pong_received
            .map_or(0, |t| t.elapsed().as_millis());

        let current_epoch = cluster
            .current_epoch
            .load(std::sync::atomic::Ordering::Relaxed);

        output.push_str(&format!(
            "{} {} {} {} {} {} {} connected {}\n",
            node.id,
            node.addr,
            flags_str,
            replica_of_str,
            node.config_epoch,
            last_pong,
            current_epoch,
            slots_str
        ));
    }

    Ok((
        RespValue::BulkString(output.into()),
        WriteOutcome::DidNotWrite,
    ))
}
