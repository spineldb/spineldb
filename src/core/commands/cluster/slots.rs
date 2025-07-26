// src/core/commands/cluster/slots.rs

use crate::core::cluster::{ClusterNode, NodeFlags};
use crate::core::commands::command_trait::WriteOutcome;
use crate::core::storage::db::ExecutionContext;
use crate::core::{RespValue, SpinelDBError};
use std::collections::{BTreeMap, BTreeSet};

pub async fn execute(
    ctx: &mut ExecutionContext<'_>,
) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
    let cluster = ctx.state.cluster.as_ref().unwrap();
    let mut masters: BTreeMap<String, BTreeSet<u16>> = BTreeMap::new();
    let nodes_view: BTreeMap<String, ClusterNode> = cluster
        .nodes
        .iter()
        .map(|entry| (entry.key().clone(), entry.value().node_info.clone()))
        .collect();

    for node in nodes_view
        .values()
        .filter(|n| n.get_flags().contains(NodeFlags::PRIMARY))
    {
        if !node.slots.is_empty() {
            masters.insert(node.id.clone(), node.slots.clone());
        }
    }

    let mut slot_info = Vec::new();
    for (master_id, master_slots) in masters {
        let mut sorted_slots: Vec<u16> = master_slots.into_iter().collect();
        sorted_slots.sort_unstable();

        if sorted_slots.is_empty() {
            continue;
        }

        let mut start = sorted_slots[0];
        let mut end = start;

        for &slot in sorted_slots.iter().skip(1) {
            if slot == end + 1 {
                end = slot;
            } else {
                slot_info.push(format_slot_range(start, end, &master_id, &nodes_view)?);
                start = slot;
                end = slot;
            }
        }
        slot_info.push(format_slot_range(start, end, &master_id, &nodes_view)?);
    }

    slot_info.sort_by_key(|v| {
        if let RespValue::Array(arr) = v {
            if let RespValue::Integer(i) = arr[0] {
                i
            } else {
                0
            }
        } else {
            0
        }
    });

    Ok((RespValue::Array(slot_info), WriteOutcome::DidNotWrite))
}

fn format_slot_range(
    start: u16,
    end: u16,
    master_id: &str,
    nodes_view: &BTreeMap<String, ClusterNode>,
) -> Result<RespValue, SpinelDBError> {
    let mut node_infos = Vec::new();
    if let Some(primary) = nodes_view.get(master_id) {
        node_infos.push(format_node_info(primary)?);
        for replica in nodes_view
            .values()
            .filter(|n| n.replica_of.as_deref() == Some(master_id))
        {
            node_infos.push(format_node_info(replica)?);
        }
    }
    let mut slot_range_info = vec![
        RespValue::Integer(start as i64),
        RespValue::Integer(end as i64),
    ];
    slot_range_info.extend(node_infos);
    Ok(RespValue::Array(slot_range_info))
}

fn format_node_info(node: &ClusterNode) -> Result<RespValue, SpinelDBError> {
    let parts: Vec<&str> = node.addr.split(':').collect();
    let ip = parts
        .first()
        .ok_or(SpinelDBError::Internal("Invalid node address".into()))?;
    let port_val: i64 = parts
        .get(1)
        .ok_or(SpinelDBError::Internal(
            "Invalid node address, missing port".into(),
        ))?
        .parse()?;
    Ok(RespValue::Array(vec![
        RespValue::BulkString(ip.to_string().into()),
        RespValue::Integer(port_val),
        RespValue::BulkString(node.id.clone().into()),
    ]))
}
