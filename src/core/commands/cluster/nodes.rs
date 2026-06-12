// src/core/commands/cluster/nodes.rs

use crate::core::commands::command_trait::WriteOutcome;
use crate::core::database::ExecutionContext;
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

#[cfg(test)]
mod tests {
    #[test]
    fn test_single_slot() {
        let ranges = make_slot_ranges(&[100]);
        assert_eq!(ranges, vec!["100"]);
    }

    #[test]
    fn test_contiguous_slots() {
        let ranges = make_slot_ranges(&[1, 2, 3, 4, 5]);
        assert_eq!(ranges, vec!["1-5"]);
    }

    #[test]
    fn test_non_contiguous_slots() {
        let ranges = make_slot_ranges(&[1, 2, 5, 6, 10]);
        assert_eq!(ranges, vec!["1-2", "5-6", "10"]);
    }

    #[test]
    fn test_empty_slots() {
        let ranges = make_slot_ranges(&[]);
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_single_range_of_two() {
        let ranges = make_slot_ranges(&[100, 101]);
        assert_eq!(ranges, vec!["100-101"]);
    }

    #[test]
    fn test_all_same_slot() {
        let ranges = make_slot_ranges(&[5]);
        assert_eq!(ranges, vec!["5"]);
    }

    #[test]
    fn test_unsorted_input() {
        let ranges = make_slot_ranges(&[10, 5, 8, 6, 7]);
        assert_eq!(ranges, vec!["5-8", "10"]);
    }

    #[test]
    fn test_replica_of_none_formats_as_dash() {
        let replica_of: Option<String> = None;
        let replica_of_str = replica_of.as_deref().unwrap_or("-");
        assert_eq!(replica_of_str, "-");
    }

    #[test]
    fn test_replica_of_some_formats_as_id() {
        let replica_of: Option<String> = Some("master-1".to_string());
        let replica_of_str = replica_of.as_deref().unwrap_or("-");
        assert_eq!(replica_of_str, "master-1");
    }

    #[test]
    fn test_flags_empty_string() {
        use crate::core::cluster::NodeFlags;
        let flags = NodeFlags::empty();
        let flags_str = flags
            .iter_names()
            .map(|(name, _)| name.to_lowercase())
            .collect::<Vec<_>>()
            .join(",");
        assert!(flags_str.is_empty());
    }

    #[test]
    fn test_flags_myself_primary() {
        use crate::core::cluster::NodeFlags;
        let flags = NodeFlags::MYSELF | NodeFlags::PRIMARY;
        let flags_str = flags
            .iter_names()
            .map(|(name, _)| name.to_lowercase())
            .collect::<Vec<_>>()
            .join(",");
        assert!(flags_str.contains("myself"));
        assert!(flags_str.contains("primary"));
    }

    #[test]
    fn test_node_flags_roundtrip() {
        use crate::core::cluster::NodeFlags;
        let flags = NodeFlags::MYSELF | NodeFlags::REPLICA;
        let raw = flags.bits();
        let restored = NodeFlags::from_bits_truncate(raw);
        assert_eq!(flags, restored);
    }

    fn make_slot_ranges(slots: &[u16]) -> Vec<String> {
        let mut sorted_slots: Vec<u16> = slots.to_vec();
        sorted_slots.sort_unstable();

        let mut ranges = vec![];
        if sorted_slots.is_empty() {
            return ranges;
        }

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
        ranges
    }
}
