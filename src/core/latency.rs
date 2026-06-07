// src/core/latency.rs

//! Implements a latency monitoring system for tracking command execution times.
//! This is used for the `SLOWLOG` and `LATENCY` commands.

use crate::core::{RespValue, SpinelDBError};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tracing::debug;

/// The maximum number of latency samples to store in the history.
/// This acts as a circular buffer.
pub const LATENCY_HISTORY_LEN: usize = 160;

/// The maximum length of a single command argument to be stored in the slow log.
/// This prevents commands with very large values from consuming excessive memory.
const SLOWLOG_MAX_ARG_LEN: usize = 128;

/// Represents a single latency measurement for a specific event (command).
#[derive(Debug, Clone)]
pub struct LatencySample {
    /// The timestamp when the sample was recorded.
    pub timestamp: Instant,
    /// The duration of the event.
    pub latency: Duration,
    /// The name of the event (e.g., the command name).
    pub command_name: &'static str,
    /// The arguments of the command.
    pub command_args: Vec<Bytes>,
}

/// The main struct for monitoring and reporting on command latencies.
/// It provides the backend for the `SLOWLOG` and `LATENCY` commands.
#[derive(Debug)]
pub struct LatencyMonitor {
    /// A circular buffer of the most recent latency samples.
    /// Wrapped in a Mutex for thread-safe access.
    samples: Mutex<VecDeque<LatencySample>>,
    /// A unique, incrementing ID for each slow log entry.
    next_id: Mutex<u64>,
}

impl LatencyMonitor {
    /// Creates a new `LatencyMonitor`.
    pub fn new() -> Self {
        Self {
            samples: Mutex::new(VecDeque::with_capacity(LATENCY_HISTORY_LEN)),
            next_id: Mutex::new(0),
        }
    }

    /// Adds a new latency sample to the monitor.
    /// This uses `try_lock` to avoid blocking the command execution path under contention.
    pub fn add_sample(
        &self,
        command_name: &'static str,
        command_args: Vec<Bytes>,
        latency: Duration,
    ) {
        // Use a non-blocking lock to prevent adding latency to the hot path.
        if let Some(mut samples) = self.samples.try_lock() {
            // A separate lock for the ID is acceptable as it's only held briefly.
            let mut next_id_guard = self.next_id.lock();

            if samples.len() == LATENCY_HISTORY_LEN {
                samples.pop_front();
            }

            // Truncate any arguments that exceed the defined maximum length.
            let truncated_args: Vec<Bytes> = command_args
                .into_iter()
                .map(|arg| {
                    if arg.len() > SLOWLOG_MAX_ARG_LEN {
                        let mut truncated = arg.slice(..SLOWLOG_MAX_ARG_LEN).to_vec();
                        truncated.extend_from_slice(b"... (truncated)");
                        Bytes::from(truncated)
                    } else {
                        arg
                    }
                })
                .collect();

            samples.push_back(LatencySample {
                timestamp: Instant::now(),
                latency,
                command_name,
                command_args: truncated_args,
            });

            *next_id_guard += 1;
        } else {
            // If the lock is contended, it's better to drop the sample than to block.
            debug!("Skipping latency sample due to contention on monitor lock.");
        }
    }

    /// Implements the `SLOWLOG GET [count]` command.
    /// It returns the most recent slow log entries.
    pub fn get_slow_log(&self, count: Option<usize>) -> RespValue {
        let samples = self.samples.lock();
        let count = count.unwrap_or(10).min(samples.len());

        let logs: Vec<RespValue> = samples
            .iter()
            .rev() // Iterate from newest to oldest.
            .take(count)
            .enumerate()
            .map(|(i, sample)| {
                let id = *self.next_id.lock() - 1 - i as u64;

                // Build the array of the command and its (potentially truncated) arguments.
                let mut full_command_array = Vec::with_capacity(sample.command_args.len() + 1);
                full_command_array.push(RespValue::BulkString(sample.command_name.into()));
                full_command_array.extend(
                    sample
                        .command_args
                        .iter()
                        .cloned()
                        .map(RespValue::BulkString),
                );

                RespValue::Array(vec![
                    // 1. Unique ID
                    RespValue::Integer(id as i64),
                    // 2. Unix timestamp of when the command was processed.
                    RespValue::Integer(
                        (std::time::SystemTime::now() - sample.timestamp.elapsed())
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs() as i64,
                    ),
                    // 3. Latency in microseconds.
                    RespValue::Integer(sample.latency.as_micros() as i64),
                    // 4. The command and its arguments.
                    RespValue::Array(full_command_array),
                ])
            })
            .collect();

        RespValue::Array(logs)
    }

    /// Implements the `SLOWLOG LEN` command.
    pub fn get_slow_log_len(&self) -> RespValue {
        let samples = self.samples.lock();
        RespValue::Integer(samples.len() as i64)
    }

    /// Implements the `SLOWLOG RESET` command.
    pub fn reset_slow_log(&self) -> RespValue {
        let mut samples = self.samples.lock();
        samples.clear();
        RespValue::SimpleString("OK".into())
    }

    /// Implements the `LATENCY HISTORY <event>` command.
    /// Returns a series of (time, latency) pairs for a specific event.
    pub fn get_history(&self, event: &str) -> Result<RespValue, SpinelDBError> {
        let samples = self.samples.lock();
        let history: Vec<RespValue> = samples
            .iter()
            .filter(|s| s.command_name == event)
            .map(|s| {
                RespValue::Array(vec![
                    RespValue::Integer(
                        s.timestamp.duration_since(samples[0].timestamp).as_secs() as i64
                    ),
                    RespValue::Integer(s.latency.as_micros() as i64),
                ])
            })
            .collect();
        Ok(RespValue::Array(history))
    }

    /// Implements the `LATENCY DOCTOR` command.
    /// Provides a human-readable analysis of latency issues.
    pub fn get_doctor_report(&self) -> String {
        let mut report = String::new();
        let samples = self.samples.lock();

        if samples.is_empty() {
            return "No latency samples available.".to_string();
        }

        let mut max_latency = Duration::from_micros(0);
        for sample in samples.iter() {
            if sample.latency > max_latency {
                max_latency = sample.latency;
            }
        }

        report.push_str(&format!(
            "SpinelDB Latency Doctor\n- Max latency so far: {} microseconds.\n",
            max_latency.as_micros()
        ));
        report.push_str("- High latency is often caused by:\n");
        report.push_str("  - Slow commands. Use SLOWLOG to inspect your slow commands.\n");
        report.push_str("  - AOF fsync blocking the main thread. Check your fsync policy.\n");
        report.push_str("  - High system load. Check CPU and I/O usage.\n");

        report
    }
}

impl Default for LatencyMonitor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_monitor_is_empty() {
        let m = LatencyMonitor::new();
        let r = m.get_slow_log_len();
        assert_eq!(r, RespValue::Integer(0));
    }

    #[test]
    fn test_add_sample_grows_history() {
        let m = LatencyMonitor::new();
        m.add_sample(
            "GET",
            vec![Bytes::from_static(b"k")],
            Duration::from_millis(5),
        );
        m.add_sample(
            "SET",
            vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
            Duration::from_millis(10),
        );
        let r = m.get_slow_log_len();
        assert_eq!(r, RespValue::Integer(2));
    }

    #[test]
    fn test_add_sample_circular_buffer_eviction() {
        let m = LatencyMonitor::new();
        // Insert LATENCY_HISTORY_LEN + 50 samples to trigger eviction.
        for i in 0..(LATENCY_HISTORY_LEN + 50) {
            m.add_sample(
                "PING",
                vec![Bytes::from(format!("{i}"))],
                Duration::from_micros(i as u64),
            );
        }
        let r = m.get_slow_log_len();
        assert_eq!(r, RespValue::Integer(LATENCY_HISTORY_LEN as i64));
    }

    #[test]
    fn test_get_slow_log_returns_newest_first() {
        let m = LatencyMonitor::new();
        m.add_sample("A", vec![], Duration::from_micros(1));
        m.add_sample("B", vec![], Duration::from_micros(2));
        m.add_sample("C", vec![], Duration::from_micros(3));
        // SLOWLOG returns newest first: C, B, A.
        let r = m.get_slow_log(None);
        if let RespValue::Array(logs) = r {
            assert_eq!(logs.len(), 3);
            // Each entry: [id, ts, latency_us, [cmd, args...]]
            let extract_cmd = |entry: &RespValue| -> String {
                if let RespValue::Array(parts) = entry
                    && let RespValue::Array(cmd_arr) = &parts[3]
                    && let RespValue::BulkString(name) = &cmd_arr[0]
                {
                    return String::from_utf8_lossy(name).to_string();
                }
                panic!("unexpected entry shape: {entry:?}");
            };
            assert_eq!(extract_cmd(&logs[0]), "C");
            assert_eq!(extract_cmd(&logs[1]), "B");
            assert_eq!(extract_cmd(&logs[2]), "A");
        } else {
            panic!("expected array");
        }
    }

    #[test]
    fn test_get_slow_log_count_capped_by_history() {
        let m = LatencyMonitor::new();
        for i in 0..20 {
            m.add_sample(
                "X",
                vec![Bytes::from(format!("{i}"))],
                Duration::from_micros(i as u64),
            );
        }
        let r = m.get_slow_log(Some(5));
        if let RespValue::Array(logs) = r {
            assert_eq!(logs.len(), 5);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_get_slow_log_count_larger_than_history() {
        let m = LatencyMonitor::new();
        m.add_sample("X", vec![], Duration::from_micros(1));
        m.add_sample("Y", vec![], Duration::from_micros(2));
        let r = m.get_slow_log(Some(100));
        if let RespValue::Array(logs) = r {
            assert_eq!(logs.len(), 2);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_slow_log_resets_history() {
        let m = LatencyMonitor::new();
        m.add_sample("A", vec![], Duration::from_micros(1));
        m.add_sample("B", vec![], Duration::from_micros(2));
        let r = m.reset_slow_log();
        assert_eq!(r, RespValue::SimpleString("OK".into()));
        assert_eq!(m.get_slow_log_len(), RespValue::Integer(0));
    }

    #[test]
    fn test_get_history_filters_by_event() {
        let m = LatencyMonitor::new();
        m.add_sample("GET", vec![], Duration::from_micros(10));
        m.add_sample("SET", vec![], Duration::from_micros(20));
        m.add_sample("GET", vec![], Duration::from_micros(30));
        let r = m.get_history("GET").unwrap();
        if let RespValue::Array(entries) = r {
            assert_eq!(entries.len(), 2);
        } else {
            panic!();
        }
        let r = m.get_history("SET").unwrap();
        if let RespValue::Array(entries) = r {
            assert_eq!(entries.len(), 1);
        } else {
            panic!();
        }
    }

    #[test]
    fn test_get_history_for_unknown_event_is_empty() {
        let m = LatencyMonitor::new();
        m.add_sample("GET", vec![], Duration::from_micros(10));
        let r = m.get_history("NEVER_HAPPENED").unwrap();
        if let RespValue::Array(entries) = r {
            assert!(entries.is_empty());
        } else {
            panic!();
        }
    }

    #[test]
    fn test_doctor_report_when_empty() {
        let m = LatencyMonitor::new();
        let s = m.get_doctor_report();
        assert!(s.contains("No latency samples"));
    }

    #[test]
    fn test_doctor_report_contains_max_latency() {
        let m = LatencyMonitor::new();
        m.add_sample("X", vec![], Duration::from_micros(100));
        m.add_sample("X", vec![], Duration::from_micros(50_000));
        m.add_sample("X", vec![], Duration::from_micros(75));
        let s = m.get_doctor_report();
        assert!(s.contains("50000"));
    }

    #[test]
    fn test_long_argument_is_truncated() {
        let m = LatencyMonitor::new();
        let big = Bytes::from(vec![b'x'; SLOWLOG_MAX_ARG_LEN + 50]);
        m.add_sample(
            "SET",
            vec![Bytes::from_static(b"k"), big.clone()],
            Duration::from_micros(1),
        );
        // The arg should be SLOWLOG_MAX_ARG_LEN bytes of 'x' plus the suffix.
        let r = m.get_slow_log(None);
        if let RespValue::Array(logs) = r
            && let RespValue::Array(parts) = &logs[0]
            && let RespValue::Array(cmd_arr) = &parts[3]
        {
            // [cmd, k, truncated_value]
            assert_eq!(cmd_arr.len(), 3);
            if let RespValue::BulkString(b) = &cmd_arr[2] {
                // The truncated arg has SLOWLOG_MAX_ARG_LEN x's + 15 bytes of suffix.
                let expected_len = SLOWLOG_MAX_ARG_LEN + b"... (truncated)".len();
                assert_eq!(b.len(), expected_len);
                let suffix = &b[SLOWLOG_MAX_ARG_LEN..];
                assert_eq!(suffix, b"... (truncated)");
            } else {
                panic!();
            }
        } else {
            panic!();
        }
    }
}
