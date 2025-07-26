// src/core/latency.rs

//! Implements a latency monitoring system for tracking command execution times.
//! This is used for the `SLOWLOG` and `LATENCY` commands.

use crate::core::{RespValue, SpinelDBError};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

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
    /// If the history buffer is full, the oldest sample is removed.
    /// Large command arguments are truncated to prevent excessive memory usage.
    pub fn add_sample(
        &self,
        command_name: &'static str,
        command_args: Vec<Bytes>,
        latency: Duration,
    ) {
        let mut samples = self.samples.lock();
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
            return "No latency samples available. Latency monitoring is disabled.".to_string();
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
