# Chapter 12: Introspection and Monitoring

Running a database in production requires visibility. You need to understand how it's performing, how much memory it's using, identify slow operations, and integrate it into your existing monitoring infrastructure. SpinelDB provides a powerful suite of tools for introspection and monitoring, compatible with the standard Redis commands and modern observability platforms like Prometheus.

---

## 1. General Server Statistics (`INFO`)

The `INFO` command is your primary tool for getting a comprehensive snapshot of the server's state and statistics. It returns a human-readable text block containing sections of information.

**Command:** `INFO [section]`

You can request all information at once or ask for a specific section, such as `server`, `replication`, `memory`, or `stats`.

### Example Session

```shell
127.0.0.1:7878> INFO
# Server
spineldb_version:0.1.0
tcp_port:7878

# Replication
role:master
master_replid:a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2
master_repl_offset:123456
connected_slaves:1

# Memory
used_memory:536870912
used_memory_human:512.00M
maxmemory:1073741824

# Stats
total_connections_received:150
total_commands_processed:12543

... and more sections ...

# Requesting a specific section
127.0.0.1:7878> INFO memory
# Memory
used_memory:536870912
used_memory_human:512.00M
maxmemory:1073741824
```

---

## 2. Analyzing Command Latency (`SLOWLOG` and `LATENCY`)

Slow commands can be a major source of performance issues, as they can block other clients from being served. SpinelDB provides tools to identify and analyze these slow operations.

### `SLOWLOG`: Finding Slow Commands

The `SLOWLOG` keeps a running log of commands that exceeded a configured execution time threshold.

**Commands:**
*   `SLOWLOG GET [count]`: Retrieves the `count` most recent slow log entries (default is 10).
*   `SLOWLOG LEN`: Returns the number of entries in the slow log.
*   `SLOWLOG RESET`: Clears the slow log.

#### Example Session

```shell
# Get the two most recent slow commands
127.0.0.1:7878> SLOWLOG GET 2
1) 1) (integer) 128      # Unique entry ID
   2) (integer) 1679591234 # Unix timestamp
   3) (integer) 15234      # Execution time in microseconds
   4) 1) "KEYS"
      2) "*"
2) 1) (integer) 127
   2) (integer) 1679591100
   3) (integer) 8500
   4) 1) "SINTERSTORE"
      2) "result-key"
      3) "set1"
      4) "set2"
```
This is an invaluable tool for debugging performance bottlenecks in your application.

### `LATENCY`: Advanced Latency Analysis

The `LATENCY` command provides tools for more advanced, real-time latency analysis.

**Commands:**
*   `LATENCY HISTORY <event>`: Shows a time-series graph of latency for a specific command (e.g., `get`, `set`).
*   `LATENCY DOCTOR`: Provides a human-readable report with suggestions based on the server's observed latency patterns.

#### Example Session

```shell
127.0.0.1:7878> LATENCY DOCTOR
SpinelDB Latency Doctor
- Max latency so far: 15234 microseconds.
- High latency is often caused by:
  - Slow commands. Use SLOWLOG to inspect your slow commands.
  - AOF fsync blocking the main thread. Check your fsync policy.
  - High system load. Check CPU and I/O usage.
```

---

## 3. Monitoring with Prometheus (`/metrics`)

For modern, automated monitoring, SpinelDB includes a built-in **Prometheus exporter**. When enabled in `config.toml`, SpinelDB starts a small HTTP server on a separate port that exposes a `/metrics` endpoint.

### Configuration

Enable the metrics server in your `config.toml`:

```toml
# In your config.toml

[metrics]
enabled = true
port = 8878
```

After restarting the server, you can access the metrics endpoint with a tool like `curl` or by pointing your Prometheus instance to it.

```shell
curl http://127.0.0.1:8878/metrics
```

### Exposed Metrics

The endpoint exposes a wide range of useful metrics, including:
*   `spineldb_connected_clients`: The current number of connected clients (Gauge).
*   `spineldb_memory_used_bytes`: Total memory used by the database (Gauge).
*   `spineldb_commands_processed_total`: A running count of all commands (Counter).
*   `spineldb_cache_hits_total` & `spineldb_cache_misses_total`: Counters for the Intelligent Caching Engine.
*   `spineldb_command_latency_seconds`: A histogram of command latencies, perfect for calculating percentiles (e.g., p99) in Grafana.
*   ...and many more.

Integrating SpinelDB with Prometheus and Grafana provides a powerful, industry-standard solution for visualizing trends, creating dashboards, and setting up alerts to ensure your database instances are always healthy and performant.

---

### A Fully Observable System

With these tools, SpinelDB is not a black box. You have deep visibility into its internal state, performance characteristics, and resource usage, giving you the confidence to run it reliably in production.

➡️ **Next Chapter: [13. Persistence and Backup](./13-persistence-and-backup.md)**
