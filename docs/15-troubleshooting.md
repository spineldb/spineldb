# Chapter 15: Operations & Troubleshooting

This chapter provides guidance on operational procedures and how to handle specific error conditions you might encounter while running a SpinelDB server in production.

## Emergency Read-Only Mode

One of the most critical states a SpinelDB server can enter is "Emergency Read-Only Mode". This mode is a self-preservation mechanism designed to prevent data inconsistency between a primary (master) and its replicas.

### What It Is

When this mode is active, the server will **reject all write commands**. Clients attempting to perform a write operation (e.g., `SET`, `DEL`, `INCR`) will receive the following error:

```
(error) READONLY Server is in emergency read-only mode due to a critical propagation failure.
```

Read commands (e.g., `GET`, `KEYS`, `SCAN`) will continue to work as normal.

### How to Detect It

You can detect if a server is in this mode in two ways:

1.  **Error Messages:** Any write command will fail with the specific error message shown above.
2.  **The `INFO` Command:** Run the `INFO server` command and check the `emergency_read_only_mode` field.

    ```
    127.0.0.1:7878> INFO server
    # Server
    spineldb_version:4.3.6
    tcp_port:7878
    emergency_read_only_mode:1
    ```
    A value of `1` means the mode is active. A value of `0` means the server is operating normally.

### Why It Happens

This mode is triggered automatically when the server detects a critical internal error where a write command was successfully executed but **could not be safely prepared for propagation** to the AOF file or replicas.

The most common cause is a rare race condition involving `EVALSHA`:
1. A client successfully executes a script via `EVALSHA`.
2. Before the server can propagate this command, the script is flushed from the script cache (e.g., by a `SCRIPT FLUSH` command from another client).
3. The server now has a new data state but cannot find the script's content to send to its replicas.

At this point, the state between the primary and its replicas has diverged. To prevent any further divergence, the server activates emergency read-only mode.

### How to Recover

Recovering from this state requires manual intervention to ensure data consistency is restored.

1.  **Acknowledge and Investigate:** The first step is to acknowledge that a data consistency problem has occurred. The server logs will contain a `CRITICAL` error message detailing the event.

2.  **Identify the Divergence (Optional but Recommended):** If possible, try to identify which write command caused the issue. The server logs should point to the specific `EVALSHA` command.

3.  **Resynchronize Replicas:** The safest way to recover is to treat the primary's data as the source of truth and force all replicas to resynchronize with it.
    *   **Restart the Replicas:** The simplest method is to restart each replica node one by one. Upon restart, each replica will perform a full resynchronization with the primary, copying its entire dataset and resolving the inconsistency.
    *   **Use `REPLICAOF`:** Alternatively, you can connect to each replica and issue a `REPLICAOF <primary_ip> <primary_port>` command to trigger a full sync without a restart.

4.  **Restart the Primary Server:** Once all replicas have been resynchronized, the final step is to **restart the primary SpinelDB server**. The emergency read-only mode is a persistent state for the life of the process and can only be cleared by a restart.

By following these steps, you can safely recover from this critical state and ensure your entire cluster is back in a consistent state.

---

<div className="doc-nav-links">
  <span>⬅️ <strong>Previous Chapter: <a href="./persistence-and-backup">14. Persistence and Backup</a></strong></span>
  <span>➡️ <strong>Next Chapter: <a href="./bloom-filter">16. Bloom Filter Commands</a></strong></span>
</div>
