# SpinelDB: A Modern, Redis-Compatible In-Memory Database in Rust
<p align="center">
  <img src="docs/spineldb-logo.png" alt="SpinelDB Logo" width="200"/>
</p>
<p align="center">
  <strong>Fast, Safe, and Intelligent. The next-generation in-memory data store.</strong>
</p>
<p align="center">
  <a href="https://github.com/spineldb/spineldb/actions/workflows/rust.yml"><img src="https://github.com/spineldb/spineldb/actions/workflows/release.yml/badge.svg" alt="CI Status"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT"></a>
</p>

---

**SpinelDB** is a high-performance, in-memory database server built from the ground up in Rust. It offers a **Redis-compatible API** while introducing powerful, modern features designed to solve common, real-world application challenges right out of the box.

Designed for today's architectures, SpinelDB is more than just a key-value store. It's a robust platform featuring an **Intelligent Caching Engine** with advanced strategies like SWR, grace periods, on-disk streaming, and cluster-aware tag-based invalidation; a **Security-First** design for authenticated cluster communications; and **Native JSON Support**, all while leveraging Rust's guarantees of memory safety and fearless concurrency.

## ✨ Why SpinelDB? The Modern Advantage

SpinelDB combines the beloved simplicity and speed of Redis with the next-generation features and reliability that modern systems demand.

🧠 **Intelligent Caching Engine**
A built-in, HTTP-aware caching engine. It natively supports **Stale-While-Revalidate (SWR)**, **grace periods**, **`Vary` header** content negotiation, efficient **tag-based invalidation (`CACHE.PURGETAG`)** across a cluster, **conditional revalidation (`IF-NONE-MATCH`, `IF-MODIFIED-SINCE`)**, **forced revalidation (`FORCE-REVALIDATE`)**, **negative caching**, and **explicit on-disk storage (`FORCE-DISK`)**. It also allows storing and retrieving **HTTP headers** (`HEADERS` option) with cached content. Offload complex caching logic from your application directly to the database.

🛡️ **Security-First by Design**
Cluster communications are authenticated with **HMAC-SHA256** to prevent spoofing. User authentication is powered by a modern, rule-based Access Control List (ACL) system with strong **Argon2** password hashing.

📄 **Native JSON Support**
A first-class JSON data type. Atomically manipulate deeply nested JSON documents on the server with a rich command set (`JSON.GET`, `JSON.SET`, `JSON.NUMINCRBY`, `JSON.ARRAPPEND`, etc.) without needing external modules.

⚙️ **Powerful Lua Scripting**
Embed complex business logic directly into the database with a sandboxed, high-performance Lua scripting engine (`EVAL`, `EVALSHA`). Reduce network round-trips by moving logic to the data.

🚀 **True Multithreading**
Internally sharded from the core, SpinelDB leverages modern multi-core CPUs for true parallel command execution. This drastically reduces lock contention and boosts throughput for multi-key operations.

🔒 **Unparalleled Reliability**
Built in Rust, eliminating entire classes of memory safety bugs, null pointer dereferences, and data races. It features robust fail-safes, like automatically entering read-only mode on critical persistence errors to protect your data.

🔌 **Seamless Compatibility**
Drop-in ready. Use your existing Redis clients and libraries in any language without modification. SpinelDB speaks RESP2/RESP3.

🏎️ **Advanced Optimistic Locking**
`WATCH`/`EXEC` and complex commands like `SORT` use an advanced optimistic versioning system. This maximizes concurrency by avoiding long-held locks, even during complex, multi-key operations.

<br>

_"Stop building boilerplate caching and data consistency logic in your application. SpinelDB provides intelligent primitives that let you focus on features, not plumbing. It's the performance of Redis with the safety of Rust and the features you wish you always had."_

---

## 🚀 Feature Highlights

*   **Intelligent Caching Engine:**
    *   **`CACHE.PROXY`:** The ultimate get-or-fetch command with declarative policy support.
    *   **`CACHE.SET/GET`:** Fine-grained control with TTL, Stale-While-Revalidate (SWR), Grace Periods, **conditional revalidation (`IF-NONE-MATCH`, `IF-MODIFIED-SINCE`)**, **negative caching**, **explicit on-disk storage (`FORCE-DISK`)**, and the ability to store and retrieve **HTTP headers** (`HEADERS` option).
    *   **`CACHE.PURGETAG`:** Blazingly fast, tag-based invalidation of multiple keys in a single atomic operation.
    *   **`Vary` Header Support:** Serve different versions of the same resource based on request headers (e.g., `Accept-Language`).
    *   **On-Disk Streaming:** Automatically streams large cache objects to disk to protect memory, with zero performance impact for the client.

*   **Rich Data Structures:** Strings, Lists, Hashes, Sets, Sorted Sets, **Native JSON**, and Streams.

*   **Server-Side Lua Scripting:**
    *   **`EVAL` & `EVALSHA`:** Full compatibility with Redis scripting commands.
    *   **Rich API:** Execute SpinelDB commands from within scripts using `spineldb.call(...)` and `spineldb.pcall(...)`.
    *   **Safe Execution:** Scripts run in a sandboxed environment with a configurable timeout to prevent long-running scripts from blocking the server.
    *   **Persistence:** Scripts executed via `EVALSHA` are automatically persisted to the AOF with their full body, ensuring correct replay during recovery or replication.
    > **Note on Atomicity:** Unlike Redis, where scripts are fully atomic, SpinelDB's `spineldb.call` executes each command with its own set of locks. This design allows for more flexible, long-running logic without holding a global lock. For atomic multi-key operations across multiple commands, use `WATCH`/`MULTI`/`EXEC` from your client.

*   **Robust Persistence:**
    *   **AOF (Append-Only File):** `fsync` policies (`always`, `everysec`, `no`), and a non-blocking, background rewrite process to keep the file compact **(buffering writes during rewrite)**.
    *   **SPLDB (Snapshotting):** Point-in-time snapshots with configurable save policies (`save <seconds> <changes>`), **including checksums and atomic writes**.
    *   **Atomic & Safe:** All file writes use a temporary file and atomic rename to prevent data corruption on crash.

*   **High Availability:**
    *   **Replication:** Classic Primary-Replica model with full (SPLDB) and partial (backlog) resynchronization.
    *   **Clustering:** Horizontally scale your data with a Redis Cluster compatible protocol, including gossip-based discovery and **self-fencing masters** to prevent split-brain.
    *   **Warden:** A dedicated, Sentinel-compatible monitoring and automatic failover system (`--warden` mode) for maximum reliability.

*   **Advanced Security:**
    *   A modern, rule-based **Access Control List (ACL)** system with strong **Argon2** password hashing.
    *   Secure, HMAC-SHA256 authenticated cluster bus to protect against unauthorized nodes.

*   **Deep Introspection:** `INFO`, `SLOWLOG`, and `LATENCY` commands for powerful monitoring and performance diagnostics.

*   **Production Safety:** Built-in circuit breakers to prevent dangerous commands (like `KEYS *` or `SMEMBERS` on huge sets) from impacting performance.

---

## 🏁 Getting Started

### Prerequisites

- Rust toolchain (latest stable version recommended)
- A C compiler toolchain (`build-essential` on Debian/Ubuntu, `Xcode Command Line Tools` on macOS)
- `pkg-config` and OpenSSL development libraries (`libssl-dev` on Debian/Ubuntu, `openssl` via Homebrew on macOS)

### Building from Source

```bash
git clone https://github.com/spineldb/spineldb.git
cd spineldb
cargo build --release
```

The compiled binary will be at `target/release/spineldb`.

### Quick Install

For a quick setup, you can use our installation script:

```bash
# Using curl
sh -c "$(curl -fsSL https://raw.githubusercontent.com/spineldb/spineldb/main/install.sh)"

# Using wget
sh -c "$(wget -qO- https://raw.githubusercontent.com/spineldb/spineldb/main/install.sh)"
```

### Docker

You can also run SpinelDB using our official Docker image:

```bash
# Pull the latest image
docker pull spineldb/spineldb:latest

# Run the container
docker run -d -p 7878:7878 --name spineldb_instance spineldb/spineldb:latest
```

### Running the Server

1.  **Configuration:** A default `config.toml` is provided. Customize it to fit your needs, especially `host` and `port`.
2.  **Start the Server:**
    ```bash
    ./target/release/spineldb [--port <port>]
    ```
3.  **Connect with any Redis Client:**
    ```bash
    redis-cli -p 7878
    127.0.0.1:7878> PING
    PONG
    127.0.0.1:7878> JSON.SET user:1 . '{"name": "Alice", "age": 30, "projects": ["SpinelDB"]}'
    OK
    127.0.0.1:7878> JSON.ARRAPPEND user:1 .projects '"Rust"'
    (integer) 2
    127.0.0.1:7878> JSON.GET user:1
    "{\"age\":30,\"name\":\"Alice\",\"projects\":[\"SpinelDB\",\"Rust\"]}"
    
    # <!-- NEW -->
    127.0.0.1:7878> CACHE.SET mypage:home "<html>Home Page</html>" TTL 60 SWR 300 REVALIDATE-URL "https://example.com/home"
    OK
    127.0.0.1:7878> CACHE.GET mypage:home
    "<html>Home Page</html>"
    # <!-- END NEW -->
    ```

### Running in Warden Mode

1.  **Configuration:** Create a `warden.toml` file to define the masters to monitor (see `warden.example.toml`).
2.  **Start the Warden:**
    ```bash
    ./target/release/spineldb --warden /path/to/warden.toml
    ```

---

## 🏛️ Architecture

SpinelDB is engineered from the ground up for modern hardware and robust, concurrent operation.

-   **Async Core:** Built entirely on the [Tokio](https://tokio.rs/) async runtime for massively concurrent, non-blocking I/O.
-   **Internally Sharded & Lock-Efficient:** Each database is internally sharded into concurrent segments. Commands acquire locks only on the necessary shards, drastically reducing contention and enabling true parallelism for multi-key operations on multi-core systems.
-   **Decoupled Event Bus:** A central, non-blocking event bus propagates write commands to persistence (AOF) and replication subsystems. This decouples core command processing from disk I/O, ensuring high throughput even when persistence is slow.
-   **Memory-Safe by Design:** Leverages Rust's ownership model and type system to prevent entire classes of bugs like buffer overflows, use-after-frees, and data races that plague systems written in C/C++.

---

## 🗺️ Roadmap

SpinelDB is actively developed. Our current focus is on hardening the core and expanding our unique features.

-   [ ] **Rigorous Testing:** Expand performance and chaos testing suites to validate clustering and failover under adverse conditions.
-   [ ] **Comprehensive Benchmarking:** Publish detailed benchmarks against Redis and other alternatives, highlighting caching and multi-core performance.
-   [ ] **Deployment & Observability:** Provide official Docker images, Helm charts, and a Grafana dashboard for Prometheus metrics.

## 🤝 Contributing

We are thrilled you're interested in contributing to SpinelDB! Whether it's reporting a bug, proposing a new feature, or writing code, your help is welcome.

Please read our **CONTRIBUTING.md** (coming soon!) to learn how you can get started.

## ⚖️ License

SpinelDB is distributed under the terms of the **MIT License**.

See [LICENSE](LICENSE) for details.
