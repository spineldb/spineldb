# SpinelDB Project Roadmap

This document outlines the development roadmap for SpinelDB, a Modern In-Memory Database designed as a Redis alternative with an integrated Intelligent Caching Engine.

A `[x]` checkbox indicates a feature that is already implemented (at least for the most part). A `[ ]` checkbox indicates a feature planned for the future.

---

## 1. Core Functionality

- [x] Basic Key-Value engine (GET, SET, DEL, EXISTS, etc.)
- [x] Key Expiration support (EXPIRE, TTL, PEXPIRE, PTTL)
- [x] Server configuration via file (`config.toml`)
- [x] RESP (Redis Serialization Protocol) Protocol
- [ ] Full RESP3 Protocol Support
- [x] Command-line Interface (CLI) for basic interaction

## 2. Data Types

- [x] **Strings**
- [x] **Bitmaps/Bitfields**
- [x] **HyperLogLogs**
- [x] **Lists**
- [x] **Hashes**
- [x] **Sets**
- [x] **Sorted Sets**
- [x] **Streams**
- [x] **Geospatial**
- [x] **Native JSON**

## 3. Advanced Data Structures & Search

- [ ] **SpinelSearch**: Full-text search engine capabilities.
- [ ] **SpinelGraph**: Graph database functionality.
- [ ] **SpinelTimeSeries**: Time-series data support.
- [x] **SpinelBloom**: Probabilistic data structures (Bloom and Cuckoo filters).
- [ ] **SpinelVector**: Vector similarity search and embeddings.

## 4. Persistence

- [x] **AOF (Append-Only File)**: Logs every write operation.
- [x] **AOF Rewrite**: Automatic background rewriting of the AOF file.
- [x] **Snapshotting (.spldb)**: Point-in-time data snapshots.

## 5. High Availability & Scalability

- [x] **Master-Replica Replication**: (PSYNC/SYNC).
- [x] **Read-Only Replicas**: Replicas are read-only by default.
- [x] **SpinelDB Cluster**: Automatic data partitioning, horizontal scalability.
- [x] **Gossip Protocol**: For node discovery and health checks within the cluster.
- [x] **Automatic Failover (Warden)**: Automatic promotion of a replica to master (SpinelDB's equivalent to Sentinel).
- [x] **Sharded Pub/Sub**: Cluster-wide Pub/Sub support.

## 6. Advanced Features

- [x] **Transactions**: (MULTI, EXEC, DISCARD, WATCH).
- [x] **Pub/Sub**: (SUBSCRIBE, PUBLISH, PSUBSCRIBE).
- [x] **Intelligent Caching Engine**: Including a declarative caching proxy.
- [ ] **Client-Side Caching**: Server-assisted client-side caching.

## 7. Extensibility & Scripting

- [x] **Lua Scripting**: (EVAL, EVALSHA).
- [ ] **SpinelDB Functions**: The evolution of Lua scripting for more advanced server-side logic.
- [ ] **Redis Gears**: Programmable data processing across the cluster.
- [ ] **Modules API**: Allow for the development of custom functionality as loadable modules.

## 8. Security

- [x] **Authentication**: (AUTH).
- [x] **Access Control Lists (ACL)**.
- [ ] **Advanced ACLs**: Granular permissions with selectors.

## 9. Operations & Monitoring

- [x] **Introspection**: `INFO`, `COMMAND`, `LATENCY` commands.
- [x] **Docker Support**: `Dockerfile` for easy deployment.
- [x] **CI/CD Pipeline**: Automated testing and releases via GitHub Actions.
- [x] **Prometheus Metrics**: A `/metrics` endpoint for integration with modern monitoring systems.
- [ ] **Official Helm Chart**: For easy deployment on Kubernetes.

## 10. Ecosystem & Clients

- [x] High compatibility with popular Redis clients.
- [ ] Official Rust client.
- [ ] Official clients for other popular languages (Python, JavaScript/TypeScript, Go, Java).

## 11. Documentation

- [x] Command reference documentation.
- [x] Core concepts documentation (Replication, Cluster, Caching, etc.).
- [ ] Public documentation website (e.g., using `mdBook` or `Docusaurus`).
- [ ] More comprehensive Getting Started Guide.
- [x] Contribution guidelines (`CONTRIBUTING.md`).
