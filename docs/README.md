<p align="center">
  <img src="spineldb-logo.png" alt="SpinelDB Logo" width="200"/>
  <h1>SpinelDB: Tutorials & Guides</h1>
</p>

<p align="center">
  Welcome to the official documentation and tutorial hub for SpinelDB. This collection of guides will walk you through setting up, using, and mastering SpinelDB, from the basics to our most advanced features.
</p>

---

## 📚 Command Reference

For a complete list of all commands supported by SpinelDB, categorized by their functionality, please refer to the dedicated command reference:

*   ➡️ **[0. Full Command List](./00-command-reference.md)**

---

## 🚀 Chapter 1: Getting Started

This is the essential starting point for all new users. We'll get you up and running in minutes.

*   ➡️ **[1. Installation & Setup](./01-installation-and-setup.md)**
    *   Building the project from source.
    *   Understanding the `config.toml` file.
    *   Running the SpinelDB server for the first time.
    *   Connecting to the server with `redis-cli`.

## ⚙️ Chapter 2: Core Data Types

Learn how to use SpinelDB just like you would use Redis. This chapter covers the fundamental commands and data types.

*   ➡️ **[2. Core Data Types & Commands](./02-core-data-types.md)**
    *   Working with **Strings** (`SET`, `GET`, `INCR`, `BITOP`).
    *   Managing **Lists** (`LPUSH`, `LRANGE`, `RPOP`).
    *   Using **Hashes** (`HSET`, `HGETALL`, `HINCRBY`).
    *   Operating on **Sets** (`SADD`, `SISMEMBER`, `SUNION`).
    *   Understanding **Sorted Sets** (`ZADD`, `ZRANGE`, `ZSCORE`).
    *   Introduction to **Streams** (`XADD`, `XREAD`, `XGROUP`).

## ✨ Chapter 3: Native Data Structures

Explore SpinelDB's powerful, built-in support for modern data formats, enabling complex operations directly on the server.

*   ➡️ **[3. Working with JSON Documents](./03-native-json.md)**
*   ➡️ **[4. Geospatial Indexing](./04-geospatial.md)**
*   ➡️ **[16. Bloom Filter](./16-bloom-filter.md)**
*   ➡️ **[17. HyperLogLogs](./17-hyperloglog.md)**

## 🧠 Chapter 4: The Intelligent Caching Engine

Dive deep into SpinelDB's flagship feature. Learn how to build a powerful, resilient, and automated caching layer, leveraging advanced capabilities like conditional revalidation, negative caching, and cluster-wide invalidation.

*   ➡️ **[5. Intelligent Caching](./05-caching.md)**

## 📈 Chapter 5: High Availability & Scalability

Learn how to run SpinelDB in a robust, fault-tolerant, and scalable production environment.

*   ➡️ **[6. Primary-Replica Replication](./06-replication.md)**
*   ➡️ **[7. Cluster Mode](./07-clustering.md)**
*   ➡️ **[8. Automatic Failover with Warden](./08-warden-failover.md) (✨ Recommended for Production)**

## 🔐 Chapter 6: Advanced Features

Master SpinelDB's powerful extended functionality.

*   ➡️ **[9. Security with Access Control Lists (ACL)](./09-security-acl.md)**
*   ➡️ **[10. Server-Side Scripting with Lua](./10-lua-scripting.md)**
*   ➡️ **[11. Atomic Operations with Transactions](./11-transactions.md)**
*   ➡️ **[12. Publish/Subscribe Messaging](./12-pubsub.md)**

## 📊 Chapter 7: Operations & Monitoring

Keep your SpinelDB instances healthy and observable.

*   ➡️ **[13. Introspection and Monitoring](./13-introspection-and-monitoring.md)**
*   ➡️ **[14. Persistence and Backup](./14-persistence-and-backup.md)**
*   ➡️ **[15. Operations & Troubleshooting](./15-troubleshooting.md)**

---

### Need Help?

If you have questions, find a bug, or have a feature request, please [open an issue](https://github.com/spineldb/spineldb/issues) on our main repository. We welcome all contributions
