<p align="center">
  <img src="spineldb-logo.svg" alt="SpinelDB Logo" width="200"/>
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
    *   Storing and retrieving entire JSON objects with `JSON.SET` and `JSON.GET`.
    *   Modifying and deleting parts of a document with `JSON.SET`, `JSON.DEL`, `JSON.FORGET`.
    *   Manipulating arrays with `JSON.ARRAPPEND`, `JSON.ARRINSERT`, `JSON.ARRPOP`, `JSON.ARRLEN`, `JSON.ARRINDEX`, `JSON.ARRTRIM`.
    *   Inspecting JSON data with `JSON.TYPE`, `JSON.OBJLEN`, `JSON.OBJKEYS`.
    *   Performing atomic numeric operations with `JSON.NUMINCRBY`, `JSON.NUMMULTBY`.
    *   Clearing JSON values with `JSON.CLEAR`.
    *   Merging JSON objects with `JSON.MERGE`.
    *   Working with JSON strings with `JSON.STRAPPEND`, `JSON.STRLEN`.
    *   Toggling boolean values with `JSON.TOGGLE`.
*   ➡️ **[4. Geospatial Indexing](./04-geospatial.md)**
    *   Adding locations to a geo index with `GEOADD`.
    *   Querying for items within a radius using `GEORADIUS` and `GEORADIUSBYMEMBER`.
    *   Calculating distances with `GEODIST` and retrieving coordinates with `GEOPOS`.

## 🧠 Chapter 4: The Intelligent Caching Engine

Dive deep into SpinelDB's flagship feature. Learn how to build a powerful, resilient, and automated caching layer, leveraging advanced capabilities like conditional revalidation, negative caching, and cluster-wide invalidation.

*   ➡️ **[4a. Manual Caching with SWR & Grace Period](./04a-manual-caching-swr.md)**
    *   Using `CACHE.SET` with `TTL`, `SWR`, and `GRACE` options.
    *   Understanding the cache item lifecycle: fresh, stale, and grace.
    *   Implementing manual revalidation with `CACHE.GET REVALIDATE`.
*   ➡️ **[4b. Declarative Caching with Policies & `CACHE.PROXY`](./04b-declarative-caching-proxy.md) (✨ Recommended)**
    *   **Core Tutorial:** Defining global caching rules in `config.toml`.
    *   Automating the *get-or-fetch* pattern with a single `CACHE.PROXY` command.
    *   Using dynamic URL interpolation based on the cache key.
*   ➡️ **[4c. Tag-Based Invalidation](./04c-tag-based-invalidation.md)**
    *   Tagging cache items using `CACHE.SET` or Policies.
    *   Atomically purging multiple cache items with `CACHE.PURGETAG`.
*   ➡️ **[4d. Handling Large Objects with On-Disk Caching](./04d-on-disk-caching.md)**
    *   Configuring the `streaming_threshold_bytes`.
    *   How SpinelDB automatically streams large origin responses to disk to save memory.
*   ➡️ **[4e. Content Negotiation with `Vary`](./04e-content-negotiation-vary.md)**
    *   Serving different versions of a cached object based on request headers like `Accept-Encoding`.
    *   Understanding how `CACHE.GET` and `CACHE.SET` use the `VARY` and `HEADERS` options.

## 📈 Chapter 5: High Availability & Scalability

Learn how to run SpinelDB in a robust, fault-tolerant, and scalable production environment.

*   ➡️ **[5. Primary-Replica Replication](./05-replication.md)**
    *   Configuring a node as a primary or a replica.
    *   Understanding the synchronization process (Full vs. Partial Resync).
*   ➡️ **[6. Cluster Mode](./06-clustering.md)**
    *   Setting up a multi-node cluster.
    *   Understanding slot-based sharding and the gossip protocol.
    *   Performing manual resharding operations with `CLUSTER RESHARD`.
*   ➡️ **[7. Automatic Failover with Warden](./07-warden-failover.md) (✨ Recommended for Production)**
    *   The role of the Warden and why you should use it.
    *   Configuring and running multiple Warden instances for a quorum.
    *   How clients discover the new master after a failover.

## 🔐 Chapter 6: Advanced Features

Master SpinelDB's powerful extended functionality.

*   ➡️ **[8. Security with Access Control Lists (ACL)](./08-security-acl.md)**
    *   Enabling and configuring ACL rules in `config.toml`.
    *   Creating users and managing permissions dynamically with `ACL SETUSER` and `ACL SAVE`.
    *   Authenticating clients with `AUTH`.
*   ➡️ **[9. Server-Side Scripting with Lua](./09-lua-scripting.md)**
    *   Executing ad-hoc scripts with `EVAL`.
    *   Caching scripts and running them with `EVALSHA`.
    *   Calling SpinelDB commands from within a Lua script (`spineldb.call`).
*   ➡️ **[10. Atomic Operations with Transactions](./10-transactions.md)**
    *   Grouping commands for atomic execution with `MULTI` and `EXEC`.
    *   Implementing optimistic locking with `WATCH`.
*   ➡️ **[11. Publish/Subscribe Messaging](./11-pubsub.md)**
    *   Subscribing to channels (`SUBSCRIBE`) and patterns (`PSUBSCRIBE`).
    *   Broadcasting messages with `PUBLISH`.

## 📊 Chapter 7: Operations & Monitoring

Keep your SpinelDB instances healthy and observable.

*   ➡️ **[12. Introspection and Monitoring](./12-introspection-and-monitoring.md)**
    *   Getting server statistics with `INFO`.
    *   Analyzing command latency with `SLOWLOG` and `LATENCY`.
    *   Enabling the Prometheus metrics exporter and scraping the `/metrics` endpoint.
*   ➡️ **[13. Persistence and Backup](./13-persistence-and-backup.md)**
    *   Understanding AOF vs. SPLDB persistence strategies.
    *   Triggering background saves (`BGSAVE`) and AOF rewrites (`BGREWRITEAOF`).
*   ➡️ **[14. Operations & Troubleshooting](./14-troubleshooting.md)**
    *   Handling critical server states like Emergency Read-Only Mode.

---

### Need Help?

If you have questions, find a bug, or have a feature request, please [open an issue](https://github.com/spineldb/spineldb/issues) on our main repository. We welcome all contributions
