# Chapter 4d: Handling Large Objects with On-Disk Caching

A common challenge with in-memory caches is managing large objects. Storing assets like high-resolution images, videos, or large JSON API responses directly in memory can quickly exhaust your server's RAM, leading to aggressive eviction of smaller, more frequently accessed items.

SpinelDB solves this problem with its **hybrid on-disk caching** mechanism. You can define a size threshold; any cacheable object larger than this threshold will be automatically streamed to a temporary file on disk instead of being stored in RAM.

This gives you the best of both worlds:
*   **Speed:** Small, hot data remains in memory for the lowest possible latency.
*   **Capacity:** Large, less frequently accessed objects are offloaded to disk, dramatically increasing your effective cache capacity without consuming precious RAM.

The entire process is transparent to the client. A `CACHE.GET` or `CACHE.PROXY` request works identically whether the item is in memory or on disk.

---

## 1. Configuring On-Disk Caching

On-disk caching is configured with two settings in your `config.toml` file, under the `[cache]` section.

*   `on_disk_path`: The directory path where SpinelDB will store the cache files. **The server must have write permissions to this directory.**
*   `streaming_threshold_bytes`: The size in bytes. Any object returned from an origin server with a `Content-Length` header greater than this value will be streamed to disk.

### Example Configuration

Let's configure SpinelDB to store any object larger than 1 megabyte on disk in a directory named `spineldb_cache_files`.

```toml
# In your config.toml

[cache]
# The directory to store large cache files. It will be created if it doesn't exist.
on_disk_path = "spineldb_cache_files"

# Set the threshold to 1MB (1024 * 1024 bytes).
streaming_threshold_bytes = 1048576
```
After setting this, restart your SpinelDB server for the changes to take effect.

---

## 2. The Workflow in Action

You don't need to change any of your `CACHE.*` commands to use this feature. SpinelDB handles the logic automatically during the fetch process.

Let's trace a `CACHE.PROXY` request for a large file.

**Scenario:** A client requests `CACHE.PROXY images:product:poster-xyz`

1.  **Cache Miss:** SpinelDB checks for the key in memory and finds it's not there.
2.  **Policy Lookup:** It finds a matching cache policy with the URL `https://cdn.myapp.com/images/poster-xyz.jpg`.
3.  **Origin Fetch:** SpinelDB sends a `GET` request to the origin URL.
4.  **Size Check:** The origin responds with the image data and a `Content-Length: 5000000` header (5 MB).
5.  **Decision:** SpinelDB sees that 5,000,000 bytes is greater than the configured `streaming_threshold_bytes` (1,048,576).
6.  **Stream to Disk:** Instead of buffering the 5 MB response in memory, SpinelDB creates a temporary file in the `spineldb_cache_files` directory and streams the response body directly into it.
7.  **Store Metadata:** Once the download is complete, SpinelDB stores the metadata for the cache item (TTL, SWR, tags, etc.) in its main in-memory database, along with a **pointer** to the file on disk. The large image data itself is *not* in RAM.
8.  **Serve Client:** SpinelDB serves the content to the original client.

### Subsequent Requests

When another client requests `CACHE.PROXY images:product:poster-xyz`:
1.  **Cache Hit:** SpinelDB finds the key in memory.
2.  **Metadata Check:** It sees that the body of the object is located on disk at a specific path.
3.  **Stream from Disk:** SpinelDB opens the file from disk and streams its contents directly to the client's socket. This is a significant performance optimization, as the large object is never fully loaded into the server's main memory, reducing memory pressure and improving throughput.

The object is served efficiently without ever being fully loaded into the server's main memory.

---

## 3. Garbage Collection

SpinelDB automatically manages the lifecycle of these on-disk files. When a cache item is evicted (due to TTL expiration, `DEL`, or `CACHE.PURGETAG`), SpinelDB will automatically delete the corresponding file from the `on_disk_path` directory.

Additionally, a background task runs periodically to scan the cache directory and remove any orphaned files that might have been left behind from an unclean shutdown, ensuring your disk usage doesn't grow indefinitely.

### Benefits of Hybrid Caching

*   **Vastly Increased Cache Capacity:** Your cache is no longer limited by RAM but by available disk space, which is typically much cheaper and more plentiful.
*   **Improved Memory Stability:** Prevents large, infrequent requests from evicting many small, frequently used items, leading to a more stable cache hit ratio.
*   **Reduced Memory Fragmentation:** Offloading large allocations to the filesystem can help reduce memory fragmentation in the main server process over time.

This hybrid approach makes SpinelDB an exceptionally powerful and memory-efficient caching solution for a wide range of content types.

➡️ **Next Chapter: [4e. Content Negotiation with `Vary`](./04e-content-negotiation-vary.md)**
