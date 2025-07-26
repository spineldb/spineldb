# Chapter 4c: Tag-Based Invalidation

One of the hardest problems in caching is **cache invalidation**. When underlying data changes, how do you efficiently remove all the related, now-outdated entries from your cache?

Consider an e-commerce site. When a product's price is updated, you might need to invalidate:
*   The product details page (`/products/123`).
*   The product's entry in various category listings (`/categories/electronics`).
*   The user's shopping cart if it contains that product.

Keeping track of every single key that needs to be purged is complex and error-prone. SpinelDB solves this with a powerful feature: **tag-based invalidation**.

The concept is simple:
1.  **Tag your data:** When you store an item in the cache, you associate it with one or more string tags.
2.  **Purge by tag:** When the underlying data changes, you issue a single command, `CACHE.PURGETAG`, with the relevant tag(s). SpinelDB will then atomically find and delete all cache items associated with those tags.

---

## 1. Tagging Cache Items

You can add tags to a cache item in two ways: manually with `CACHE.SET` or automatically with a cache policy.

### Method 1: Manual Tagging with `CACHE.SET`

You can add tags at the end of a `CACHE.SET` command using the `TAGS` keyword.

**Command:** `CACHE.SET key value ... TAGS tag1 [tag2 ...]`

#### Example Session

Let's cache a few pieces of data related to a user, tagging them appropriately.

```shell
# Cache the user's main profile page, tagging it with a unique user ID tag.
127.0.0.1:7878> CACHE.SET page:user:123 '<html>...</html>' TTL 3600 TAGS user:123

# Cache the user's order history, tagging it with the same user ID and an "orders" tag.
127.0.0.1:7878> CACHE.SET api:orders:user:123 '[...]' TTL 600 TAGS user:123 orders

# Cache a global list of all users, which includes user 123.
127.0.0.1:7878> CACHE.SET page:users:all '[...]' TTL 300 TAGS all-users
```

### Method 2: Automatic Tagging with Policies

For a more maintainable setup, you can define tags directly in your `config.toml` cache policies. Any item cached via `CACHE.PROXY` that matches the policy will automatically receive its tags.

#### Example Policy Configuration

```toml
# In your config.toml

[[cache.policy]]
name = "user-api-data"
key_pattern = "api:user:*:profile"
url_template = "https://api.myapp.com/users/{1}"
ttl = 300
# {1} is interpolated here too! The tag will be "user:123" for key "api:user:123:profile".
tags = ["user-data", "user:{1}"]

[[cache.policy]]
name = "product-pages"
key_pattern = "page:product:*"
url_template = "https://www.myapp.com/products/{1}"
ttl = 3600
# You can have multiple dynamic tags.
tags = ["product-page", "product:{1}"]
```

---

## 2. Purging Items with `CACHE.PURGETAG`

This is the command that makes the magic happen. You can provide one or more tags, and SpinelDB will find and delete all keys associated with *any* of the provided tags. The command is atomic and returns the number of items that were purged.

**Command:** `CACHE.PURGETAG tag1 [tag2 ...]`

### Example Session

Imagine user 123 has updated their profile information. We need to invalidate all cached data related to them.

```shell
# Using the tags we set up in the first example...
127.0.0.1:7878> CACHE.PURGETAG user:123
(integer) 2
```
**What just happened?**
SpinelDB found all keys associated with the `user:123` tag (`page:user:123` and `api:orders:user:123`) and deleted them from the cache. The `page:users:all` key was not affected because it did not have that tag. The next time these keys are requested, they will be fetched fresh from the origin.

This is incredibly powerful. Your application no longer needs to know about every single key. It only needs to know the logical entity that changed (e.g., `user:123` or `product:abc`) and purge the corresponding tag.

### Purging Multiple Tags

You can purge multiple tags in a single, atomic operation.

```shell
# Imagine a site-wide change requires clearing all user data and all product pages.
127.0.0.1:7878> CACHE.PURGETAG user-data product-page
(integer) 500  # Assuming 500 items were purged in total
```

---

### A Scalable Invalidation Strategy

Tag-based invalidation decouples your application logic from your caching implementation. It provides a clean, scalable, and maintainable way to ensure your cache stays consistent with your source of truth.

#### Tag Invalidation in a Clustered Environment

In a distributed SpinelDB cluster, ensuring consistent tag invalidation across all nodes is crucial. SpinelDB achieves this using a global, monotonically increasing **"purge epoch"** counter. When a `CACHE.PURGETAG` command is executed on any node, the cluster's global purge epoch is incremented.

Each cached item also stores the `tags_epoch` at the time it was last set. When a `CACHE.GET` request comes in, SpinelDB not only checks the item's TTL but also compares its stored `tags_epoch` with the current global purge epoch for any of its associated tags. If the item's `tags_epoch` is older than the current purge epoch for any of its tags, the item is considered stale and is immediately invalidated, even if its TTL has not yet expired.

This mechanism ensures that a `CACHE.PURGETAG` command issued on one node will effectively invalidate relevant cache entries across the entire cluster, providing strong consistency for your cached data.

➡️ **Next Chapter: [4d. Handling Large Objects with On-Disk Caching](./04d-on-disk-caching.md)**
