# Chapter 4b: Declarative Caching with Policies & `CACHE.PROXY`

In the previous guide, you learned how to manually manage the cache lifecycle using `CACHE.SET` and `CACHE.GET`. While powerful, this approach requires your application to handle the logic for when and how to cache items.

SpinelDB offers a far superior method: **Declarative Caching**. Instead of telling SpinelDB *how* to cache each item, you simply declare your caching strategy in the `config.toml` file, and SpinelDB handles the rest.

The `CACHE.PROXY` command is the key to this workflow. It acts as a smart, atomic *get-or-fetch* operator. When you call it, SpinelDB will:
1.  Attempt to get the item from the cache.
2.  If it's a miss, it will automatically look up the matching policy.
3.  It will then fetch the content from the URL defined in the policy.
4.  Finally, it will store the result in the cache according to the policy's rules and return the content to you.

This single command replaces a complex sequence of application-level logic (`GET`, check for nil, `FETCH`, `SET`), making your code cleaner and more resilient.

---

## 1. Defining Cache Policies

Cache policies are defined in your `config.toml` file within an array table named `[[cache.policy]]`. A policy links a glob-style key pattern to a URL template and caching rules.

### Example Policy Configuration

Let's define two policies in our `config.toml`: one for user profile API responses and one for product images.

```toml
# In your config.toml

# [[cache.policy]] is an array table, you can define as many as you need.

[[cache.policy]]
# A unique name for this policy
name = "api-user-profiles"
# A glob pattern that matches the keys this policy applies to
key_pattern = "api:user:*"
# The URL template. {1} will be replaced by the part of the key matched by the '*'
url_template = "https://api.myapp.com/users/{1}"
# Caching rules for items matching this policy
ttl = 300            # 5 minutes fresh
swr = 3600           # 1 hour stale-while-revalidate
grace = 86400        # 1 day grace period if origin is down
# A list of tags to automatically apply
tags = ["api", "user-data"]

[[cache.policy]]
name = "product-images"
key_pattern = "images:product:*"
url_template = "https://cdn.myapp.com/images/{1}.jpg"
ttl = 86400          # 1 day fresh
swr = 604800         # 1 week stale-while-revalidate
# No grace period for images, we prefer they don't show up if the CDN is down
tags = ["images", "product-assets"]
```
**Important:** You must restart your SpinelDB server after adding or changing policies in the `config.toml` file.

### Policy Precedence Rule

When a key is evaluated against the list of policies, SpinelDB uses a **"first match wins"** rule. The policies are checked in the order they appear in your `config.toml` file. The first policy where the `key_pattern` matches the key will be used. Subsequent policies will be ignored for that key.

Therefore, you should order your policies from **most specific** to **most general** to ensure the correct policy is applied.

### Understanding URL Interpolation

The `url_template` is dynamic. The placeholder `{1}` corresponds to the first `*` in the `key_pattern`. `{2}` would correspond to the second `*`, and so on.

*   If `key_pattern` is `api:user:*`, a request for the key `api:user:123` will use `{1}` = `123`.
*   If `key_pattern` is `api:*:details:*`, a request for `api:user:details:123` will use `{1}` = `user` and `{2}` = `123`.

---

## 2. Using `CACHE.PROXY`

With our policies defined, our application logic becomes incredibly simple. Instead of a complex chain of commands, we just use one: `CACHE.PROXY`.

**Command:** `CACHE.PROXY key [options...]`

The command accepts the same optional arguments as `CACHE.SET` (like `TTL`, `SWR`, `TAGS`), which will **override** the values from the matched policy for that specific request.

### Example Session

Let's request a user profile. The key `api:user:123` matches our `api-user-profiles` policy.

**First Request (Cache Miss):**

```shell
127.0.0.1:7878> CACHE.PROXY api:user:123
# 1. SpinelDB checks the cache for 'api:user:123'. It's a miss.
# 2. It finds the 'api-user-profiles' policy matches the key.
# 3. It interpolates the URL to "https://api.myapp.com/users/123".
# 4. It fetches the content from the URL.
# 5. It stores the content under the key 'api:user:123' with the TTL, SWR, and tags from the policy.
# 6. It returns the fetched content to the client.
"{\"id\": 123, \"name\": \"Charlie\", \"email\": \"charlie@example.com\"}"
```

**Second Request (Cache Hit):**

Now, if we request the same key again within the 5-minute TTL:

```shell
127.0.0.1:7878> CACHE.PROXY api:user:123
# 1. SpinelDB checks the cache and finds the item. It's fresh.
# 2. It returns the cached content instantly.
"{\"id\": 123, \"name\": \"Charlie\", \"email\": \"charlie@example.com\"}"
```
The entire SWR and Grace Period logic you learned about in the previous chapter is now handled **automatically** for you by SpinelDB whenever you use `CACHE.PROXY`.

### Overriding Policy Rules

Sometimes you need to override a policy for a specific request. For example, maybe a high-priority background job needs to ensure it gets the absolute freshest data.

```shell
# This will force the TTL for this specific item to be only 10 seconds,
# ignoring the 300 seconds from the policy.
127.0.0.1:7878> CACHE.PROXY api:user:123 TTL 10
"{\"id\": 123, \"name\": \"Charlie\", \"email\": \"charlie@example.com\"}"
```

---

### Simplify Your Architecture

By adopting the declarative caching pattern with policies and `CACHE.PROXY`, you can remove a significant amount of complex, stateful logic from your application layer. Your caching strategy becomes centralized, consistent, and easy to manage, allowing your application code to focus on its core business logic.

➡️ **Next Chapter: [4c. Tag-Based Invalidation](./04c-tag-based-invalidation.md)**
