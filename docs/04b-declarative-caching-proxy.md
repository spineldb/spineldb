# Chapter 4b: Declarative Caching with CACHE.PROXY

SpinelDB provides a powerful and easy-to-use caching mechanism through the `CACHE.PROXY` command. Instead of manually managing `CACHE.GET` and `CACHE.SET` logic in your application, you can delegate the entire caching process to SpinelDB.

This feature acts as a "proxy" or "wrapper" for other commands. When you execute a command through `CACHE.PROXY`, SpinelDB will:
1.  Create a unique cache key based on the command you provide.
2.  Check if there is a valid result in the cache for that key.
3.  If so, the cached result is returned immediately.
4.  If not, SpinelDB will execute the original command you provided.
5.  The result of the original command is then stored in the cache according to the policy you specify, and returned to you.

This drastically simplifies application logic and ensures caching consistency.

## Command Syntax

The basic syntax for `CACHE.PROXY` is as follows:

```
CACHE.PROXY <policy_name> <command_to_execute> [args...]
```

*   `policy_name`: The name of the caching policy you have previously defined (using `CACHE.POLICY`). This policy determines the TTL, eviction strategy (LRU/LFU), and other parameters.
*   `command_to_execute`: The command whose result you want to cache.
*   `[args...]`: The arguments for the `command_to_execute`.

## Usage Example

Let's look at a practical example. Imagine you have an e-commerce application and want to cache the list of best-selling products, which is obtained from the `ZREVRANGE products:sales 0 9` command.

**1. Define a Cache Policy**

First, we create a cache policy with a TTL of 60 seconds.

```
> CACHE.POLICY my_policy TTL 60
OK
```

**2. Use CACHE.PROXY**

Now, we use `CACHE.PROXY` to execute and cache the `ZREVRANGE` command.

```
> CACHE.PROXY my_policy ZREVRANGE products:sales 0 9
1) "product:123"
2) "product:456"
...
```

**Analysis:**
*   **First Call**: SpinelDB does not find a cache for this command. It will execute `ZREVRANGE products:sales 0 9`, store the result under the `my_policy` policy, and then return the result.
*   **Subsequent Calls (within 60 seconds)**: SpinelDB will find a valid cache entry and immediately return the result without having to run the potentially expensive `ZREVRANGE` command again.
*   **After 60 seconds**: The cache will expire. The next call will again execute the original command and store the result back in the cache.

With `CACHE.PROXY`, you get the benefits of high-performance caching without having to write complex `if/else` logic in your application code.