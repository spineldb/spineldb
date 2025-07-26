# Chapter 4c: Tag-Based Invalidation

Tag-based invalidation is a powerful feature that allows you to invalidate multiple cache entries at once, even if they don't share a common key pattern. This is especially useful when a single piece of data is used in multiple cached results.

## The Problem

Imagine you have an e-commerce application. You might cache:
*   The user's profile (`GET user:123`)
*   The user's recent orders (`LGET orders:user:123`)
*   The user's shopping cart (`HGETALL cart:user:123`)

If the user updates their profile, you would need to manually invalidate all three of these cache entries. This can be complex and error-prone.

## The Solution: Tags

With tags, you can associate multiple cache entries with a single tag. Then, you can invalidate all entries associated with that tag in a single command.

### Using Tags with `CACHE.PROXY`

You can specify tags when defining a cache policy. All cache entries created using that policy will be associated with the specified tags.

**1. Define a Cache Policy with Tags**

Let's create a policy for user-related data, and associate it with a tag based on the user's ID.

```
> CACHE.POLICY user_data_policy TTL 3600 TAGS "user:{id}"
OK
```

**2. Use `CACHE.PROXY` with the Tagged Policy**

Now, when you use `CACHE.PROXY` with this policy, the tags will be automatically applied.

```
> CACHE.PROXY user_data_policy GET user:123
...

> CACHE.PROXY user_data_policy LGET orders:user:123
...
```

**3. Invalidate by Tag**

Now, if user `123` updates their profile, you can invalidate all related cache entries with a single command:

```
> CACHE.PURGETAG "user:123"
(integer) 2
```

This command will invalidate the cached results for both `GET user:123` and `LGET orders:user:123`, ensuring that the next time they are requested, fresh data will be fetched.

Tag-based invalidation simplifies cache management and reduces the risk of stale data.