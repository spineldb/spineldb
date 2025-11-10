# 15-Bloom Filter Commands

SpinelDB provides a set of commands to interact with probabilistic Bloom filters. A Bloom filter is a space-efficient probabilistic data structure that is used to test whether an element is a member of a set. False positive matches are possible, but false negatives are not.

### Bloom Filter Structure

![Bloom Filter Structure](./diagram/bloom-filter.png)

## BF.RESERVE key error_rate capacity

Creates a new Bloom filter with a specified error rate and initial capacity.

-   **key**: The name of the Bloom filter to create.
-   **error_rate**: The desired probability of false positives, a floating-point number between 0 and 1 (exclusive). A lower value means a lower false positive rate but requires more memory.
-   **capacity**: The expected number of items to be added to the filter. This influences the size of the filter.

**Return Value:**
-   `OK` on success.
-   Error if the key already exists or if `error_rate` or `capacity` are invalid.

**Examples:**

```
BF.RESERVE myapp:users:bloom 0.01 10000
```
This creates a Bloom filter named `myapp:users:bloom` that can hold approximately 10,000 items with a 1% chance of false positives.

## BF.ADD key item

Adds an item to a Bloom filter. If the Bloom filter specified by the key does not exist, it is implicitly created with default parameters (capacity 100, error rate 0.01).

-   **key**: The name of the Bloom filter.
-   **item**: The item to add to the filter.

**Return Value:**
-   `1` if the item was added (or might have been added, meaning it was not certainly present before).
-   `0` if the item was already considered present in the filter.
-   Error if the key exists but holds a different data type.

**Examples:**

```
BF.ADD myapp:users:bloom user:123
BF.ADD myapp:users:bloom user:456
```

## BF.EXISTS key item

Checks if an item might be present in a Bloom filter.

-   **key**: The name of the Bloom filter.
-   **item**: The item to check for existence.

**Return Value:**
-   `1` if the item might exist in the Bloom filter (a false positive is possible).
-   `0` if the item definitely does not exist in the Bloom filter.
-   If the key does not exist, it is treated as if the item does not exist in an empty filter, returning `0`.
-   Error if the key exists but holds a different data type.

**Examples:**

```
BF.EXISTS myapp:users:bloom user:123  // Returns 1
BF.EXISTS myapp:users:bloom user:789  // Returns 0
BF.EXISTS non_existent_bloom item:abc // Returns 0
```

## Error Conditions

-   `WRONGTYPE Operation against a key holding the wrong kind of value`: Occurs when attempting to use Bloom filter commands on a key that holds a different data type (e.g., a string or a list).
-   `Key already exists`: Returned by `BF.RESERVE` if a Bloom filter with the specified key already exists.
-   `Invalid request: error rate must be between 0 and 1`: Returned by `BF.RESERVE` if the `error_rate` is not within the valid range.
-   `Invalid request: capacity must be greater than 0`: Returned by `BF.RESERVE` if the `capacity` is 0.
