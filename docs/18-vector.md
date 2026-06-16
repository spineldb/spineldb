# 18-SpinelVector Commands

SpinelDB provides vector similarity search commands powered by SpinelVector. A SpinelVector index stores high-dimensional vectors and supports fast approximate nearest-neighbor (ANN) search using the HNSW (Hierarchical Navigable Small World) algorithm.

## Index Management

### VS.RESERVE key dimension metric [CAPACITY capacity] [M m] [EF_CONSTRUCTION ef] [EF_SEARCH ef]

Creates a new vector index with the specified parameters.

-   **key**: The name of the vector index to create.
-   **dimension**: The dimensionality of vectors to be stored (1–65536).
-   **metric**: The distance metric to use: `L2` (Euclidean), `COSINE`, or `IP` (inner product).
-   **CAPACITY**: Maximum number of vectors (default: 10000).
-   **M**: Maximum number of connections per node in the HNSW graph (default: 16, minimum: 2).
-   **EF_CONSTRUCTION**: Size of the dynamic candidate list during index building (default: 200, minimum: 1).
-   **EF_SEARCH**: Size of the dynamic candidate list during search (default: 10, minimum: 1).

**Return Value:** `OK` on success.

**Examples:**

```
VS.RESERVE myapp:embeddings 128 COSINE
VS.RESERVE myapp:vectors 768 L2 CAPACITY 100000 M 32 EF_CONSTRUCTION 400
VS.RESERVE myapp:index 512 IP EF_SEARCH 20
```

### VS.REBUILD key

Manually triggers a full rebuild of the internal HNSW graph index. Useful after bulk deletions to reclaim memory and restore search quality.

-   **key**: The name of the vector index.

**Return Value:** Integer — the number of vectors in the index before rebuild.

**Examples:**

```
VS.REBUILD myapp:embeddings
```

### VS.OPTIMIZE key

Compacts the index by removing tombstoned/deleted entries and optimizing storage.

-   **key**: The name of the vector index.

**Return Value:** Integer — the number of entries removed during optimization.

**Examples:**

```
VS.OPTIMIZE myapp:embeddings
```

## Vector CRUD

### VS.ADD key id vector [METADATA metadata]

Adds a vector to an existing index.

-   **key**: The name of the vector index.
-   **id**: A unique string identifier for the vector.
-   **vector**: One or more floating-point values representing the vector data, separated by spaces.
-   **metadata**: Optional string metadata to associate with the vector (JSON recommended for filtering).

**Return Value:** `1` if a new vector was added, `0` if an existing vector was overwritten. Error if the index is full, dimension mismatches, or the vector contains NaN/Inf.

**Examples:**

```
VS.ADD myapp:embeddings vec:001 0.1 0.2 0.3 0.4
VS.ADD myapp:embeddings vec:002 0.5 0.6 0.7 0.8 METADATA "{\"user\":\"123\"}"
```

### VS.MADD key [id vector [METADATA metadata] ...]

Adds multiple vectors to an index in a single command.

-   **key**: The name of the vector index.
-   **id vector**: Pairs of ID and vector data, optionally followed by `METADATA`.

**Return Value:** An array of results, one per input pair. Each result is `1` (new) or `0` (overwrite) on success, or an error message.

**Examples:**

```
VS.MADD myapp:embeddings v1 0.1 0.2 0.3 v2 0.4 0.5 0.6
VS.MADD myapp:embeddings v1 0.1 0.2 METADATA "{\"a\":1}" v2 0.3 0.4 METADATA "{\"b\":2}"
```

### VS.GET key id

Retrieves a vector by its ID.

-   **key**: The name of the vector index.
-   **id**: The vector identifier.

**Return Value:** An array `[id, [vector], metadata]` if found. `nil` if the vector does not exist.

**Examples:**

```
VS.GET myapp:embeddings vec:001
```

### VS.UPDATE key id [VECTOR v1 v2 ...] [METADATA metadata]

Updates a vector's embedding and/or metadata in place. At least one of `VECTOR` or `METADATA` must be provided. Updating the vector triggers an incremental HNSW node update (no full rebuild needed).

-   **key**: The name of the vector index.
-   **id**: The vector identifier.
-   **VECTOR**: New vector values (optional).
-   **METADATA**: New metadata (optional).

**Return Value:** `OK` on success.

**Examples:**

```
VS.UPDATE myapp:embeddings vec:001 VECTOR 0.9 0.8 0.7 0.6
VS.UPDATE myapp:embeddings vec:001 METADATA "{\"user\":\"456\"}"
VS.UPDATE myapp:embeddings vec:001 VECTOR 0.1 0.2 0.3 METADATA "{\"updated\":true}"
```

### VS.DEL key id [id ...]

Removes one or more vectors by ID. Uses lazy deletion — the entry is tombstoned and removed from the HNSW graph, but storage is not reclaimed until `VS.OPTIMIZE` is called.

-   **key**: The name of the vector index.
-   **id**: One or more vector identifiers to remove.

**Return Value:** Integer — the number of vectors successfully removed.

**Examples:**

```
VS.DEL myapp:embeddings vec:001
VS.DEL myapp:embeddings vec:001 vec:002 vec:003
```

### VS.EXISTS key id [id ...]

Checks whether one or more vector IDs exist in the index.

-   **key**: The name of the vector index.
-   **id**: One or more vector identifiers to check.

**Return Value:** An array of integers (1 = exists, 0 = not found), one per input ID.

**Examples:**

```
VS.EXISTS myapp:embeddings vec:001 vec:999
```

### VS.CARD key [FILTER filter]

Returns the number of vectors currently stored in the index.

-   **key**: The name of the vector index.
-   **FILTER**: Optional metadata filter expression. When provided, only vectors matching the filter are counted.

**Return Value:** Integer — the number of vectors in the index (or matching the filter).

**Examples:**

```
VS.CARD myapp:embeddings
VS.CARD myapp:embeddings FILTER "type=a"
```

## Search

### VS.SEARCH key vector [EF ef] [COUNT count] [FILTER filter] [THRESHOLD threshold]

Performs a k-nearest-neighbor similarity search.

-   **key**: The name of the vector index.
-   **vector**: The query vector (floating-point values separated by spaces).
-   **EF**: Size of the dynamic candidate list during search (default: 10). Higher values improve recall at the cost of speed.
-   **COUNT**: Number of nearest neighbors to return (default: 10).
-   **FILTER**: Metadata filter expression to restrict results.
-   **THRESHOLD**: Maximum distance threshold. Results farther than this are excluded.

**Return Value:** An array of results, each containing `[id, distance, [vector], metadata]`, sorted by distance (most similar first).

**Examples:**

```
VS.SEARCH myapp:embeddings 0.1 0.2 0.3 0.4 COUNT 5
VS.SEARCH myapp:embeddings 0.1 0.2 0.3 0.4 EF 100 COUNT 10
VS.SEARCH myapp:embeddings 0.1 0.2 0.3 0.4 FILTER "type=a" THRESHOLD 0.5
```

### VS.MSEARCH key [COUNT count] [EF ef] [FILTER filter] [THRESHOLD threshold] QUERY v1 v2 ... QUERY v1 v2 ...

Performs batch k-nearest-neighbor search with multiple query vectors.

-   **key**: The name of the vector index.
-   **COUNT**: Number of results per query (default: 10).
-   **EF**: Search-time expansion factor (optional).
-   **FILTER**: Metadata filter expression (optional).
-   **THRESHOLD**: Distance threshold (optional).
-   **QUERY**: Delimiter keyword separating multiple query vectors.

**Return Value:** An array of arrays (one per query), each containing `[id, distance, [vector], metadata]` tuples.

**Examples:**

```
VS.MSEARCH myapp:embeddings QUERY 0.1 0.2 0.3 0.4 QUERY 0.5 0.6 0.7 0.8
VS.MSEARCH myapp:embeddings COUNT 3 FILTER "type=a" QUERY 0.1 0.2 QUERY 0.3 0.4
```

### VS.HYBRIDSEARCH key VECTOR vec... TEXT text [COUNT count] [EF ef] [FILTER filter] [WEIGHTS wv wb]

Performs a hybrid search combining vector similarity and BM25 full-text search, merged via Reciprocal Rank Fusion (RRF).

-   **key**: The name of the vector index.
-   **VECTOR**: The query vector values.
-   **TEXT**: The text query for BM25 scoring.
-   **COUNT**: Number of results (default: 10).
-   **EF**: Search-time expansion factor (optional).
-   **FILTER**: Metadata filter expression (optional).
-   **WEIGHTS**: Two floats `wv wb` controlling the blend between vector score and BM25 score (default: 0.5 0.5).

**Return Value:** An array of results, each containing `[id, score, [vector], metadata]`, sorted by fused score.

**Examples:**

```
VS.HYBRIDSEARCH myapp:embeddings VECTOR 0.1 0.2 0.3 TEXT "machine learning" COUNT 5
VS.HYBRIDSEARCH myapp:embeddings VECTOR 0.1 0.2 TEXT "search" WEIGHTS 0.7 0.3
```

### VS.FEDERATEDSEARCH key vector [EF ef] [COUNT count] [FILTER filter] [THRESHOLD threshold]

Performs a scatter-gather k-nearest-neighbor search across **all primary nodes** in a cluster. Each node searches its local data in parallel, then results are merged and deduplicated.

-   **key**: The name of the vector index.
-   **vector**: The query vector (floating-point values separated by spaces).
-   **EF**: Search-time expansion factor (optional).
-   **COUNT**: Number of nearest neighbors to return (default: 10).
-   **FILTER**: Metadata filter expression (optional).
-   **THRESHOLD**: Maximum distance threshold (optional).

**Return Value:** An array of `[id, distance]` tuples, sorted by distance (most similar first). Results are deduplicated across nodes — if the same ID exists on multiple nodes, the best distance is kept.

**Examples:**

```
VS.FEDERATEDSEARCH myapp:embeddings 0.1 0.2 0.3 0.4 COUNT 10
VS.FEDERATEDSEARCH myapp:embeddings 0.1 0.2 0.3 FILTER "type=a" THRESHOLD 0.5
```

## Quantization

### VS.QUANTIZE key method

Enables quantization on a vector index for memory-efficient storage and search.

-   **key**: The name of the vector index.
-   **method**: The quantization method: `INT8` (affine INT8 quantization), `PQ` (Product Quantization), or `NONE` (disable).

**Return Value:** Integer — the number of bytes saved by quantization.

**Examples:**

```
VS.QUANTIZE myapp:embeddings INT8
VS.QUANTIZE myapp:embeddings NONE
```

### VS.TRAINPQ key [SUBSPACES n] [BITS b]

Trains Product Quantization (PQ) codebooks on the existing vectors in the index. Uses k-means++ initialization for faster convergence and better codebook quality.

-   **key**: The name of the vector index.
-   **SUBSPACES**: Number of PQ subspaces (default: dimension/4).
-   **BITS**: Number of bits per subspace code (default: 8).

**Return Value:** An array `[subspaces, bits]` confirming the configuration.

**Examples:**

```
VS.TRAINPQ myapp:embeddings SUBSPACES 8 BITS 8
VS.TRAINPQ myapp:embeddings
```

## TTL & Expiration

### VS.EXPIRE key seconds

Sets a time-to-live (in seconds) on a vector index.

-   **key**: The name of the vector index.
-   **seconds**: TTL in seconds (must be > 0).

**Return Value:** `1` on success.

**Examples:**

```
VS.EXPIRE myapp:embeddings 3600
```

### VS.TTL key

Returns the remaining TTL of a vector index.

-   **key**: The name of the vector index.

**Return Value:**
-   `-1` — no TTL set
-   `-2` — key has expired or does not exist
-   `>= 0` — remaining seconds

**Examples:**

```
VS.TTL myapp:embeddings
```

## Introspection

### VS.INFO key

Returns metadata about a vector index.

-   **key**: The name of the vector index.

**Return Value:** An array of field-value pairs:
-   `dimension` — vector dimensionality
-   `metric` — distance metric (`L2`, `COSINE`, `IP`)
-   `capacity` — maximum number of vectors
-   `size` — current number of stored vectors
-   `m` — max connections per node
-   `ef_construction` — EF parameter used during build
-   `ef_search` — EF parameter used during search
-   `deleted_count` — number of lazily deleted nodes
-   `vectors_added` — total vectors ever added
-   `vectors_deleted` — total vectors ever deleted
-   `memory_usage_bytes` — estimated memory usage in bytes

**Examples:**

```
VS.INFO myapp:embeddings
```

### VS.STATS key

Returns detailed HNSW and index performance statistics.

-   **key**: The name of the vector index.

**Return Value:** An array of field-value pairs:
-   `dimension`, `metric`, `capacity`, `size`, `m`, `ef_construction`, `ef_search`
-   `hnsw_max_level` — current max level in the HNSW graph
-   `hnsw_node_count` — number of nodes in the graph
-   `hnsw_deleted_count` — lazily deleted nodes
-   `hnsw_total_inserts`, `hnsw_total_searches`, `hnsw_total_deletes` — lifetime counters
-   `memory_usage_bytes` — estimated memory usage in bytes

**Examples:**

```
VS.STATS myapp:embeddings
```

## Metadata Filter Syntax

The `FILTER` parameter accepted by `VS.SEARCH`, `VS.MSEARCH`, `VS.CARD`, `VS.HYBRIDSEARCH`, and `VS.FEDERATEDSEARCH` supports the following expression syntax:

| Operator | Example | Description |
|----------|---------|-------------|
| `=` | `type=a` | Equals |
| `!=` | `type!=a` | Not equals |
| `>` | `price>10` | Greater than |
| `>=` | `price>=10` | Greater than or equal |
| `<` | `price<10` | Less than |
| `<=` | `price<=10` | Less than or equal |
| `AND` | `type=a AND status=active` | Logical AND (both must match) |
| `OR` | `type=a OR type=b` | Logical OR (either matches) |
| `NOT` | `NOT type=a` | Negation |

Complex expressions can be combined: `NOT (type=a AND status=deleted)`.

Metadata is stored as JSON. Filter expressions are evaluated against the JSON fields. Values are compared as strings unless both sides are parseable as numbers.

**Performance note:** Equality filters (`=`) use an internal inverted index for O(1) lookup, making them significantly faster than comparison or compound filters on large datasets.

## Distance Metrics

| Metric | Description | Distance Range |
|--------|-------------|----------------|
| `L2` | Euclidean distance (√Σ(a-b)²) | [0, ∞) |
| `COSINE` | Cosine distance (1 - cosine_similarity) | [0, 2] |
| `IP` | Negated inner product (-a·b) | (-∞, 0] |

-   **L2**: Best for general-purpose similarity. Lower distance means more similar.
-   **Cosine**: Best for normalized embeddings (e.g., text embeddings). Measures angle between vectors. Vectors are auto-normalized on insert and search.
-   **Inner Product**: Best for embeddings where magnitude matters. Lower (more negative) distance means more similar.

## HNSW Parameters

The HNSW algorithm uses these parameters to balance speed vs. recall:

-   **M**: Controls the number of connections per node. Higher M improves recall but uses more memory. Default: 16.
-   **EF_CONSTRUCTION**: Controls the candidate list size during index building. Higher values produce a higher quality graph but slower insertion. Default: 200.
-   **EF**: Controls the candidate list size during search. Higher values improve recall but slow down search. Default: 10.

**Recommended settings by use case:**

| Use Case | M | EF_CONSTRUCTION | EF |
|----------|---|-----------------|-----|
| Low latency, moderate recall | 16 | 200 | 10 |
| High recall | 32 | 400 | 50-100 |
| Large scale (>1M vectors) | 16-32 | 200-400 | 10-50 |

## Performance Optimizations

SpinelVector includes several built-in optimizations for production workloads:

-   **Loop-unrolled distance computation**: L2, Cosine, and Inner Product distances use 8-element loop unrolling for automatic compiler vectorization.
-   **K-means++ initialization**: PQ codebook training uses k-means++ seeding for faster convergence and better quantization quality.
-   **Metadata inverted index**: Equality filters (`FILTER key=value`) use an internal inverted index for O(1) lookup instead of scanning all entries.
-   **Incremental HNSW updates**: `VS.UPDATE` reconnects only the updated node instead of rebuilding the entire graph.
-   **Pre-filter during graph traversal**: Metadata filters and distance thresholds are evaluated during HNSW traversal, not after, reducing unnecessary distance computations. Filtered search uses adaptive expansion factor bounded by total entries.
-   **O(1) BM25 average**: Hybrid search maintains a running total of term counts for O(1) average document length updates instead of O(N) re-scans.
-   **Lazy deletion with compaction**: `VS.DEL` marks vectors as tombstones without shifting indices. `VS.OPTIMIZE` compacts storage and rebuilds the metadata index.
-   **Binary serialization**: Compact binary format (v3) with backward compatibility to v1. BM25 state, quantization parameters, and TTL are persisted across restarts.

## Serialization Format

SpinelVector indices are serialized using a compact binary format for persistence (`.spldb` files). The format uses little-endian encoding with the following layout:

```
Header: "SPINELVEC" (9 bytes) | version (1 byte, currently 3)
Config: dimension (4) | metric (1) | max_capacity (8) | vectors_added (8)
        | m (4) | ef_construction (4) | ef_search (4)
State:  vectors_deleted (8) | has_ttl (1) | [ttl (8)] | created_at (8)
Quant:  quantization_method (1) | [quantized data]
Text:   bm25_doc_count (4) | bm25_total_term_count (4) | bm25_avg_doc_len (4)
        | bm25 entries... | bm25 idf... | hybrid_weights (8)
Data:   vector_count (4) | [for each: id + vector + metadata]
```

Version 1 and 2 data files are automatically upgraded to v3 on deserialization.
