# SpinelDB Testing Strategy & Implementation Plan

## Executive Summary

This document outlines a comprehensive testing strategy for SpinelDB to achieve high test coverage (>80%) and ensure reliability across all features. The plan includes unit tests, integration tests, property-based tests, and performance benchmarks.

**Current State:**
- ✅ 67 test files with 413 test cases
- ✅ Comprehensive unit tests for command parsing
- ❌ Missing integration tests for command execution
- ❌ Missing tests for complex workflows (transactions, replication, etc.)
- ❌ Missing property-based tests

**Target State:**
- 🎯 >80% code coverage
- 🎯 Integration tests for all command categories
- 🎯 Property-based tests for data consistency
- 🎯 Performance benchmarks for critical paths

---

## Table of Contents

1. [Current State Analysis](#current-state-analysis)
2. [Recommended Test Structure](#recommended-test-structure)
3. [Unit Test Placement Strategy](#unit-test-placement-strategy)
4. [Implementation Phases](#implementation-phases)
5. [Coverage Targets](#coverage-targets)
6. [Test Guidelines](#test-guidelines)
7. [Tools & Dependencies](#tools--dependencies)
8. [Examples](#examples)

---

## Current State Analysis

### Existing Test Coverage

**Unit Tests (Parsing):**
- ✅ Command parsing validation (`ParseCommand` trait)
- ✅ Error handling for invalid arguments
- ✅ Type validation
- ✅ Argument count validation

**Test Files Structure:**
```
tests/
├── unit_*_test.rs (67 files)
└── ping_test.rs
```

**Coverage Areas:**
- Command parsing: ~90% coverage
- Command execution: ~10% coverage (minimal)
- Integration flows: ~0% coverage
- Error paths: ~30% coverage

### Gaps Identified

1. **No Integration Tests**
   - Commands are tested in isolation (parsing only)
   - No end-to-end command execution tests
   - No database state validation

2. **Missing Test Categories**
   - Transaction workflows (MULTI/EXEC/WATCH)
   - Replication scenarios
   - PubSub message flow
   - Persistence (AOF, snapshots)
   - Cache operations
   - Cluster operations

3. **Limited Error Testing**
   - Edge cases not fully covered
   - Concurrent access scenarios
   - Resource exhaustion cases

---

## Recommended Test Structure

### Directory Layout

```
tests/
├── unit/                          # Unit tests (parsing, utilities)
│   ├── mod.rs
│   ├── command_parsing/           # Existing unit_*_test.rs files
│   │   ├── mod.rs
│   │   ├── string_commands.rs
│   │   ├── list_commands.rs
│   │   ├── hash_commands.rs
│   │   ├── set_commands.rs
│   │   ├── zset_commands.rs
│   │   └── ...
│   ├── data_structures/           # Test storage, TTL, etc.
│   │   ├── mod.rs
│   │   ├── storage_test.rs
│   │   ├── ttl_test.rs
│   │   └── eviction_test.rs
│   └── helpers/                    # Test utility functions
│       ├── mod.rs
│       └── protocol_test.rs
│
├── integration/                   # Integration tests (NEW - Priority 1)
│   ├── mod.rs
│   ├── test_helpers.rs            # Database setup utilities
│   ├── fixtures.rs                # Test data fixtures
│   ├── string_commands_test.rs    # SET, GET, APPEND, etc.
│   ├── list_commands_test.rs      # LPUSH, RPOP, etc.
│   ├── hash_commands_test.rs     # HSET, HGET, etc.
│   ├── set_commands_test.rs       # SADD, SMEMBERS, etc.
│   ├── zset_commands_test.rs      # ZADD, ZRANGE, etc.
│   ├── stream_commands_test.rs    # XADD, XREAD, etc.
│   ├── json_commands_test.rs      # JSON.SET, JSON.GET, etc.
│   ├── geospatial_test.rs         # GEOADD, GEORADIUS, etc.
│   ├── transaction_test.rs        # MULTI, EXEC, WATCH
│   ├── persistence_test.rs        # BGSAVE, AOF, persistence
│   ├── replication_test.rs        # PSYNC, replication flow
│   ├── pubsub_test.rs             # PUBLISH, SUBSCRIBE
│   ├── cache_test.rs              # Cache commands
│   ├── acl_test.rs                # ACL commands
│   ├── cluster_test.rs            # Cluster commands
│   └── blocking_test.rs           # Blocking operations
│
├── property/                      # Property-based tests (NEW - Priority 2)
│   ├── mod.rs
│   ├── roundtrip_test.rs          # SET/GET, HSET/HGET roundtrips
│   ├── consistency_test.rs        # Data consistency properties
│   └── serialization_test.rs      # Serialization/deserialization
│
├── performance/                   # Benchmark tests (NEW - Priority 3)
│   ├── mod.rs
│   ├── command_bench.rs           # Command execution benchmarks
│   ├── concurrent_bench.rs        # Concurrent access benchmarks
│   └── memory_bench.rs             # Memory usage benchmarks
│
└── common/                        # Shared test utilities
    ├── mod.rs
    ├── fixtures.rs                # Common test data
    └── assertions.rs              # Custom assertion helpers
```

### Test Categories

#### 1. Unit Tests (`tests/unit/`)

**Purpose:** Test individual components in isolation

**Scope:**
- Command parsing logic
- Data structure operations
- Utility functions
- Error handling

**Characteristics:**
- Fast execution (< 1ms per test)
- No external dependencies
- Deterministic
- High coverage of edge cases

**Example:**
```rust
#[tokio::test]
async fn test_set_parse_basic() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key")),
        RespFrame::BulkString(Bytes::from_static(b"value")),
    ];
    let cmd = Set::parse(&args).unwrap();
    assert_eq!(cmd.key, Bytes::from_static(b"key"));
}
```

#### 2. Integration Tests (`tests/integration/`)

**Purpose:** Test command execution end-to-end with real database state

**Scope:**
- Full command execution flow
- Database state changes
- Multi-command workflows
- Error propagation
- Concurrent operations

**Characteristics:**
- Slower execution (10-100ms per test)
- Requires database setup
- May test multiple components together
- Realistic scenarios

**Example:**
```rust
#[tokio::test]
async fn test_set_get_flow() {
    let ctx = TestContext::new().await;
    
    // SET command
    let result = ctx.execute(Command::Set(...)).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    
    // GET command
    let result = ctx.execute(Command::Get(...)).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Bytes::from("value")));
}
```

#### 3. Property-Based Tests (`tests/property/`)

**Purpose:** Test invariants and properties that should always hold

**Scope:**
- Roundtrip properties (SET/GET, HSET/HGET)
- Data consistency
- Serialization correctness
- Mathematical properties

**Characteristics:**
- Uses `proptest` or `quickcheck`
- Generates random inputs
- Tests properties, not specific cases
- Catches edge cases automatically

**Example:**
```rust
proptest! {
    #[test]
    fn test_set_get_roundtrip(
        key in "[a-z]{1,100}",
        value in "[a-z]{1,1000}"
    ) {
        let ctx = TestContext::new().await;
        ctx.execute(Command::Set(...)).await.unwrap();
        let result = ctx.execute(Command::Get(...)).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Bytes::from(value)));
    }
}
```

#### 4. Performance Tests (`tests/performance/`)

**Purpose:** Ensure performance doesn't regress

**Scope:**
- Command execution time
- Throughput under load
- Memory usage
- Concurrent access patterns

**Characteristics:**
- Uses `criterion` for benchmarking
- Not run in regular test suite
- Tracked over time
- Performance regression detection

---

## Unit Test Placement Strategy

### Hybrid Approach

We use a **hybrid approach** combining both `tests/` directory and in-module `#[cfg(test)]` tests to maximize maintainability and test coverage.

**Rationale:**
- Different types of tests have different requirements
- Some tests need access to private/internal functions
- Some tests are better organized separately
- Balance between co-location and separation of concerns

### When to Use In-Module Tests (`#[cfg(test)]`)

**Use `#[cfg(test)]` modules when:**

1. **Testing Private Functions**
   - Internal helper functions
   - Private methods that aren't exposed via public API
   - Module-specific utilities

2. **Testing Internal Data Structures**
   - Internal state management
   - Data structure internals
   - Implementation details that need verification

3. **Tightly Coupled Tests**
   - Tests that are tightly coupled to specific implementation
   - Tests that require access to module internals
   - Tests that verify internal invariants

4. **Module-Specific Edge Cases**
   - Edge cases specific to a single module
   - Internal error handling paths
   - Internal validation logic

**Example:**
```rust
// src/core/database/core.rs
impl Db {
    /// Calculates the shard index for a given key.
    fn calculate_shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_calculate_shard_index_distribution() {
        let db = Db::new();
        let mut shard_counts = vec![0; NUM_SHARDS];
        
        // Test that keys are distributed across shards
        for i in 0..1000 {
            let key = Bytes::from(format!("key_{}", i));
            let shard = db.calculate_shard_index(&key);
            shard_counts[shard] += 1;
        }
        
        // Verify reasonable distribution
        let min = shard_counts.iter().min().unwrap();
        let max = shard_counts.iter().max().unwrap();
        assert!(*max - *min < 50, "Shard distribution should be relatively even");
    }
    
    #[test]
    fn test_calculate_shard_index_consistency() {
        let db = Db::new();
        let key = Bytes::from("test_key");
        
        // Same key should always map to same shard
        let shard1 = db.calculate_shard_index(&key);
        let shard2 = db.calculate_shard_index(&key);
        assert_eq!(shard1, shard2);
    }
}
```

### When to Use Directory Tests (`tests/`)

**Use `tests/` directory when:**

1. **Testing Public API**
   - Command parsing (existing pattern)
   - Public functions and methods
   - Exported types and traits

2. **Cross-Module Tests**
   - Tests that span multiple modules
   - Integration-style unit tests
   - Tests requiring multiple components

3. **Complex Setup**
   - Tests requiring test helpers
   - Tests with shared fixtures
   - Tests with external dependencies

4. **Test Organization**
   - Grouping related tests together
   - Integration tests
   - Property-based tests
   - Performance benchmarks

**Example:**
```rust
// tests/unit/string_commands.rs
use spineldb::core::commands::string::set::Set;
use spineldb::core::commands::command_trait::ParseCommand;
use spineldb::core::protocol::RespFrame;
use bytes::Bytes;

#[tokio::test]
async fn test_set_parse_basic() {
    let args = [
        RespFrame::BulkString(Bytes::from_static(b"key")),
        RespFrame::BulkString(Bytes::from_static(b"value")),
    ];
    let cmd = Set::parse(&args).unwrap();
    assert_eq!(cmd.key, Bytes::from_static(b"key"));
}
```

### Recommended Placement by Module

| Module | Test Location | Rationale |
|--------|--------------|-----------|
| `src/core/commands/*` | `tests/unit/` | Public API, command parsing |
| `src/core/database/core.rs` | `#[cfg(test)]` | Private shard logic, internal state |
| `src/core/storage/*` | `#[cfg(test)]` | Internal data structures, TTL logic |
| `src/core/protocol/*` | `#[cfg(test)]` | Internal parsing logic |
| `src/core/commands/helpers.rs` | `#[cfg(test)]` | Private utility functions |
| `src/core/handler/*` | `tests/integration/` | Cross-module, requires setup |
| `src/core/persistence/*` | `tests/integration/` | Requires file I/O, complex setup |
| `src/core/replication/*` | `tests/integration/` | Network, multi-instance setup |

### Guidelines Summary

**Decision Tree:**

```
Is the function/component being tested:
├─ Public API?
│  └─ Use tests/ directory
├─ Private/internal function?
│  └─ Use #[cfg(test)] module
├─ Cross-module test?
│  └─ Use tests/ directory
├─ Requires complex setup?
│  └─ Use tests/ directory
└─ Module-specific internal logic?
   └─ Use #[cfg(test)] module
```

**Best Practices:**

1. **Start with `tests/` directory** for new tests (follows existing pattern)
2. **Add `#[cfg(test)]` modules** when you need to test private functions
3. **Keep in-module tests focused** on the specific module's internals
4. **Use descriptive test names** regardless of location
5. **Document why** you chose a particular location for complex cases

**Benefits of Hybrid Approach:**

✅ **Maintainability:** Tests are organized logically  
✅ **Coverage:** Can test both public and private APIs  
✅ **Flexibility:** Choose the right location for each test  
✅ **Consistency:** Follows Rust community best practices  
✅ **Discoverability:** Tests are easy to find and understand

---

## Implementation Phases

### Phase 1: Foundation (Week 1-2)

**Goal:** Set up infrastructure for integration testing

**Tasks:**
1. ✅ Create `tests/integration/` directory structure
2. ✅ Implement `TestContext` helper
3. ✅ Create test fixtures and utilities
4. ✅ Write integration tests for basic string commands (SET, GET, DEL)
5. ✅ Set up CI to run integration tests

**Deliverables:**
- `tests/integration/test_helpers.rs` with `TestContext`
- `tests/integration/string_commands_test.rs` with 20+ test cases
- Integration test documentation

**Success Criteria:**
- Integration tests run in CI
- Basic string commands have >80% coverage
- Test execution time < 5 seconds

### Phase 2: Core Data Types (Week 3-4)

**Goal:** Complete integration tests for all core data types

**Tasks:**
1. ✅ List commands integration tests
2. ✅ Hash commands integration tests
3. ✅ Set commands integration tests
4. ✅ Sorted Set commands integration tests
5. ✅ Stream commands integration tests

**Deliverables:**
- Integration tests for all data type commands
- >70% coverage for command execution paths
- Test utilities for common patterns

**Success Criteria:**
- All data type commands have integration tests
- Coverage report shows >70% for command execution
- No regressions in existing tests

### Phase 3: Advanced Features (Week 5-6)

**Goal:** Test complex features and workflows

**Tasks:**
1. ✅ Transaction tests (MULTI/EXEC/WATCH)
2. ✅ Persistence tests (BGSAVE, AOF)
3. ✅ PubSub tests
4. ✅ Cache command tests
5. ✅ ACL tests

**Deliverables:**
- Integration tests for advanced features
- Edge case coverage
- Error scenario tests

**Success Criteria:**
- All advanced features have test coverage
- Edge cases documented and tested
- >75% overall code coverage

### Phase 4: Property-Based & Performance (Week 7-8)

**Goal:** Add property-based tests and benchmarks

**Tasks:**
1. ✅ Set up `proptest` infrastructure
2. ✅ Write property-based tests for roundtrips
3. ✅ Set up `criterion` benchmarks
4. ✅ Create performance baseline
5. ✅ Document performance characteristics

**Deliverables:**
- Property-based test suite
- Performance benchmark suite
- Performance baseline documentation

**Success Criteria:**
- Property-based tests catch edge cases
- Benchmarks run in CI (nightly)
- Performance regressions detected

### Phase 5: Cluster & Replication (Week 9-10)

**Goal:** Test distributed features

**Tasks:**
1. ✅ Cluster integration tests
2. ✅ Replication tests
3. ✅ Failover scenarios
4. ✅ Network partition tests

**Deliverables:**
- Cluster test suite
- Replication test suite
- Failover test scenarios

**Success Criteria:**
- Cluster operations tested
- Replication correctness verified
- >80% overall code coverage achieved

---

## Coverage Targets

### Overall Coverage Goals

| Component | Current | Target | Priority |
|-----------|---------|--------|----------|
| Command Parsing | ~90% | 95% | High |
| Command Execution | ~10% | 85% | **Critical** |
| Database Core | ~20% | 80% | High |
| Persistence | ~15% | 75% | Medium |
| Replication | ~5% | 70% | Medium |
| Cluster | ~5% | 70% | Medium |
| Cache | ~30% | 80% | High |
| **Overall** | **~35%** | **>80%** | **Critical** |

### Coverage by Test Type

| Test Type | Coverage Contribution | Target | Location |
|-----------|----------------------|--------|----------|
| Unit Tests (Directory) | 25% | Maintain 25% | `tests/unit/` |
| Integration Tests | 50% | **Add 50%** | `tests/integration/` |
| Property Tests | 5% | Add 5% | `tests/property/` |
| In-Module Tests | 20% | **Add 20%** | `#[cfg(test)]` in source files |
| Performance Tests | N/A | Track regressions | `tests/performance/` |

**Note:** In-module tests (`#[cfg(test)]`) will focus on testing private/internal functions and data structures that cannot be easily tested from the `tests/` directory. This hybrid approach ensures comprehensive coverage of both public and private APIs.

### Critical Paths (Must Have 100% Coverage)

1. **Command Execution Flow**
   - Command routing
   - Lock acquisition
   - Execution
   - Response generation

2. **Transaction Flow**
   - MULTI/EXEC
   - WATCH/UNWATCH
   - Rollback on error

3. **Persistence**
   - AOF writing
   - Snapshot creation
   - Data loading

4. **Error Handling**
   - All error paths
   - Error propagation
   - Error responses

---

## Test Guidelines

### Test Placement

**Follow the Hybrid Approach:**
- See [Unit Test Placement Strategy](#unit-test-placement-strategy) for detailed guidelines
- Use `tests/` directory for public API and cross-module tests
- Use `#[cfg(test)]` modules for private/internal function tests
- When in doubt, start with `tests/` directory (follows existing pattern)

### Naming Conventions

**File Names:**
- Unit tests (directory): `unit_<feature>_test.rs` (existing pattern)
- Unit tests (in-module): `mod tests` (inside source file)
- Integration tests: `<feature>_test.rs` or `<feature>_commands_test.rs`
- Property tests: `<feature>_property_test.rs`
- Performance tests: `<feature>_bench.rs`

**Test Function Names:**
- Use descriptive names: `test_<what>_<condition>_<expected_result>`
- Examples:
  - `test_set_get_roundtrip`
  - `test_list_lpush_rpop_flow`
  - `test_transaction_exec_with_watch_failure`

### Test Organization

**Arrange-Act-Assert Pattern:**
```rust
#[tokio::test]
async fn test_example() {
    // Arrange: Set up test context and data
    let ctx = TestContext::new().await;
    let key = Bytes::from("test_key");
    
    // Act: Execute the operation
    let result = ctx.execute(Command::Set(...)).await;
    
    // Assert: Verify the result
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), RespValue::SimpleString("OK".into()));
}
```

### Test Isolation

- Each test should be independent
- Use `TestContext::new()` to create fresh database state
- Clean up after tests (automatic with `Drop` trait)
- Use unique keys per test to avoid conflicts

### Error Testing

- Test both success and failure paths
- Test error messages and error types
- Test edge cases (empty strings, max values, etc.)
- Test concurrent access scenarios

### Performance Considerations

- Keep unit tests fast (< 1ms)
- Integration tests can be slower (10-100ms)
- Use `#[tokio::test]` for async tests
- Use `#[test]` for sync tests
- Mark slow tests with `#[ignore]` and run separately

### Documentation

- Add doc comments to test helpers
- Document test scenarios
- Explain complex test setups
- Note any test-specific assumptions

---

## Tools & Dependencies

### Required Dependencies

Add to `Cargo.toml`:

```toml
[dev-dependencies]
# Property-based testing
proptest = "1.4"

# Benchmarking
criterion = { version = "0.5", features = ["async_tokio"] }

# Test utilities
tokio-test = "0.4"
```

### Testing Tools

**Coverage:**
- `cargo tarpaulin` (already configured in Makefile)
- Run: `make test-coverage-html`

**Linting:**
- `cargo clippy` (already configured)
- Run: `make clippy`

**Formatting:**
- `cargo fmt` (already configured)
- Run: `make fmt`

**Test Execution:**
- `cargo test` - Run all tests
- `cargo test --test <test_file>` - Run specific test file
- `cargo test -- --test-threads=1` - Run tests sequentially
- `cargo test -- --ignored` - Run ignored tests

### CI Integration

**GitHub Actions:**
```yaml
- name: Run tests
  run: cargo test --all-features

- name: Run integration tests
  run: cargo test --test integration

- name: Generate coverage
  run: make test-coverage
```

---

## Examples

### Example 1: Integration Test Helper

```rust
// tests/integration/test_helpers.rs
use spineldb::core::state::ServerState;
use spineldb::core::database::core::Db;
use spineldb::core::Command;
use spineldb::core::protocol::RespValue;
use spineldb::core::SpinelDBError;
use spineldb::config::Config;
use std::sync::Arc;

pub struct TestContext {
    pub state: Arc<ServerState>,
    pub db: Arc<Db>,
    pub db_index: usize,
}

impl TestContext {
    pub async fn new() -> Self {
        let config = Config::default();
        let (state, _) = ServerState::initialize(config, ...).unwrap();
        let db = state.get_db(0).unwrap();
        
        Self {
            state,
            db,
            db_index: 0,
        }
    }

    pub async fn execute(
        &self,
        command: Command,
    ) -> Result<RespValue, SpinelDBError> {
        // Create execution context
        let ctx = ExecutionContext::new(
            self.state.clone(),
            self.db.clone(),
            self.db_index,
        );
        
        // Execute command
        command.execute(&ctx).await
    }

    pub async fn execute_multiple(
        &self,
        commands: Vec<Command>,
    ) -> Vec<Result<RespValue, SpinelDBError>> {
        let mut results = Vec::new();
        for cmd in commands {
            results.push(self.execute(cmd).await);
        }
        results
    }
}
```

### Example 2: String Commands Integration Test

```rust
// tests/integration/string_commands_test.rs
use super::test_helpers::TestContext;
use spineldb::core::Command;
use spineldb::core::commands::string::get::Get;
use spineldb::core::commands::string::set::Set;
use spineldb::core::protocol::RespValue;
use bytes::Bytes;

#[tokio::test]
async fn test_set_get_basic() {
    let ctx = TestContext::new().await;
    
    // SET
    let set_cmd = Command::Set(Set::parse(&[
        RespFrame::BulkString(Bytes::from("key1")),
        RespFrame::BulkString(Bytes::from("value1")),
    ]).unwrap());
    
    let result = ctx.execute(set_cmd).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".into()));
    
    // GET
    let get_cmd = Command::Get(Get::parse(&[
        RespFrame::BulkString(Bytes::from("key1")),
    ]).unwrap());
    
    let result = ctx.execute(get_cmd).await.unwrap();
    assert_eq!(
        result,
        RespValue::BulkString(Bytes::from("value1"))
    );
}

#[tokio::test]
async fn test_set_get_nonexistent_key() {
    let ctx = TestContext::new().await;
    
    let get_cmd = Command::Get(Get::parse(&[
        RespFrame::BulkString(Bytes::from("nonexistent")),
    ]).unwrap());
    
    let result = ctx.execute(get_cmd).await.unwrap();
    assert_eq!(result, RespValue::Null);
}
```

### Example 3: Property-Based Test

```rust
// tests/property/roundtrip_test.rs
use proptest::prelude::*;
use super::super::integration::test_helpers::TestContext;

proptest! {
    #[test]
    fn test_set_get_roundtrip(
        key in "[a-zA-Z0-9_]{1,100}",
        value in "[a-zA-Z0-9_\\s]{1,1000}"
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let ctx = TestContext::new().await;
            
            // SET
            let set_cmd = Command::Set(Set::parse(&[
                RespFrame::BulkString(Bytes::from(key.clone())),
                RespFrame::BulkString(Bytes::from(value.clone())),
            ]).unwrap());
            
            ctx.execute(set_cmd).await.unwrap();
            
            // GET
            let get_cmd = Command::Get(Get::parse(&[
                RespFrame::BulkString(Bytes::from(key)),
            ]).unwrap());
            
            let result = ctx.execute(get_cmd).await.unwrap();
            assert_eq!(
                result,
                RespValue::BulkString(Bytes::from(value))
            );
        });
    }
}
```

### Example 4: In-Module Test (Private Function)

```rust
// src/core/database/core.rs
impl Db {
    /// Internal function to calculate shard index for a key.
    /// This is a private function that needs testing.
    fn calculate_shard_index(&self, key: &Bytes) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }
    
    // ... other public methods
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    
    #[test]
    fn test_calculate_shard_index_distribution() {
        let db = Db::new();
        let mut shard_counts = vec![0; NUM_SHARDS];
        
        // Test that keys are distributed across shards
        for i in 0..1000 {
            let key = Bytes::from(format!("key_{}", i));
            let shard = db.calculate_shard_index(&key);
            shard_counts[shard] += 1;
        }
        
        // Verify reasonable distribution
        let min = shard_counts.iter().min().unwrap();
        let max = shard_counts.iter().max().unwrap();
        assert!(*max - *min < 50, "Shard distribution should be relatively even");
    }
    
    #[test]
    fn test_calculate_shard_index_consistency() {
        let db = Db::new();
        let key = Bytes::from("test_key");
        
        // Same key should always map to same shard
        let shard1 = db.calculate_shard_index(&key);
        let shard2 = db.calculate_shard_index(&key);
        assert_eq!(shard1, shard2);
    }
    
    #[test]
    fn test_calculate_shard_index_bounds() {
        let db = Db::new();
        
        // Test that shard index is always within bounds
        for i in 0..100 {
            let key = Bytes::from(format!("key_{}", i));
            let shard = db.calculate_shard_index(&key);
            assert!(shard < NUM_SHARDS, "Shard index must be within bounds");
        }
    }
}
```

### Example 5: Transaction Test

```rust
// tests/integration/transaction_test.rs
#[tokio::test]
async fn test_multi_exec_success() {
    let ctx = TestContext::new().await;
    
    // Start transaction
    ctx.execute(Command::Multi).await.unwrap();
    
    // Queue commands
    ctx.execute(Command::Set(...)).await.unwrap();
    ctx.execute(Command::Set(...)).await.unwrap();
    
    // Execute transaction
    let result = ctx.execute(Command::Exec).await.unwrap();
    
    match result {
        RespValue::Array(results) => {
            assert_eq!(results.len(), 2);
            // Verify both commands succeeded
        }
        _ => panic!("Expected array of results"),
    }
}
```

---

## Success Metrics

### Coverage Metrics

- **Overall Code Coverage:** >80%
- **Command Execution Coverage:** >85%
- **Error Path Coverage:** >75%
- **Critical Path Coverage:** 100%

### Test Quality Metrics

- **Test Execution Time:** < 30 seconds for full suite
- **Test Reliability:** >99% pass rate
- **Test Maintainability:** Clear, documented, isolated

### Process Metrics

- **Test Coverage Trend:** Increasing over time
- **Bug Detection:** Tests catch bugs before production
- **Refactoring Confidence:** High confidence in refactoring

---

## Maintenance Plan

### Regular Tasks

1. **Weekly:**
   - Review test coverage reports
   - Add tests for new features
   - Fix flaky tests

2. **Monthly:**
   - Review and update test strategy
   - Analyze coverage gaps
   - Optimize slow tests

3. **Quarterly:**
   - Review overall test architecture
   - Update test guidelines
   - Performance benchmark review

### Test Review Checklist

- [ ] All new features have tests
- [ ] Integration tests cover happy paths
- [ ] Error cases are tested
- [ ] Edge cases are covered
- [ ] Tests are fast and reliable
- [ ] Tests are well-documented
- [ ] Coverage targets are met

---

## Conclusion

This testing plan provides a comprehensive strategy for achieving high test coverage in SpinelDB. By following the phased implementation approach, we can systematically build a robust test suite that ensures reliability and maintainability.

**Next Steps:**
1. Review and approve this plan
2. Set up Phase 1 infrastructure
3. Begin implementation of integration tests
4. Track progress against coverage targets

**Questions or Concerns:**
Please discuss any questions or concerns about this plan before beginning implementation.

---

**Document Version:** 1.0  
**Last Updated:** 2024  
**Owner:** SpinelDB Team

