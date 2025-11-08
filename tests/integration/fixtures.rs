// tests/integration/fixtures.rs

//! Common test fixtures and data generators
//!
//! Fixtures provide reusable test data for:
//! - Consistency: using the same data across different tests
//! - Maintainability: easy to change test data in one place
//! - Readability: clear names for test data
//!
//! **Note:** Some fixtures may not be used in all tests yet,
//! but they are available for use when needed.

use bytes::Bytes;

/// Common test keys - for tests that need multiple keys
///
/// **Usage:**
/// ```rust
/// ctx.set(TEST_KEY1, TEST_VALUE1).await.unwrap();
/// ctx.set(TEST_KEY2, TEST_VALUE2).await.unwrap();
/// ```
pub const TEST_KEY1: &str = "test_key_1";
pub const TEST_KEY2: &str = "test_key_2";
pub const TEST_KEY3: &str = "test_key_3";

/// Common test values - for tests that need multiple values
///
/// **Usage:**
/// ```rust
/// ctx.set("mykey", TEST_VALUE1).await.unwrap();
/// ```
pub const TEST_VALUE1: &str = "test_value_1";
pub const TEST_VALUE2: &str = "test_value_2";
pub const TEST_VALUE3: &str = "test_value_3";

/// Generates a unique test key with a prefix
///
/// **Usage:** For tests that need many unique keys
/// ```rust
/// for i in 0..10 {
///     let key = unique_key("test", i);
///     ctx.set(&key, "value").await.unwrap();
/// }
/// ```
pub fn unique_key(prefix: &str, id: usize) -> String {
    format!("{}_{}", prefix, id)
}

/// Generates test data of a specific size (binary data)
///
/// **Usage:** For tests that need data of a specific size
/// ```rust
/// let data = generate_test_data(1024); // 1KB of 'x' bytes
/// ctx.set("large_key", &String::from_utf8_lossy(&data)).await.unwrap();
/// ```
#[allow(dead_code)] // Available for tests that need binary data of specific size
pub fn generate_test_data(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

/// Common test patterns - various data patterns for testing
pub mod patterns {
    /// ASCII printable characters - for testing ASCII characters
    ///
    /// **Usage:**
    /// ```rust
    /// ctx.set("ascii_key", patterns::ASCII_CHARS).await.unwrap();
    /// ```
    #[allow(dead_code)] // Available for tests that need all ASCII printable characters
    pub const ASCII_CHARS: &str =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()";

    /// Unicode test string - for testing UTF-8/Unicode support
    ///
    /// **Usage:**
    /// ```rust
    /// ctx.set("unicode_key", patterns::UNICODE_STR).await.unwrap();
    /// ```
    pub const UNICODE_STR: &str = "Hello 世界 🌍 Привет";

    /// Empty string - for testing empty strings
    ///
    /// **Usage:**
    /// ```rust
    /// ctx.set("empty_key", patterns::EMPTY_STR).await.unwrap();
    /// ```
    pub const EMPTY_STR: &str = "";

    /// Large text (1KB) - for testing large data
    ///
    /// **Usage:**
    /// ```rust
    /// let large = patterns::large_text_1kb();
    /// ctx.set("large_key", &large).await.unwrap();
    /// ```
    pub fn large_text_1kb() -> String {
        "x".repeat(1024)
    }

    /// Large text (1MB) - for testing very large data
    ///
    /// **Note:** Use with caution as this will make tests slower
    ///
    /// **Usage:**
    /// ```rust
    /// let huge = patterns::large_text_1mb();
    /// ctx.set("huge_key", &huge).await.unwrap();
    /// ```
    #[allow(dead_code)] // Available for performance or stress tests in the future
    pub fn large_text_1mb() -> String {
        "x".repeat(1024 * 1024)
    }
}

/// Constants for numeric test values
pub mod constants {
    /// Near i64::MAX for testing large number operations
    pub const NEAR_I64_MAX: &str = "9223372036854775806";

    /// i64::MAX value as string
    #[allow(dead_code)] // Available for future tests
    pub const I64_MAX_STR: &str = "9223372036854775807";

    /// i64::MAX value
    pub const I64_MAX: i64 = i64::MAX;

    /// i64::MIN value as string
    #[allow(dead_code)] // Available for future tests
    pub const I64_MIN_STR: &str = "-9223372036854775808";

    /// i64::MIN value
    #[allow(dead_code)] // Available for future tests
    pub const I64_MIN: i64 = i64::MIN;

    /// Default TTL in seconds for tests
    pub const DEFAULT_TTL_SECONDS: u64 = 60;

    /// Default TTL in milliseconds for tests
    pub const DEFAULT_TTL_MILLIS: u64 = 60000;

    /// Character 'A' in ASCII (0x41 = 01000001) - used for bit operation tests
    pub const CHAR_A: &str = "A";

    /// Character 'A' byte value
    #[allow(dead_code)] // Available for future tests that need the byte value
    pub const CHAR_A_BYTE: u8 = 0x41;

    /// Expected bit count for character 'A' (01000001 has 2 bits set)
    pub const CHAR_A_BIT_COUNT: i64 = 2;
}
