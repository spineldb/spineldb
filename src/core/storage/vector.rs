// src/core/storage/vector.rs

use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Distance metric used for vector similarity search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DistanceMetric {
    #[default]
    L2 = 0,
    Cosine = 1,
    InnerProduct = 2,
}

impl DistanceMetric {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "L2" | "EUCLIDEAN" => Some(DistanceMetric::L2),
            "COSINE" => Some(DistanceMetric::Cosine),
            "IP" | "INNER" | "DOT" => Some(DistanceMetric::InnerProduct),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DistanceMetric::L2 => "L2",
            DistanceMetric::Cosine => "COSINE",
            DistanceMetric::InnerProduct => "IP",
        }
    }
}

// ─── Optimized distance computation ─────────────────────────────────────────

/// Compute dot product with loop unrolling (8 elements per iteration).
#[inline]
fn dot_product_unrolled(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let len = a.len();
    let chunks8 = len / 8;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut i = 0;
    for _ in 0..chunks8 {
        sum0 += a[i] * b[i];
        sum1 += a[i + 1] * b[i + 1];
        sum2 += a[i + 2] * b[i + 2];
        sum3 += a[i + 3] * b[i + 3];
        sum0 += a[i + 4] * b[i + 4];
        sum1 += a[i + 5] * b[i + 5];
        sum2 += a[i + 6] * b[i + 6];
        sum3 += a[i + 7] * b[i + 7];
        i += 8;
    }
    let mut result = (sum0 + sum1) + (sum2 + sum3);
    for j in i..len {
        result += a[j] * b[j];
    }
    result
}

/// Compute L2 squared distance with loop unrolling.
#[inline]
fn l2_squared_unrolled(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let len = a.len();
    let chunks8 = len / 8;
    let mut sum0 = 0.0f32;
    let mut sum1 = 0.0f32;
    let mut sum2 = 0.0f32;
    let mut sum3 = 0.0f32;
    let mut i = 0;
    for _ in 0..chunks8 {
        let d0 = a[i] - b[i];
        let d1 = a[i + 1] - b[i + 1];
        let d2 = a[i + 2] - b[i + 2];
        let d3 = a[i + 3] - b[i + 3];
        let d4 = a[i + 4] - b[i + 4];
        let d5 = a[i + 5] - b[i + 5];
        let d6 = a[i + 6] - b[i + 6];
        let d7 = a[i + 7] - b[i + 7];
        sum0 += d0 * d0;
        sum1 += d1 * d1;
        sum2 += d2 * d2;
        sum3 += d3 * d3;
        sum0 += d4 * d4;
        sum1 += d5 * d5;
        sum2 += d6 * d6;
        sum3 += d7 * d7;
        i += 8;
    }
    let mut result = (sum0 + sum1) + (sum2 + sum3);
    for j in i..len {
        let d = a[j] - b[j];
        result += d * d;
    }
    result
}

/// Compute L2 (Euclidean) distance between two vectors.
#[inline]
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    l2_squared_unrolled(a, b).sqrt()
}

/// Compute squared L2 norm of a vector with loop unrolling.
#[inline]
fn norm_squared_unrolled(v: &[f32]) -> f32 {
    dot_product_unrolled(v, v)
}

/// Normalize a vector to unit length (L2 norm = 1).
/// Returns None if the vector is zero or contains NaN/Inf.
/// Uses a single pass to compute norm and normalize.
pub fn normalize_vector(v: &[f32]) -> Option<Vec<f32>> {
    let mut norm: f32 = 0.0;
    for &x in v {
        norm += x * x;
    }
    norm = norm.sqrt();
    if !norm.is_finite() || norm < f32::EPSILON {
        return None;
    }
    let inv_norm = 1.0 / norm;
    Some(v.iter().map(|&x| x * inv_norm).collect())
}

/// Validate that a vector contains only finite values (no NaN, no Inf).
pub fn validate_vector(v: &[f32]) -> Result<(), String> {
    for (i, &val) in v.iter().enumerate() {
        if !val.is_finite() {
            return Err(format!(
                "vector element at index {} is not finite: {}",
                i, val
            ));
        }
    }
    Ok(())
}

// ─── INT8 Quantization ─────────────────────────────────────────────────────

/// Quantization method for vector compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QuantizationMethod {
    #[default]
    None = 0,
    Int8 = 1,
    ProductQuantize = 2,
}

impl QuantizationMethod {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "" | "NONE" => Some(QuantizationMethod::None),
            "INT8" => Some(QuantizationMethod::Int8),
            "PQ" | "PRODUCT" | "PRODUCTQUANTIZE" => Some(QuantizationMethod::ProductQuantize),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            QuantizationMethod::None => "NONE",
            QuantizationMethod::Int8 => "INT8",
            QuantizationMethod::ProductQuantize => "PQ",
        }
    }
}

/// INT8 quantization parameters for a vector.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantizationParams {
    pub scale: f32,
    pub zero_point: i8,
}

/// Quantize a float32 vector to INT8 with affine quantization.
/// Returns (quantized_bytes, scale, zero_point).
pub fn quantize_int8(v: &[f32]) -> (Vec<u8>, f32, i8) {
    if v.is_empty() {
        return (Vec::new(), 1.0, 0);
    }

    let min = v.iter().cloned().fold(f32::INFINITY, f32::min);
    let max = v.iter().cloned().fold(f32::NEG_INFINITY, f32::max);

    let range = if (max - min).abs() < f32::EPSILON {
        1.0
    } else {
        max - min
    };

    let scale = range / 255.0;
    let zero_point_f = (-min / scale).round();
    let zero_point = (zero_point_f as i32).clamp(-128, 127) as i8;

    let quantized: Vec<u8> = v
        .iter()
        .map(|&x| {
            let q = (x / scale + zero_point_f).round() as i32;
            q.clamp(0, 255) as u8
        })
        .collect();

    (quantized, scale, zero_point)
}

/// Dequantize INT8 vector back to float32.
pub fn dequantize_int8(quantized: &[u8], scale: f32, zero_point: i8) -> Vec<f32> {
    quantized
        .iter()
        .map(|&q| (q as f32 - zero_point as f32) * scale)
        .collect()
}

/// Compute L2 distance between INT8 quantized vectors.
pub fn l2_distance_int8(a: &[u8], b: &[u8], scale: f32) -> f32 {
    let sum_sq: f32 = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let diff = x as f32 - y as f32;
            diff * diff
        })
        .sum();
    sum_sq.sqrt() * scale
}

// ─── Product Quantization (PQ) ─────────────────────────────────────────────

/// PQ codebook for a single subspace.
#[derive(Debug, Clone)]
pub struct PQCodebook {
    pub centroids: Vec<Vec<f32>>,
}

/// Product Quantization index.
#[derive(Debug, Clone)]
pub struct ProductQuantizer {
    pub num_subspaces: usize,
    pub bits_per_code: usize,
    pub codebooks: Vec<PQCodebook>,
    pub trained: bool,
}

impl ProductQuantizer {
    /// Create a new PQ index (not yet trained).
    pub fn new(_dimension: usize, num_subspaces: usize, bits_per_code: usize) -> Self {
        let _cluster_count = 1 << bits_per_code;
        let codebooks = (0..num_subspaces)
            .map(|_| PQCodebook {
                centroids: Vec::new(),
            })
            .collect();
        Self {
            num_subspaces,
            bits_per_code,
            codebooks,
            trained: false,
        }
    }

    /// Train PQ codebooks using k-means on the training vectors.
    pub fn train(&mut self, vectors: &[Vec<f32>], max_iterations: usize) {
        if vectors.is_empty() {
            return;
        }

        let dimension = vectors[0].len();
        let subspace_dim = dimension / self.num_subspaces;
        let cluster_count = 1 << self.bits_per_code;

        for s in 0..self.num_subspaces {
            let start = s * subspace_dim;
            let end = start + subspace_dim.min(dimension - start);
            if start >= dimension {
                break;
            }

            let sub_vectors: Vec<Vec<f32>> =
                vectors.iter().map(|v| v[start..end].to_vec()).collect();

            let centroids = k_means(&sub_vectors, cluster_count, max_iterations);
            self.codebooks[s] = PQCodebook { centroids };
        }

        self.trained = true;
    }

    /// Encode a vector into PQ codes.
    pub fn encode(&self, vector: &[f32]) -> Vec<u8> {
        let subspace_dim = vector.len() / self.num_subspaces;
        let max_cluster = (1 << self.bits_per_code).min(256);
        let mut codes = Vec::with_capacity(self.num_subspaces);

        for s in 0..self.num_subspaces {
            let start = s * subspace_dim;
            let end = start + subspace_dim.min(vector.len() - start);
            if start >= vector.len() {
                break;
            }

            let sub = &vector[start..end];
            let mut best_cluster = 0usize;
            let mut best_dist = f32::MAX;

            for (c, centroid) in self.codebooks[s].centroids.iter().enumerate() {
                if c >= max_cluster {
                    break;
                }
                let dist: f32 = sub
                    .iter()
                    .zip(centroid.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum();
                if dist < best_dist {
                    best_dist = dist;
                    best_cluster = c;
                }
            }

            codes.push(best_cluster as u8);
        }

        codes
    }

    /// Decode PQ codes back to approximate vector.
    pub fn decode(&self, codes: &[u8], dimension: usize) -> Vec<f32> {
        let subspace_dim = dimension / self.num_subspaces;
        let mut reconstructed = vec![0.0; dimension];

        for (s, &code) in codes.iter().enumerate() {
            let start = s * subspace_dim;
            let _end = start + subspace_dim.min(dimension - start);
            if start >= dimension || code as usize >= self.codebooks[s].centroids.len() {
                break;
            }

            let centroid = &self.codebooks[s].centroids[code as usize];
            for (i, &val) in centroid.iter().enumerate() {
                if start + i < dimension {
                    reconstructed[start + i] = val;
                }
            }
        }

        reconstructed
    }

    /// Compute approximate L2 distance using PQ codes.
    pub fn distance_pq(&self, codes_a: &[u8], codes_b: &[u8]) -> f32 {
        let mut dist = 0.0f32;
        for (s, (&a, &b)) in codes_a.iter().zip(codes_b.iter()).enumerate() {
            if let (Some(centroid_a), Some(centroid_b)) = (
                self.codebooks
                    .get(s)
                    .and_then(|cb| cb.centroids.get(a as usize)),
                self.codebooks
                    .get(s)
                    .and_then(|cb| cb.centroids.get(b as usize)),
            ) {
                for (x, y) in centroid_a.iter().zip(centroid_b.iter()) {
                    let d = x - y;
                    dist += d * d;
                }
            }
        }
        dist.sqrt()
    }
}

/// Compute squared L2 distance between two vectors (used in k-means).
fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum()
}

/// K-means++ initialization: pick centroids spread far apart.
fn kmeans_pp_init(vectors: &[Vec<f32>], k: usize) -> Vec<Vec<f32>> {
    let n = vectors.len();
    let actual_k = k.min(n);
    let mut rng_seed: u64 = 0xdeadbeef;

    // Simple xorshift64 PRNG for deterministic but good centroid seeding.
    let next_rand = |state: &mut u64| -> usize {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        (*state % n as u64) as usize
    };

    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(actual_k);
    // Pick first centroid deterministically (index 0)
    centroids.push(vectors[0].clone());

    // Squared distances from each vector to nearest existing centroid
    let mut min_sq_dists: Vec<f32> = vectors
        .iter()
        .map(|v| squared_l2(v, &centroids[0]))
        .collect();

    for _ in 1..actual_k {
        // Compute total distance for weighted random selection
        let total: f32 = min_sq_dists.iter().sum();
        if total <= 0.0 {
            // All remaining vectors are identical to existing centroids;
            // pick evenly spaced indices as fallback
            let idx = next_rand(&mut rng_seed);
            centroids.push(vectors[idx].clone());
            continue;
        }

        // Weighted random selection proportional to squared distance
        let threshold = total * (next_rand(&mut rng_seed) as f32 / n as f32);
        let mut cumulative = 0.0f32;
        let mut selected = centroids.len().min(n - 1);
        for (i, &d) in min_sq_dists.iter().enumerate() {
            cumulative += d;
            if cumulative >= threshold {
                selected = i;
                break;
            }
        }

        centroids.push(vectors[selected].clone());

        // Update min distances for the new centroid
        for (i, v) in vectors.iter().enumerate() {
            let d = squared_l2(v, centroids.last().unwrap());
            if d < min_sq_dists[i] {
                min_sq_dists[i] = d;
            }
        }
    }

    centroids
}

/// K-means clustering for PQ training.
///
/// Uses k-means++ initialization and runs up to `max_iterations` EM steps.
fn k_means(vectors: &[Vec<f32>], k: usize, max_iterations: usize) -> Vec<Vec<f32>> {
    if vectors.is_empty() || k == 0 {
        return Vec::new();
    }

    let dim = vectors[0].len();
    let n = vectors.len();
    let actual_k = k.min(n);

    let mut centroids = kmeans_pp_init(vectors, actual_k);

    let mut assignments = vec![0usize; n];

    for _ in 0..max_iterations {
        let mut changed = false;

        // Assign each vector to nearest centroid
        for (i, v) in vectors.iter().enumerate() {
            let mut best = 0;
            let mut best_dist = f32::MAX;
            for (c, centroid) in centroids.iter().enumerate() {
                let dist = squared_l2(v, centroid);
                if dist < best_dist {
                    best_dist = dist;
                    best = c;
                }
            }
            if assignments[i] != best {
                assignments[i] = best;
                changed = true;
            }
        }

        if !changed {
            break;
        }

        // Recompute centroids (handle empty clusters by keeping old centroid)
        let mut counts = vec![0usize; actual_k];
        let mut sums = vec![vec![0.0f32; dim]; actual_k];
        for (i, v) in vectors.iter().enumerate() {
            let c = assignments[i];
            counts[c] += 1;
            for (j, &val) in v.iter().enumerate() {
                sums[c][j] += val;
            }
        }
        for c in 0..actual_k {
            if counts[c] > 0 {
                for j in 0..dim {
                    centroids[c][j] = sums[c][j] / counts[c] as f32;
                }
            }
            // If counts[c] == 0, keep old centroid (avoids dead clusters)
        }
    }

    centroids
}

// ─── BM25 for Hybrid Search ────────────────────────────────────────────────

/// BM25 index entry for a single document.
#[derive(Debug, Clone)]
pub struct BM25Entry {
    pub doc_id: usize,
    pub terms: Vec<String>,
    pub term_freq: std::collections::HashMap<String, u32>,
}

/// BM25 index for hybrid search.
#[derive(Debug, Clone)]
pub struct BM25Index {
    pub entries: Vec<BM25Entry>,
    pub avg_doc_len: f32,
    pub total_term_count: usize,
    pub doc_count: usize,
    pub idf: std::collections::HashMap<String, f32>,
    pub df: std::collections::HashMap<String, usize>,
    pub k1: f32,
    pub b: f32,
}

impl Default for BM25Index {
    fn default() -> Self {
        Self::new()
    }
}

impl BM25Index {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            avg_doc_len: 0.0,
            total_term_count: 0,
            doc_count: 0,
            idf: std::collections::HashMap::new(),
            df: std::collections::HashMap::new(),
            k1: 1.5,
            b: 0.75,
        }
    }

    /// Tokenize text into terms (simple whitespace + lowercase).
    pub fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    /// Add a document to the index.
    pub fn add_document(&mut self, doc_id: usize, text: &str) {
        let terms = Self::tokenize(text);
        let mut term_freq = std::collections::HashMap::new();
        for term in &terms {
            *term_freq.entry(term.clone()).or_insert(0) += 1;
        }

        // Incrementally update document frequency
        let mut seen = std::collections::HashSet::new();
        for term in &terms {
            if seen.insert(term.clone()) {
                *self.df.entry(term.clone()).or_insert(0) += 1;
            }
        }

        let doc_len = terms.len();
        self.entries.push(BM25Entry {
            doc_id,
            terms,
            term_freq,
        });
        self.doc_count += 1;
        self.total_term_count += doc_len;

        // O(1) incremental average update instead of O(N) full scan
        self.avg_doc_len = self.total_term_count as f32 / self.doc_count as f32;

        // Incrementally update IDF for affected terms
        let n = self.doc_count as f32;
        for term in &seen {
            if let Some(&df_val) = self.df.get(term) {
                let idf = ((n - df_val as f32 + 0.5) / (df_val as f32 + 0.5) + 1.0).ln();
                self.idf.insert(term.clone(), idf);
            }
        }
    }

    /// Score a query against all documents.
    pub fn score(&self, query_terms: &[String]) -> Vec<(usize, f32)> {
        let mut scores: Vec<(usize, f32)> = Vec::new();
        let avg_dl = if self.avg_doc_len > 0.0 {
            self.avg_doc_len
        } else {
            1.0
        };

        for entry in &self.entries {
            let mut score = 0.0f32;
            for term in query_terms {
                if let Some(&tf) = entry.term_freq.get(term) {
                    let idf = self.idf.get(term).copied().unwrap_or(0.0);
                    let tf_norm = (tf as f32 * (self.k1 + 1.0))
                        / (tf as f32
                            + self.k1
                                * (1.0 - self.b + self.b * entry.terms.len() as f32 / avg_dl));
                    score += idf * tf_norm;
                }
            }
            if score > 0.0 {
                scores.push((entry.doc_id, score));
            }
        }

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        scores
    }
}

/// Reciprocal Rank Fusion: combine ranked lists from vector search and BM25.
pub fn reciprocal_rank_fusion(
    vector_results: &[(usize, f32)],
    bm25_results: &[(usize, f32)],
    k: usize,
    vector_weight: f32,
    bm25_weight: f32,
) -> Vec<(usize, f32)> {
    let rrf_k = 60.0; // Standard RRF constant
    let mut scores: std::collections::HashMap<usize, f32> = std::collections::HashMap::new();

    // Score from vector results (lower distance = higher rank)
    for (rank, &(doc_id, _)) in vector_results.iter().enumerate() {
        let rrf_score = vector_weight / (rrf_k + rank as f32 + 1.0);
        *scores.entry(doc_id).or_insert(0.0) += rrf_score;
    }

    // Score from BM25 results (higher score = higher rank)
    for (rank, &(doc_id, _)) in bm25_results.iter().enumerate() {
        let rrf_score = bm25_weight / (rrf_k + rank as f32 + 1.0);
        *scores.entry(doc_id).or_insert(0.0) += rrf_score;
    }

    let mut results: Vec<(usize, f32)> = scores.into_iter().collect();
    results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    results.into_iter().take(k).collect()
}

/// Get current Unix timestamp in seconds. Falls back to 0 if system clock is before UNIX_EPOCH.
fn current_timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Compute cosine distance between two vectors. Returns a value in [0, 2].
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot = dot_product_unrolled(a, b);
    let norm_a = norm_squared_unrolled(a).sqrt();
    let norm_b = norm_squared_unrolled(b).sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    let similarity = dot / (norm_a * norm_b);
    let similarity = similarity.clamp(-1.0, 1.0);
    1.0 - similarity
}

/// Compute negated inner product (for min-heap based search, lower = more similar).
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f32 {
    -dot_product_unrolled(a, b)
}

/// Compute distance between two vectors using the given metric.
pub fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_distance(a, b),
        DistanceMetric::InnerProduct => inner_product_distance(a, b),
    }
}

/// Metadata filter for search results.
#[derive(Debug, Clone)]
pub enum MetadataFilter {
    /// Simple key=value match (metadata must be UTF-8 JSON containing the key)
    Equals(String, String),
    /// Key != value
    NotEquals(String, String),
    /// Key contains substring
    Contains(String, String),
    /// Key > value (numeric comparison for numbers, lexicographic for strings)
    GreaterThan(String, String),
    /// Key >= value (inclusive)
    GreaterThanEquals(String, String),
    /// Key < value (numeric comparison for numbers, lexicographic for strings)
    LessThan(String, String),
    /// Key <= value (inclusive)
    LessThanEquals(String, String),
    /// Logical AND of multiple filters
    And(Vec<MetadataFilter>),
    /// Logical OR of multiple filters
    Or(Vec<MetadataFilter>),
    /// Negate a filter
    Not(Box<MetadataFilter>),
}

/// Try to parse a string as f64 for numeric comparison.
fn try_parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

impl MetadataFilter {
    /// Check if a metadata entry matches this filter.
    pub fn matches(&self, metadata: Option<&Bytes>) -> bool {
        match self {
            MetadataFilter::Equals(key, value) => {
                if let Some(meta) = metadata {
                    if let Ok(obj) = serde_json::from_slice::<serde_json::Value>(meta) {
                        return match obj.get(key.as_str()) {
                            Some(v) => match v {
                                serde_json::Value::String(s) => s == value,
                                serde_json::Value::Number(n) => n.to_string() == *value,
                                serde_json::Value::Bool(b) => {
                                    b.to_string().to_lowercase() == value.to_lowercase()
                                }
                                serde_json::Value::Null => value == "null",
                                _ => v.to_string().as_str() == value.as_str(),
                            },
                            None => false,
                        };
                    }
                    // JSON parse failed: no match possible
                    false
                } else {
                    false
                }
            }
            MetadataFilter::NotEquals(key, value) => {
                !MetadataFilter::Equals(key.clone(), value.clone()).matches(metadata)
            }
            MetadataFilter::Contains(key, substring) => {
                if let Some(meta) = metadata {
                    if let Ok(obj) = serde_json::from_slice::<serde_json::Value>(meta)
                        && let Some(v) = obj.get(key)
                    {
                        let v_str = v.to_string();
                        return v_str.contains(substring.as_str());
                    }
                    false
                } else {
                    false
                }
            }
            MetadataFilter::GreaterThan(key, value) => {
                if let Some(meta) = metadata {
                    if let Ok(obj) = serde_json::from_slice::<serde_json::Value>(meta)
                        && let Some(v) = obj.get(key)
                    {
                        let v_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        };
                        // Try numeric comparison first
                        if let (Some(vn), Some(vn2)) = (try_parse_f64(&v_str), try_parse_f64(value))
                        {
                            return vn > vn2;
                        }
                        return v_str > *value;
                    }
                    false
                } else {
                    false
                }
            }
            MetadataFilter::GreaterThanEquals(key, value) => {
                if let Some(meta) = metadata {
                    if let Ok(obj) = serde_json::from_slice::<serde_json::Value>(meta)
                        && let Some(v) = obj.get(key)
                    {
                        let v_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        };
                        if let (Some(vn), Some(vn2)) = (try_parse_f64(&v_str), try_parse_f64(value))
                        {
                            return vn >= vn2;
                        }
                        return v_str >= *value;
                    }
                    false
                } else {
                    false
                }
            }
            MetadataFilter::LessThan(key, value) => {
                if let Some(meta) = metadata {
                    if let Ok(obj) = serde_json::from_slice::<serde_json::Value>(meta)
                        && let Some(v) = obj.get(key)
                    {
                        let v_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        };
                        if let (Some(vn), Some(vn2)) = (try_parse_f64(&v_str), try_parse_f64(value))
                        {
                            return vn < vn2;
                        }
                        return v_str < *value;
                    }
                    false
                } else {
                    false
                }
            }
            MetadataFilter::LessThanEquals(key, value) => {
                if let Some(meta) = metadata {
                    if let Ok(obj) = serde_json::from_slice::<serde_json::Value>(meta)
                        && let Some(v) = obj.get(key)
                    {
                        let v_str = match v {
                            serde_json::Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        };
                        if let (Some(vn), Some(vn2)) = (try_parse_f64(&v_str), try_parse_f64(value))
                        {
                            return vn <= vn2;
                        }
                        return v_str <= *value;
                    }
                    false
                } else {
                    false
                }
            }
            MetadataFilter::And(filters) => filters.iter().all(|f| f.matches(metadata)),
            MetadataFilter::Or(filters) => filters.iter().any(|f| f.matches(metadata)),
            MetadataFilter::Not(inner) => !inner.matches(metadata),
        }
    }

    /// Parse a simple filter expression like "key=value", "key!=value", etc.
    pub fn parse_expr(expr: &str) -> Result<Self, String> {
        // Check for AND/OR
        if let Some(pos) = find_top_level_operator(expr, " AND ") {
            let left = Self::parse_expr(&expr[..pos])?;
            let right = Self::parse_expr(&expr[pos + 5..])?;
            return Ok(MetadataFilter::And(vec![left, right]));
        }
        if let Some(pos) = find_top_level_operator(expr, " OR ") {
            let left = Self::parse_expr(&expr[..pos])?;
            let right = Self::parse_expr(&expr[pos + 4..])?;
            return Ok(MetadataFilter::Or(vec![left, right]));
        }

        // Check for NOT
        if let Some(rest) = expr.strip_prefix("NOT ") {
            let inner = Self::parse_expr(rest)?;
            return Ok(MetadataFilter::Not(Box::new(inner)));
        }

        // Parse simple operators (order matters: check >=, <=, != before >, <, =)
        if let Some(pos) = expr.find("!=") {
            let key = expr[..pos].trim().to_string();
            let val = expr[pos + 2..].trim().to_string();
            return Ok(MetadataFilter::NotEquals(key, val));
        }
        if let Some(pos) = expr.find(">=") {
            let key = expr[..pos].trim().to_string();
            let val = expr[pos + 2..].trim().to_string();
            return Ok(MetadataFilter::GreaterThanEquals(key, val));
        }
        if let Some(pos) = expr.find("<=") {
            let key = expr[..pos].trim().to_string();
            let val = expr[pos + 2..].trim().to_string();
            return Ok(MetadataFilter::LessThanEquals(key, val));
        }
        if let Some(pos) = expr.find('>') {
            let key = expr[..pos].trim().to_string();
            let val = expr[pos + 1..].trim().to_string();
            return Ok(MetadataFilter::GreaterThan(key, val));
        }
        if let Some(pos) = expr.find('<') {
            let key = expr[..pos].trim().to_string();
            let val = expr[pos + 1..].trim().to_string();
            return Ok(MetadataFilter::LessThan(key, val));
        }
        if let Some(pos) = expr.find('=') {
            let key = expr[..pos].trim().to_string();
            let val = expr[pos + 1..].trim().to_string();
            // Remove surrounding quotes
            let val = val.trim_matches('\'').trim_matches('"').to_string();
            return Ok(MetadataFilter::Equals(key, val));
        }

        Err(format!("Invalid filter expression: {}", expr))
    }
}

/// Find top-level operator position (not inside quotes/parens).
fn find_top_level_operator(expr: &str, op: &str) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_quote = false;
    let mut quote_char = b'\0';
    let bytes = expr.as_bytes();
    let op_bytes = op.as_bytes();

    for i in 0..bytes.len() {
        if in_quote {
            if bytes[i] == quote_char {
                in_quote = false;
            }
            continue;
        }
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'\'' | b'"' => {
                in_quote = true;
                quote_char = bytes[i];
            }
            _ => {
                if depth == 0
                    && i + op_bytes.len() <= bytes.len()
                    && &bytes[i..i + op_bytes.len()] == op_bytes
                {
                    return Some(i);
                }
            }
        }
    }
    None
}

/// A stored vector entry with ID, vector data, and optional metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorEntry {
    pub id: Bytes,
    pub vector: Vec<f32>,
    pub metadata: Option<Bytes>,
    /// Tombstone flag: true if this entry has been logically deleted.
    pub deleted: bool,
}

/// Statistics about the HNSW index.
#[derive(Debug, Clone)]
pub struct HnswStats {
    pub max_level: usize,
    pub node_count: usize,
    pub deleted_count: usize,
    pub entry_point: Option<usize>,
    pub total_inserts: u64,
    pub total_searches: u64,
    pub total_deletes: u64,
}

/// A min-heap entry for distance-based search.
#[derive(Debug, Clone)]
struct HeapEntry {
    distance: f32,
    id: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap using BinaryHeap (which is max-heap)
        // Treat NaN as greater than any finite value for consistent ordering
        match self.distance.partial_cmp(&other.distance) {
            Some(ord) => ord.reverse(),
            None => {
                // NaN handling: NaN is considered greater than any finite value
                if self.distance.is_nan() {
                    if other.distance.is_nan() {
                        Ordering::Equal
                    } else {
                        Ordering::Less // NaN < finite in reverse (so NaN pops first from max-heap)
                    }
                } else {
                    Ordering::Greater
                }
            }
        }
    }
}

/// Bundles immutable parameters for HNSW level search.
struct HnswSearchCtx<'a> {
    query: &'a [f32],
    vectors: &'a [Vec<f32>],
    metric: DistanceMetric,
}

/// HNSW index for approximate nearest neighbor search.
#[derive(Debug)]
pub struct HnswIndex {
    max_level: usize,
    ef_construction: usize,
    ef_search: usize,
    m: usize,
    m_max0: usize,
    levels: Vec<Vec<usize>>,
    entry_point: Option<usize>,
    node_levels: Vec<usize>,
    node_connections: Vec<Vec<Vec<usize>>>,
    deleted: std::collections::HashSet<usize>,
    total_inserts: u64,
    total_searches: AtomicU64,
    total_deletes: u64,
}

impl PartialEq for HnswIndex {
    fn eq(&self, other: &Self) -> bool {
        self.max_level == other.max_level
            && self.ef_construction == other.ef_construction
            && self.ef_search == other.ef_search
            && self.m == other.m
            && self.m_max0 == other.m_max0
            && self.levels == other.levels
            && self.entry_point == other.entry_point
            && self.node_levels == other.node_levels
            && self.node_connections == other.node_connections
            && self.total_searches.load(AtomicOrdering::Relaxed)
                == other.total_searches.load(AtomicOrdering::Relaxed)
    }
}

impl Clone for HnswIndex {
    fn clone(&self) -> Self {
        Self {
            max_level: self.max_level,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            m: self.m,
            m_max0: self.m_max0,
            levels: self.levels.clone(),
            entry_point: self.entry_point,
            node_levels: self.node_levels.clone(),
            node_connections: self.node_connections.clone(),
            deleted: self.deleted.clone(),
            total_inserts: self.total_inserts,
            total_searches: AtomicU64::new(self.total_searches.load(AtomicOrdering::Relaxed)),
            total_deletes: self.total_deletes,
        }
    }
}

impl HnswIndex {
    fn new(m: usize, ef_construction: usize, ef_search: usize) -> Self {
        let m_max0 = m * 2;
        Self {
            max_level: 0,
            ef_construction,
            ef_search,
            m,
            m_max0,
            levels: vec![Vec::new()],
            entry_point: None,
            node_levels: Vec::new(),
            node_connections: Vec::new(),
            deleted: std::collections::HashSet::new(),
            total_inserts: 0,
            total_searches: AtomicU64::new(0),
            total_deletes: 0,
        }
    }

    fn random_level(&mut self) -> usize {
        // HNSW paper: level = floor(-ln(rand) * (1/ln(m)))
        // Using m_max0 for level 0 multiplier
        let m_factor = 1.0 / (self.m as f32).ln();
        let r: f32 = rand::random::<f32>().max(f32::EPSILON);
        let level = (-r.ln() * m_factor).floor() as usize;
        level.min(self.max_level + 1)
    }

    fn get_max_connections(&self, level: usize) -> usize {
        if level == 0 { self.m_max0 } else { self.m }
    }

    fn search_level(
        &self,
        ctx: &HnswSearchCtx,
        entry_id: usize,
        ef: usize,
        level: usize,
    ) -> Vec<usize> {
        self.search_level_with_filter(ctx, entry_id, ef, level, None)
    }

    /// HNSW level search with optional pre-filter.
    /// When filter is provided, only nodes matching the filter are explored.
    fn search_level_with_filter(
        &self,
        ctx: &HnswSearchCtx,
        entry_id: usize,
        ef: usize,
        level: usize,
        filter: Option<&dyn Fn(usize) -> bool>,
    ) -> Vec<usize> {
        let mut visited = std::collections::HashSet::new();
        let mut candidates: BinaryHeap<HeapEntry> = BinaryHeap::new();
        let mut results: BinaryHeap<HeapEntry> = BinaryHeap::new();

        // Skip deleted entry point
        if self.deleted.contains(&entry_id) {
            return Vec::new();
        }

        // Check filter on entry point
        if let Some(f) = filter
            && !f(entry_id)
        {
            return Vec::new();
        }

        let dist = compute_distance(ctx.query, &ctx.vectors[entry_id], ctx.metric);
        candidates.push(HeapEntry {
            distance: dist,
            id: entry_id,
        });
        results.push(HeapEntry {
            distance: dist,
            id: entry_id,
        });
        visited.insert(entry_id);

        while let Some(current) = candidates.pop() {
            let worst_result = results.peek().map(|e| e.distance).unwrap_or(f32::MAX);
            if current.distance > worst_result && results.len() >= ef {
                break;
            }

            let node_id = current.id;
            if node_id < self.node_connections.len() && level < self.node_connections[node_id].len()
            {
                for &neighbor_id in &self.node_connections[node_id][level] {
                    if visited.contains(&neighbor_id) || self.deleted.contains(&neighbor_id) {
                        continue;
                    }

                    // Apply pre-filter: skip nodes that don't match
                    if let Some(f) = filter
                        && !f(neighbor_id)
                    {
                        visited.insert(neighbor_id);
                        continue;
                    }

                    visited.insert(neighbor_id);

                    let dist = compute_distance(ctx.query, &ctx.vectors[neighbor_id], ctx.metric);
                    let worst = results.peek().map(|e| e.distance).unwrap_or(f32::MAX);

                    if dist < worst || results.len() < ef {
                        candidates.push(HeapEntry {
                            distance: dist,
                            id: neighbor_id,
                        });
                        results.push(HeapEntry {
                            distance: dist,
                            id: neighbor_id,
                        });

                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }

        results
            .into_sorted_vec()
            .into_iter()
            .map(|e| e.id)
            .collect()
    }

    fn select_neighbors(
        &self,
        candidates: &[usize],
        vectors: &[Vec<f32>],
        query: &[f32],
        m: usize,
        metric: DistanceMetric,
    ) -> Vec<usize> {
        let mut scored: Vec<(f32, usize)> = candidates
            .iter()
            .map(|&id| (compute_distance(query, &vectors[id], metric), id))
            .collect();
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
        scored.into_iter().take(m).map(|(_, id)| id).collect()
    }

    pub fn insert(&mut self, node_id: usize, vectors: &[Vec<f32>], metric: DistanceMetric) {
        self.total_inserts += 1;
        let level = self.random_level();

        // Extend levels if needed
        while self.levels.len() <= level {
            self.levels.push(Vec::new());
        }

        self.node_levels.push(level);
        self.node_connections.push(vec![Vec::new(); level + 1]);

        if self.entry_point.is_none() {
            self.entry_point = Some(node_id);
            self.max_level = level;
            self.levels[level].push(node_id);
            return;
        }

        let entry = self.entry_point.unwrap();
        let mut current_closest = entry;

        let ctx = HnswSearchCtx {
            query: &vectors[node_id],
            vectors,
            metric,
        };

        // Search from top level down to level+1
        for l in ((level + 1)..=self.max_level).rev() {
            let results = self.search_level(&ctx, current_closest, 1, l);
            if let Some(&best) = results.first() {
                current_closest = best;
            }
        }

        // Search at each level from min(level, max_level) down to 0
        for l in (0..=level.min(self.max_level)).rev() {
            let ef = if l == 0 {
                self.ef_construction * 2
            } else {
                self.ef_construction
            };
            let results = self.search_level(&ctx, current_closest, ef, l);

            let max_conn = self.get_max_connections(l);
            let neighbors =
                self.select_neighbors(&results, vectors, &vectors[node_id], max_conn, metric);

            // Add bidirectional connections
            for &neighbor in &neighbors {
                self.node_connections[node_id][l].push(neighbor);
                self.node_connections[neighbor][l].push(node_id);

                // Prune if too many connections
                let max_conn = self.get_max_connections(l);
                if self.node_connections[neighbor][l].len() > max_conn {
                    let pruned = self.select_neighbors(
                        &self.node_connections[neighbor][l],
                        vectors,
                        &vectors[neighbor],
                        max_conn,
                        metric,
                    );
                    self.node_connections[neighbor][l] = pruned;
                }
            }

            current_closest = results.first().copied().unwrap_or(current_closest);
        }

        self.levels[level].push(node_id);

        if level > self.max_level {
            self.max_level = level;
            self.entry_point = Some(node_id);
        }
    }

    pub fn search(
        &self,
        query: &[f32],
        vectors: &[Vec<f32>],
        k: usize,
        metric: DistanceMetric,
        ef: usize,
    ) -> Vec<(usize, f32)> {
        self.search_with_filter(query, vectors, k, metric, ef, None)
    }

    /// HNSW search with optional pre-filter applied during graph traversal.
    pub fn search_with_filter(
        &self,
        query: &[f32],
        vectors: &[Vec<f32>],
        k: usize,
        metric: DistanceMetric,
        ef: usize,
        filter: Option<&dyn Fn(usize) -> bool>,
    ) -> Vec<(usize, f32)> {
        if vectors.is_empty() {
            return Vec::new();
        }

        // Brute-force for small datasets
        if vectors.len() <= self.m * 2 {
            let mut scored: Vec<(usize, f32)> = vectors
                .iter()
                .enumerate()
                .filter(|(i, _)| !self.deleted.contains(i))
                .filter(|(i, _)| filter.is_none_or(|f| f(*i)))
                .map(|(i, v)| (i, compute_distance(query, v, metric)))
                .collect();
            scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
            return scored.into_iter().take(k).collect();
        }

        if self.entry_point.is_none() {
            return Vec::new();
        }

        let ctx = HnswSearchCtx {
            query,
            vectors,
            metric,
        };

        let entry = self.entry_point.unwrap();
        let mut current_closest = entry;

        // If filter provided, find a valid entry point
        if let Some(f) = filter
            && !f(entry)
        {
            // Search for a valid entry point among nodes at level 0
            if let Some(&first_valid) = self.levels[0].iter().find(|&&id| f(id)) {
                current_closest = first_valid;
            } else {
                return Vec::new(); // No valid nodes
            }
        }

        // Search from top level down to level 1
        for l in (1..=self.max_level).rev() {
            let results = self.search_level_with_filter(&ctx, current_closest, 1, l, filter);
            if let Some(&best) = results.first() {
                current_closest = best;
            }
        }

        // Search at level 0 with full ef
        let results = self.search_level_with_filter(&ctx, current_closest, ef.max(k), 0, filter);

        results
            .into_iter()
            .take(k)
            .map(|id| {
                let dist = compute_distance(query, &vectors[id], metric);
                (id, dist)
            })
            .collect()
    }

    /// Search and return the number of searches performed (for stats).
    pub fn search_with_stats(
        &self,
        query: &[f32],
        vectors: &[Vec<f32>],
        k: usize,
        metric: DistanceMetric,
        ef: usize,
    ) -> (Vec<(usize, f32)>, u64) {
        let results = self.search(query, vectors, k, metric, ef);
        let count = self.total_searches.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        (results, count)
    }

    pub fn remove_node(&mut self, node_id: usize) {
        if node_id >= self.node_levels.len() {
            return;
        }

        // Mark as deleted (lazy deletion)
        self.deleted.insert(node_id);
        self.total_deletes += 1;

        let level = self.node_levels[node_id];

        // Remove from all neighbors' connections
        for l in 0..=level {
            if l < self.node_connections[node_id].len() {
                let neighbors: Vec<usize> = self.node_connections[node_id][l].clone();
                for &neighbor in &neighbors {
                    if neighbor < self.node_connections.len()
                        && l < self.node_connections[neighbor].len()
                    {
                        self.node_connections[neighbor][l].retain(|&x| x != node_id);
                    }
                }
            }
        }

        // Remove from levels
        for l in 0..=level {
            if l < self.levels.len() {
                self.levels[l].retain(|&x| x != node_id);
            }
        }

        // Clear connections for the deleted node
        self.node_connections[node_id] = Vec::new();

        // Update entry point if needed
        if self.entry_point == Some(node_id) {
            self.entry_point = None;
            // Find a new entry point from the highest non-empty level
            for l in (0..=self.max_level).rev() {
                if let Some(&first) = self.levels[l].first() {
                    self.entry_point = Some(first);
                    break;
                }
            }
            if self.entry_point.is_none() {
                self.max_level = 0;
            }
        }
    }

    /// Incrementally update a node: remove old connections and re-insert with new vector.
    /// This avoids a full graph rebuild.
    pub fn update_node(&mut self, node_id: usize, vectors: &[Vec<f32>], metric: DistanceMetric) {
        if node_id >= self.node_levels.len() {
            return;
        }

        // Remove old connections from neighbors and levels
        self.remove_node(node_id);

        // Re-insert the node with updated vector
        self.insert(node_id, vectors, metric);
    }

    /// Returns the number of lazily deleted nodes.
    pub fn deleted_count(&self) -> usize {
        self.deleted.len()
    }

    /// Returns the total number of insert operations.
    pub fn total_inserts(&self) -> u64 {
        self.total_inserts
    }

    /// Returns the total number of search operations.
    pub fn total_searches(&self) -> u64 {
        self.total_searches.load(AtomicOrdering::Relaxed)
    }

    /// Returns the total number of delete operations.
    pub fn total_deletes(&self) -> u64 {
        self.total_deletes
    }

    /// Rebuild the HNSW index from scratch.
    pub fn rebuild(&mut self, vectors: &[Vec<f32>], metric: DistanceMetric) {
        let saved_searches = self.total_searches.load(AtomicOrdering::Relaxed);
        let saved_deletes = self.total_deletes;
        *self = HnswIndex::new(self.m, self.ef_construction, self.ef_search);
        self.total_searches = AtomicU64::new(saved_searches);
        self.total_deletes = saved_deletes;
        for i in 0..vectors.len() {
            self.insert(i, vectors, metric);
        }
    }
}

/// The main SpinelVector struct storing vectors and their HNSW index.
#[derive(Debug, Clone)]
pub struct SpinelVector {
    pub dimension: u32,
    pub metric: DistanceMetric,
    pub max_capacity: u64,
    pub vectors_added: u64,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    entries: Vec<VectorEntry>,
    vectors_raw: Vec<Vec<f32>>,
    id_to_index: HashMap<Bytes, usize>,
    hnsw: HnswIndex,
    vectors_deleted: u64,
    normalize_vectors: bool,
    // Quantization
    quantization: QuantizationMethod,
    quantized_vectors: Vec<Vec<u8>>,
    quantization_params: Vec<QuantizationParams>,
    pq: Option<ProductQuantizer>,
    pq_codes: Vec<Vec<u8>>,
    // TTL
    ttl_seconds: Option<u64>,
    created_at: u64,
    last_expiry_check: u64,
    // BM25 for hybrid search
    bm25: BM25Index,
    hybrid_weight_vector: f32,
    hybrid_weight_bm25: f32,
    // Metadata inverted index: field -> value -> set of indices
    metadata_index: HashMap<String, HashMap<String, Vec<usize>>>,
}

impl PartialEq for SpinelVector {
    fn eq(&self, other: &Self) -> bool {
        self.dimension == other.dimension
            && self.metric == other.metric
            && self.max_capacity == other.max_capacity
            && self.vectors_added == other.vectors_added
            && self.m == other.m
            && self.ef_construction == other.ef_construction
            && self.ef_search == other.ef_search
            && self.entries == other.entries
            && self.hnsw == other.hnsw
            && self.vectors_deleted == other.vectors_deleted
    }
}

impl SpinelVector {
    const MAGIC: &'static [u8] = b"SPINELVEC";
    const VERSION: u8 = 3;

    /// Maximum supported vector dimension.
    pub const MAX_DIMENSION: u32 = 65536;

    /// Create a new SpinelVector index.
    pub fn new(
        dimension: u32,
        metric: DistanceMetric,
        max_capacity: u64,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Self {
        // Auto-normalize for Cosine metric
        let normalize_vectors = metric == DistanceMetric::Cosine;
        let now = current_timestamp_secs();
        Self {
            dimension,
            metric,
            max_capacity,
            vectors_added: 0,
            m,
            ef_construction,
            ef_search,
            entries: Vec::new(),
            vectors_raw: Vec::new(),
            id_to_index: HashMap::new(),
            hnsw: HnswIndex::new(m, ef_construction, ef_search),
            vectors_deleted: 0,
            normalize_vectors,
            quantization: QuantizationMethod::None,
            quantized_vectors: Vec::new(),
            quantization_params: Vec::new(),
            pq: None,
            pq_codes: Vec::new(),
            ttl_seconds: None,
            created_at: now,
            last_expiry_check: now,
            bm25: BM25Index::new(),
            hybrid_weight_vector: 0.5,
            hybrid_weight_bm25: 0.5,
            metadata_index: HashMap::new(),
        }
    }

    /// Get the number of lazily deleted vectors.
    pub fn deleted_count(&self) -> usize {
        self.hnsw.deleted_count()
    }

    /// Get memory usage in bytes.
    pub fn memory_usage_bytes(&self) -> usize {
        self.memory_usage()
    }

    /// Get HNSW graph statistics.
    pub fn hnsw_stats(&self) -> HnswStats {
        HnswStats {
            max_level: self.hnsw.max_level,
            node_count: self.hnsw.node_levels.len(),
            deleted_count: self.hnsw.deleted.len(),
            entry_point: self.hnsw.entry_point,
            total_inserts: self.hnsw.total_inserts,
            total_searches: self.hnsw.total_searches.load(AtomicOrdering::Relaxed),
            total_deletes: self.hnsw.total_deletes,
        }
    }

    /// Rebuild the HNSW index from scratch.
    pub fn rebuild_index(&mut self) {
        self.hnsw.rebuild(&self.vectors_raw, self.metric);
    }

    /// Optimize the index by removing deleted nodes and compacting parallel arrays.
    /// This is an O(N) operation that rebuilds all data structures.
    pub fn optimize(&mut self) -> usize {
        let deleted_count = self.hnsw.deleted_count();
        if deleted_count == 0 {
            return 0;
        }

        // Compact parallel arrays: remove tombstoned entries
        let mut new_entries = Vec::with_capacity(self.entries.len() - deleted_count);
        let mut new_vectors_raw = Vec::with_capacity(self.entries.len() - deleted_count);
        let mut new_quantized = Vec::new();
        let mut new_quant_params = Vec::new();
        let mut new_pq_codes = Vec::new();
        let mut new_id_to_index = HashMap::with_capacity(self.entries.len() - deleted_count);

        if self.quantization != QuantizationMethod::None {
            new_quantized = Vec::with_capacity(self.entries.len() - deleted_count);
            new_quant_params = Vec::with_capacity(self.entries.len() - deleted_count);
        }
        if self.pq.is_some() {
            new_pq_codes = Vec::with_capacity(self.entries.len() - deleted_count);
        }

        for (old_idx, entry) in self.entries.iter().enumerate() {
            if entry.deleted {
                continue;
            }
            let new_idx = new_entries.len();
            new_id_to_index.insert(entry.id.clone(), new_idx);
            new_vectors_raw.push(self.vectors_raw[old_idx].clone());
            new_entries.push(VectorEntry {
                id: entry.id.clone(),
                vector: entry.vector.clone(),
                metadata: entry.metadata.clone(),
                deleted: false,
            });
            if old_idx < self.quantized_vectors.len() {
                new_quantized.push(self.quantized_vectors[old_idx].clone());
            }
            if old_idx < self.quantization_params.len() {
                new_quant_params.push(self.quantization_params[old_idx].clone());
            }
            if old_idx < self.pq_codes.len() {
                new_pq_codes.push(self.pq_codes[old_idx].clone());
            }
        }

        self.entries = new_entries;
        self.vectors_raw = new_vectors_raw;
        self.quantized_vectors = new_quantized;
        self.quantization_params = new_quant_params;
        self.pq_codes = new_pq_codes;
        self.id_to_index = new_id_to_index;

        // Rebuild metadata inverted index with compacted indices
        self.metadata_index.clear();
        let entries_meta: Vec<Option<Bytes>> =
            self.entries.iter().map(|e| e.metadata.clone()).collect();
        for (i, meta) in entries_meta.into_iter().enumerate() {
            if let Some(ref m) = meta {
                self.index_metadata(i, m);
            }
        }

        // Rebuild HNSW from scratch with compacted vectors
        self.hnsw.rebuild(&self.vectors_raw, self.metric);

        deleted_count
    }

    /// Check if a vector exists by ID.
    pub fn exists(&self, id: &Bytes) -> bool {
        self.id_to_index.contains_key(id)
    }

    /// Get the number of deleted vectors.
    pub fn vectors_deleted(&self) -> u64 {
        self.vectors_deleted
    }

    /// Get the current number of non-deleted vectors stored.
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|e| !e.deleted).count()
    }

    /// Check if the index is empty (no non-deleted vectors).
    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(|e| e.deleted)
    }

    /// Get the vector dimension.
    pub fn dimension(&self) -> u32 {
        self.dimension
    }

    /// Get the distance metric.
    pub fn metric(&self) -> DistanceMetric {
        self.metric
    }

    /// Get the maximum capacity.
    pub fn max_capacity(&self) -> u64 {
        self.max_capacity
    }

    /// Get the total vectors ever added (including deleted).
    pub fn vectors_added(&self) -> u64 {
        self.vectors_added
    }

    /// Get the M parameter (max connections per node).
    pub fn m(&self) -> usize {
        self.m
    }

    /// Get the ef_construction parameter.
    pub fn ef_construction(&self) -> usize {
        self.ef_construction
    }

    /// Get the ef_search parameter.
    pub fn ef_search(&self) -> usize {
        self.ef_search
    }

    // ─── Quantization Methods ──────────────────────────────────────────────

    /// Get the quantization method.
    pub fn quantization_method(&self) -> QuantizationMethod {
        self.quantization
    }

    /// Enable INT8 quantization for existing vectors.
    /// Returns the memory saved in bytes.
    pub fn enable_quantization(&mut self, method: QuantizationMethod) -> Result<usize, String> {
        if method == QuantizationMethod::None {
            return Ok(0);
        }

        let old_memory = self.memory_usage();
        self.quantization = method;
        self.quantized_vectors.clear();
        self.quantization_params.clear();

        match method {
            QuantizationMethod::Int8 => {
                for v in &self.vectors_raw {
                    let (quantized, scale, zero_point) = quantize_int8(v);
                    self.quantized_vectors.push(quantized);
                    self.quantization_params
                        .push(QuantizationParams { scale, zero_point });
                }
            }
            QuantizationMethod::None => {}
            QuantizationMethod::ProductQuantize => {
                if self.pq.is_none() {
                    return Err("PQ not trained: call VS.TRAINPQ first".to_string());
                }
                for v in &self.vectors_raw {
                    if let Some(ref pq) = self.pq {
                        self.pq_codes.push(pq.encode(v));
                    }
                }
            }
        }

        let new_memory = self.memory_usage();
        Ok(old_memory.saturating_sub(new_memory))
    }

    /// Get quantized vector if available.
    pub fn get_quantized(&self, idx: usize) -> Option<&[u8]> {
        self.quantized_vectors.get(idx).map(|v| v.as_slice())
    }

    /// Get quantization params for a vector.
    pub fn get_quantization_params(&self, idx: usize) -> Option<&QuantizationParams> {
        self.quantization_params.get(idx)
    }

    // ─── TTL Methods ───────────────────────────────────────────────────────

    /// Set TTL in seconds for the entire index.
    pub fn set_ttl(&mut self, seconds: u64) {
        self.ttl_seconds = Some(seconds);
        self.created_at = current_timestamp_secs();
    }

    /// Get TTL in seconds.
    pub fn get_ttl(&self) -> Option<u64> {
        self.ttl_seconds
    }

    /// Get remaining TTL in seconds.
    pub fn get_remaining_ttl(&self) -> Option<u64> {
        let ttl = self.ttl_seconds?;
        let now = current_timestamp_secs();
        let elapsed = now.saturating_sub(self.created_at);
        Some(ttl.saturating_sub(elapsed))
    }

    /// Check if the index has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(remaining) = self.get_remaining_ttl() {
            remaining == 0
        } else {
            false
        }
    }

    /// Get creation timestamp.
    pub fn created_at(&self) -> u64 {
        self.created_at
    }

    /// Get last expiry check timestamp.
    pub fn last_expiry_check(&self) -> u64 {
        self.last_expiry_check
    }

    /// Update last expiry check timestamp.
    pub fn update_expiry_check(&mut self) {
        self.last_expiry_check = current_timestamp_secs();
    }

    // ─── Count with Filter ─────────────────────────────────────────────────

    /// Count vectors matching a filter (excludes deleted).
    pub fn count_with_filter(&self, filter: &MetadataFilter) -> usize {
        self.entries
            .iter()
            .filter(|e| !e.deleted && filter.matches(e.metadata.as_ref()))
            .count()
    }

    // ─── PQ Quantization Methods ──────────────────────────────────────────

    /// Train PQ codebooks on existing vectors.
    pub fn train_pq(&mut self, num_subspaces: usize, bits: usize) -> Result<(), String> {
        if self.vectors_raw.is_empty() {
            return Err("cannot train PQ on empty index".to_string());
        }

        let dim = self.dimension as usize;
        let subs = if num_subspaces == 0 {
            (dim / 4).max(1)
        } else {
            num_subspaces
        };
        let bits_per_code = if bits == 0 { 8 } else { bits };

        let mut pq = ProductQuantizer::new(dim, subs, bits_per_code);
        pq.train(&self.vectors_raw, 20);

        // Encode all vectors
        self.pq_codes.clear();
        for v in &self.vectors_raw {
            self.pq_codes.push(pq.encode(v));
        }

        self.pq = Some(pq);
        Ok(())
    }

    /// Get PQ codes for a vector.
    pub fn get_pq_codes(&self, idx: usize) -> Option<&[u8]> {
        self.pq_codes.get(idx).map(|v| v.as_slice())
    }

    /// Check if PQ is enabled.
    pub fn has_pq(&self) -> bool {
        self.pq.is_some()
    }

    /// Get PQ info as (num_subspaces, bits_per_code).
    pub fn pq_info(&self) -> (usize, usize) {
        self.pq
            .as_ref()
            .map(|p| (p.num_subspaces, p.bits_per_code))
            .unwrap_or((0, 0))
    }

    // ─── Hybrid Search Methods ────────────────────────────────────────────

    /// Add a document for hybrid search (vector + text).
    pub fn add_hybrid_document(&mut self, idx: usize, text: &str) {
        self.bm25.add_document(idx, text);
    }

    /// Set hybrid search weights (0.0 to 1.0 each).
    pub fn set_hybrid_weights(&mut self, vector_weight: f32, bm25_weight: f32) {
        self.hybrid_weight_vector = vector_weight;
        self.hybrid_weight_bm25 = bm25_weight;
    }

    /// Get hybrid search weights.
    pub fn hybrid_weights(&self) -> (f32, f32) {
        (self.hybrid_weight_vector, self.hybrid_weight_bm25)
    }

    /// Hybrid search: combine vector similarity with BM25 text relevance.
    pub fn hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        k: usize,
        ef: Option<usize>,
        filter: Option<&MetadataFilter>,
    ) -> Result<Vec<SearchResult>, String> {
        // Vector search
        let vector_results = self.search_internal(query_vector, k, ef, filter, None)?;

        // BM25 search
        let query_terms = BM25Index::tokenize(query_text);
        let bm25_scores = self.bm25.score(&query_terms);

        // Merge with RRF
        let vector_refs: Vec<(usize, f32)> = vector_results
            .iter()
            .enumerate()
            .filter_map(|(rank, r)| {
                let idx = *self.id_to_index.get(&r.id)?;
                Some((idx, rank as f32))
            })
            .collect();

        let bm25_refs: Vec<(usize, f32)> = bm25_scores
            .iter()
            .map(|&(doc_id, score)| (doc_id, score))
            .collect();

        let merged = reciprocal_rank_fusion(
            &vector_refs,
            &bm25_refs,
            k,
            self.hybrid_weight_vector,
            self.hybrid_weight_bm25,
        );

        let results: Vec<SearchResult> = merged
            .into_iter()
            .map(|(idx, score)| SearchResult {
                id: self.entries[idx].id.clone(),
                distance: score,
                vector: self.entries[idx].vector.clone(),
                metadata: self.entries[idx].metadata.clone(),
            })
            .collect();

        Ok(results)
    }

    // ─── Metadata Inverted Index ──────────────────────────────────────────

    /// Index metadata fields for fast equality filtering.
    fn index_metadata(&mut self, idx: usize, metadata: &Bytes) {
        if let Ok(serde_json::Value::Object(map)) =
            serde_json::from_slice::<serde_json::Value>(metadata)
        {
            for (key, value) in map {
                let value_str = match value {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "null".to_string(),
                    _ => value.to_string(),
                };
                self.metadata_index
                    .entry(key)
                    .or_default()
                    .entry(value_str)
                    .or_default()
                    .push(idx);
            }
        }
    }

    /// Remove metadata from inverted index.
    fn remove_metadata_index(&mut self, idx: usize, metadata: &Option<Bytes>) {
        if let Some(meta) = metadata
            && let Ok(serde_json::Value::Object(map)) =
                serde_json::from_slice::<serde_json::Value>(meta)
        {
            for (key, value) in map {
                let value_str = match value {
                    serde_json::Value::String(s) => s,
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "null".to_string(),
                    _ => value.to_string(),
                };
                if let Some(field_map) = self.metadata_index.get_mut(&key) {
                    if let Some(indices) = field_map.get_mut(&value_str) {
                        indices.retain(|&i| i != idx);
                        if indices.is_empty() {
                            field_map.remove(&value_str);
                        }
                    }
                    if field_map.is_empty() {
                        self.metadata_index.remove(&key);
                    }
                }
            }
        }
    }

    /// Get candidate indices from inverted index for an equality filter.
    fn metadata_index_lookup(&self, key: &str, value: &str) -> Option<&Vec<usize>> {
        self.metadata_index.get(key)?.get(value)
    }

    /// Add a vector with the given ID. Returns true if new, false if overwritten.
    pub fn add(
        &mut self,
        id: Bytes,
        vector: Vec<f32>,
        metadata: Option<Bytes>,
    ) -> Result<bool, String> {
        if vector.len() != self.dimension as usize {
            return Err(format!(
                "vector dimension mismatch: expected {}, got {}",
                self.dimension,
                vector.len()
            ));
        }

        // Validate vector values
        validate_vector(&vector)?;

        // Normalize vector if needed (for Cosine metric)
        let final_vector = if self.normalize_vectors {
            normalize_vector(&vector)
                .ok_or_else(|| "cannot normalize zero vector for Cosine metric".to_string())?
        } else {
            vector
        };

        if let Some(&idx) = self.id_to_index.get(&id) {
            // Overwrite existing - use incremental HNSW update (avoids full rebuild)
            let old_metadata = self.entries[idx].metadata.take();
            self.remove_metadata_index(idx, &old_metadata);
            self.entries[idx].vector = final_vector.clone();
            self.entries[idx].metadata = metadata.clone();
            self.vectors_raw[idx] = final_vector.clone();
            if let Some(ref meta) = metadata {
                self.index_metadata(idx, meta);
            }

            // Update quantized representation if quantization is active
            match self.quantization {
                QuantizationMethod::Int8 => {
                    let (quantized, scale, zero_point) = quantize_int8(&final_vector);
                    if idx < self.quantized_vectors.len() {
                        self.quantized_vectors[idx] = quantized;
                    }
                    if idx < self.quantization_params.len() {
                        self.quantization_params[idx] = QuantizationParams { scale, zero_point };
                    }
                }
                QuantizationMethod::ProductQuantize => {
                    if let Some(ref pq) = self.pq {
                        let codes = pq.encode(&final_vector);
                        if idx < self.pq_codes.len() {
                            self.pq_codes[idx] = codes;
                        }
                    }
                }
                QuantizationMethod::None => {}
            }

            // Incrementally update HNSW (removes old connections, re-inserts with new vector)
            self.hnsw.update_node(idx, &self.vectors_raw, self.metric);
            return Ok(false);
        }

        if self.entries.iter().filter(|e| !e.deleted).count() as u64 >= self.max_capacity {
            return Err("index is full".to_string());
        }

        let idx = self.entries.len();
        self.entries.push(VectorEntry {
            id: id.clone(),
            vector: final_vector.clone(),
            metadata: metadata.clone(),
            deleted: false,
        });
        self.vectors_raw.push(final_vector);
        self.id_to_index.insert(id, idx);
        self.vectors_added += 1;

        if let Some(ref meta) = metadata {
            self.index_metadata(idx, meta);
        }

        // Insert into HNSW index
        self.hnsw.insert(idx, &self.vectors_raw, self.metric);

        Ok(true)
    }

    /// Add multiple vectors at once. Returns Vec of results (Ok(true)=new, Ok(false)=overwrite, Err=msg).
    pub fn madd(
        &mut self,
        entries: Vec<(Bytes, Vec<f32>, Option<Bytes>)>,
    ) -> Vec<Result<bool, String>> {
        entries
            .into_iter()
            .map(|(id, vector, meta)| self.add(id, vector, meta))
            .collect()
    }

    /// Get a vector entry by ID. Returns None if not found or deleted.
    pub fn get(&self, id: &Bytes) -> Option<&VectorEntry> {
        self.id_to_index
            .get(id)
            .map(|&idx| &self.entries[idx])
            .filter(|e| !e.deleted)
    }

    /// Remove a vector by ID. Returns true if found and removed.
    /// Uses lazy deletion: marks the entry as deleted and removes from HNSW graph,
    /// but does NOT shift indices or rebuild HNSW. Call `optimize()` to compact.
    pub fn del(&mut self, id: &Bytes) -> bool {
        if let Some(&idx) = self.id_to_index.get(id) {
            if self.entries[idx].deleted {
                return false;
            }

            // Mark as tombstone
            self.entries[idx].deleted = true;
            self.vectors_deleted += 1;

            // Remove from HNSW graph (lazy delete, no rebuild needed)
            self.hnsw.remove_node(idx);

            // Remove metadata from inverted index
            let old_metadata = self.entries[idx].metadata.take();
            self.remove_metadata_index(idx, &old_metadata);

            // Remove from id_to_index so get() returns None for deleted entries
            self.id_to_index.remove(id);

            true
        } else {
            false
        }
    }

    /// Search for k-nearest neighbors to the query vector.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> Result<Vec<SearchResult>, String> {
        self.search_internal(query, k, ef, None, None)
    }

    /// Search with metadata filter and optional distance threshold.
    pub fn search_with_filter(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
        filter: Option<&MetadataFilter>,
        threshold: Option<f32>,
    ) -> Result<Vec<SearchResult>, String> {
        self.search_internal(query, k, ef, filter, threshold)
    }

    /// Internal search implementation with pre-filter HNSW traversal.
    fn search_internal(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
        filter: Option<&MetadataFilter>,
        threshold: Option<f32>,
    ) -> Result<Vec<SearchResult>, String> {
        if query.len() != self.dimension as usize {
            return Err(format!(
                "query dimension mismatch: expected {}, got {}",
                self.dimension,
                query.len()
            ));
        }

        // Validate query vector
        validate_vector(query)?;

        if self.entries.is_empty() {
            return Ok(Vec::new());
        }

        let effective_ef = ef.unwrap_or(self.ef_search);

        // Normalize query for Cosine metric
        let final_query = if self.normalize_vectors {
            normalize_vector(query)
                .ok_or_else(|| "cannot normalize zero query vector for Cosine metric".to_string())?
        } else {
            query.to_vec()
        };

        // Pre-filter function: checks metadata AND threshold during HNSW traversal
        // Try inverted index for simple equality filters first
        let index_candidates: Option<Vec<usize>> = match filter {
            Some(MetadataFilter::Equals(key, value)) => {
                self.metadata_index_lookup(key, value).cloned()
            }
            _ => None,
        };

        let index_set: Option<std::collections::HashSet<usize>> =
            index_candidates.map(|v| v.into_iter().collect());

        let filter_fn = |idx: usize| -> bool {
            // Skip deleted entries
            if self.entries[idx].deleted {
                return false;
            }
            // Check threshold
            if let Some(t) = threshold {
                let dist = compute_distance(&final_query, &self.vectors_raw[idx], self.metric);
                if dist > t {
                    return false;
                }
            }
            // Check inverted index candidates first (O(1) lookup)
            if let Some(ref candidates) = index_set
                && !candidates.contains(&idx)
            {
                return false;
            }
            // Check metadata filter (for complex filters or when no index available)
            if index_set.is_none()
                && let Some(f) = filter
                && !f.matches(self.entries[idx].metadata.as_ref())
            {
                return false;
            }
            true
        };

        let has_filter = filter.is_some() || threshold.is_some();

        // Use pre-filter during HNSW traversal for better performance
        let raw_results = if has_filter {
            // Use pre-filtered search: filter is checked during graph traversal
            // Adaptive ef: cap at total entries to avoid excessive expansion
            let total_entries = self.entries.len();
            let search_ef = if let Some(ref candidates) = index_set {
                // When inverted index provides candidates, ef is bounded by match count
                let match_count = candidates.len();
                (effective_ef + match_count).min(total_entries)
            } else {
                // Without index: use 2x multiplier with floor of k*2, capped by total
                (effective_ef * 2).max(k * 2).min(total_entries)
            };
            self.hnsw.search_with_filter(
                &final_query,
                &self.vectors_raw,
                k,
                self.metric,
                search_ef,
                Some(&filter_fn),
            )
        } else {
            // No filter: use standard search
            self.hnsw.search(
                &final_query,
                &self.vectors_raw,
                k,
                self.metric,
                effective_ef,
            )
        };

        self.hnsw
            .total_searches
            .fetch_add(1, AtomicOrdering::Relaxed);

        let mut results: Vec<SearchResult> = raw_results
            .into_iter()
            .map(|(idx, distance)| SearchResult {
                id: self.entries[idx].id.clone(),
                distance,
                vector: self.entries[idx].vector.clone(),
                metadata: self.entries[idx].metadata.clone(),
            })
            .take(k)
            .collect();

        // Ensure sorted by distance
        results.sort_by(|a, b| {
            a.distance
                .partial_cmp(&b.distance)
                .unwrap_or(Ordering::Equal)
        });

        Ok(results)
    }

    /// Batch search: perform multiple queries at once.
    pub fn msearch(
        &self,
        queries: &[Vec<f32>],
        k: usize,
        ef: Option<usize>,
        filter: Option<&MetadataFilter>,
        threshold: Option<f32>,
    ) -> Result<Vec<Vec<SearchResult>>, String> {
        queries
            .iter()
            .map(|q| self.search_internal(q, k, ef, filter, threshold))
            .collect()
    }

    /// Update a vector's embedding and/or metadata, rebuilding HNSW if vector changes.
    pub fn update(
        &mut self,
        id: &Bytes,
        vector: Option<Vec<f32>>,
        metadata: Option<Option<Bytes>>,
    ) -> Result<bool, String> {
        let &idx = self
            .id_to_index
            .get(id)
            .ok_or_else(|| format!("vector '{}' not found", String::from_utf8_lossy(id)))?;

        let mut vector_changed = false;

        if let Some(v) = vector {
            if v.len() != self.dimension as usize {
                return Err(format!(
                    "vector dimension mismatch: expected {}, got {}",
                    self.dimension,
                    v.len()
                ));
            }

            // Validate vector values
            validate_vector(&v)?;

            // Normalize vector if needed
            let final_vector = if self.normalize_vectors {
                normalize_vector(&v)
                    .ok_or_else(|| "cannot normalize zero vector for Cosine metric".to_string())?
            } else {
                v
            };

            self.entries[idx].vector = final_vector.clone();
            self.vectors_raw[idx] = final_vector;
            vector_changed = true;
        }

        if let Some(m) = metadata {
            // Remove old metadata from inverted index
            let old_metadata = self.entries[idx].metadata.take();
            self.remove_metadata_index(idx, &old_metadata);
            // Set new metadata and index it
            if let Some(ref meta_bytes) = m {
                self.index_metadata(idx, meta_bytes);
            }
            self.entries[idx].metadata = m;
        }

        // Incrementally update HNSW if vector data changed (avoids full rebuild)
        if vector_changed {
            self.hnsw.update_node(idx, &self.vectors_raw, self.metric);
        }

        Ok(true)
    }

    /// Estimate actual memory usage in bytes (RSS-oriented: counts allocated capacity).
    pub fn memory_usage(&self) -> usize {
        let base = std::mem::size_of::<Self>();
        let entries_size: usize = self
            .entries
            .iter()
            .map(|e| {
                std::mem::size_of::<VectorEntry>()
                    + e.id.len()
                    + e.vector.len() * std::mem::size_of::<f32>()
                    + e.metadata.as_ref().map_or(0, |m| m.len())
            })
            .sum();
        // vectors_raw stores raw float vectors; len() == actual used elements
        let vectors_raw_size: usize = std::mem::size_of::<Vec<Vec<f32>>>() * self.vectors_raw.len()
            + self
                .vectors_raw
                .iter()
                .map(|v| v.len() * std::mem::size_of::<f32>())
                .sum::<usize>();
        // id_to_index: HashMap overhead + entries (using capacity for actual allocation)
        let id_map_size: usize = self.id_to_index.capacity()
            * (std::mem::size_of::<Bytes>() + std::mem::size_of::<usize>());

        // HNSW graph memory
        let hnsw_node_levels_size = self.hnsw.node_levels.len() * std::mem::size_of::<usize>();
        let hnsw_levels_size: usize = self
            .hnsw
            .levels
            .iter()
            .map(|l| l.len() * std::mem::size_of::<usize>())
            .sum();
        let hnsw_connections_size: usize = self
            .hnsw
            .node_connections
            .iter()
            .map(|node| {
                node.iter()
                    .map(|level| level.len() * std::mem::size_of::<usize>())
                    .sum::<usize>()
            })
            .sum();
        // HashSet capacity is approximate; count actual deleted nodes
        let hnsw_deleted_size = self.hnsw.deleted.len() * std::mem::size_of::<usize>();

        // Quantized vectors and PQ codes
        let quantized_size: usize = self.quantized_vectors.iter().map(|v| v.len()).sum();
        let pq_codes_size: usize = self.pq_codes.iter().map(|v| v.len()).sum();

        // Metadata inverted index
        let metadata_index_size: usize = self
            .metadata_index
            .iter()
            .map(|(k, v)| {
                k.len()
                    + v.iter()
                        .map(|(vk, vl)| {
                            vk.len()
                                + std::mem::size_of::<String>()
                                + vl.len() * std::mem::size_of::<usize>()
                        })
                        .sum::<usize>()
            })
            .sum();

        base + entries_size
            + vectors_raw_size
            + id_map_size
            + hnsw_node_levels_size
            + hnsw_levels_size
            + hnsw_connections_size
            + hnsw_deleted_size
            + quantized_size
            + pq_codes_size
            + metadata_index_size
    }

    /// Serialize the SpinelVector to a compact binary format.
    ///
    /// Format v2:
    /// "SPINELVEC" (9) | version (1) | dimension (4) | metric (1) |
    /// max_capacity (8) | vectors_added (8) | m (4) | ef_construction (4) | ef_search (4) |
    /// vectors_deleted (8) | has_ttl (1) | [ttl_seconds (8)] | created_at (8) |
    /// quantization_method (1) |
    /// [INT8: count(4) | for each: data_len(4) + data + scale(4) + zero_point(1)] |
    /// [PQ: trained(1) | num_subspaces(4) | bits_per_code(4) |
    ///   for each codebook: centroid_count(4) | for each centroid: dim(4) + floats |
    ///   pq_count(4) | for each: len(4) + codes] |
    /// bm25_doc_count(4) | bm25_avg_doc_len(4) |
    /// [for each bm25 entry: doc_id(4) | terms_count(4) | for each term: len(4)+bytes |
    ///   tf_count(4) | for each tf: len(4)+bytes + val(4)] |
    /// idf_count(4) | for each idf: len(4)+bytes + val(4) |
    /// hybrid_weight_vector(4) | hybrid_weight_bm25(4) |
    /// count (4) | [for each vector: id_len (4) | id | vec_len (4) | vec_data | meta_len (4) | meta]
    pub fn serialize(&self) -> Bytes {
        let mut bytes = Vec::new();

        // Header
        bytes.extend_from_slice(Self::MAGIC);
        bytes.push(Self::VERSION);
        bytes.extend_from_slice(&self.dimension.to_le_bytes());
        bytes.push(self.metric as u8);
        bytes.extend_from_slice(&self.max_capacity.to_le_bytes());
        bytes.extend_from_slice(&self.vectors_added.to_le_bytes());
        bytes.extend_from_slice(&(self.m as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.ef_construction as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.ef_search as u32).to_le_bytes());

        // State
        bytes.extend_from_slice(&self.vectors_deleted.to_le_bytes());
        match self.ttl_seconds {
            Some(ttl) => {
                bytes.push(1u8);
                bytes.extend_from_slice(&ttl.to_le_bytes());
            }
            None => {
                bytes.push(0u8);
            }
        }
        bytes.extend_from_slice(&self.created_at.to_le_bytes());

        // Quantization
        bytes.push(self.quantization as u8);
        match self.quantization {
            QuantizationMethod::Int8 => {
                let count = self.quantized_vectors.len() as u32;
                bytes.extend_from_slice(&count.to_le_bytes());
                for (qv, qp) in self
                    .quantized_vectors
                    .iter()
                    .zip(self.quantization_params.iter())
                {
                    bytes.extend_from_slice(&(qv.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(qv);
                    bytes.extend_from_slice(&qp.scale.to_le_bytes());
                    bytes.push(qp.zero_point as u8);
                }
            }
            QuantizationMethod::ProductQuantize => {
                match &self.pq {
                    Some(pq) => {
                        bytes.push(1u8); // trained
                        bytes.extend_from_slice(&(pq.num_subspaces as u32).to_le_bytes());
                        bytes.extend_from_slice(&(pq.bits_per_code as u32).to_le_bytes());
                        for codebook in &pq.codebooks {
                            bytes.extend_from_slice(
                                &(codebook.centroids.len() as u32).to_le_bytes(),
                            );
                            for centroid in &codebook.centroids {
                                bytes.extend_from_slice(&(centroid.len() as u32).to_le_bytes());
                                for &v in centroid {
                                    bytes.extend_from_slice(&v.to_le_bytes());
                                }
                            }
                        }
                    }
                    None => {
                        bytes.push(0u8); // not trained
                    }
                }
                let count = self.pq_codes.len() as u32;
                bytes.extend_from_slice(&count.to_le_bytes());
                for codes in &self.pq_codes {
                    bytes.extend_from_slice(&(codes.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(codes);
                }
            }
            QuantizationMethod::None => {}
        }

        // BM25
        bytes.extend_from_slice(&(self.bm25.doc_count as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.bm25.total_term_count as u32).to_le_bytes());
        bytes.extend_from_slice(&self.bm25.avg_doc_len.to_le_bytes());
        bytes.extend_from_slice(&(self.bm25.entries.len() as u32).to_le_bytes());
        for entry in &self.bm25.entries {
            bytes.extend_from_slice(&(entry.doc_id as u32).to_le_bytes());
            bytes.extend_from_slice(&(entry.terms.len() as u32).to_le_bytes());
            for term in &entry.terms {
                bytes.extend_from_slice(&(term.len() as u32).to_le_bytes());
                bytes.extend_from_slice(term.as_bytes());
            }
            bytes.extend_from_slice(&(entry.term_freq.len() as u32).to_le_bytes());
            for (k, v) in &entry.term_freq {
                bytes.extend_from_slice(&(k.len() as u32).to_le_bytes());
                bytes.extend_from_slice(k.as_bytes());
                bytes.extend_from_slice(&v.to_le_bytes());
            }
        }
        bytes.extend_from_slice(&(self.bm25.idf.len() as u32).to_le_bytes());
        for (k, v) in &self.bm25.idf {
            bytes.extend_from_slice(&(k.len() as u32).to_le_bytes());
            bytes.extend_from_slice(k.as_bytes());
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        bytes.extend_from_slice(&self.hybrid_weight_vector.to_le_bytes());
        bytes.extend_from_slice(&self.hybrid_weight_bm25.to_le_bytes());

        // Vector count (only non-deleted)
        let count = self.entries.iter().filter(|e| !e.deleted).count() as u32;
        bytes.extend_from_slice(&count.to_le_bytes());

        // Vector entries (skip deleted)
        for entry in self.entries.iter().filter(|e| !e.deleted) {
            bytes.extend_from_slice(&(entry.id.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&entry.id);
            bytes.extend_from_slice(&(entry.vector.len() as u32).to_le_bytes());
            for &v in &entry.vector {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            match &entry.metadata {
                Some(meta) => {
                    bytes.extend_from_slice(&(meta.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(meta);
                }
                None => {
                    bytes.extend_from_slice(&0u32.to_le_bytes());
                }
            }
        }

        Bytes::from(bytes)
    }

    /// Deserialize a SpinelVector from the binary format.
    pub fn deserialize(data: &Bytes) -> Option<Self> {
        if data.len() < 9 || !data.starts_with(Self::MAGIC) {
            return None;
        }

        let mut cursor = 9;
        let version = *data.get(cursor)?;
        cursor += 1;

        if version > Self::VERSION {
            return None;
        }

        let dimension = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;

        let metric = match data.get(cursor)? {
            0 => DistanceMetric::L2,
            1 => DistanceMetric::Cosine,
            2 => DistanceMetric::InnerProduct,
            _ => return None,
        };
        cursor += 1;

        let max_capacity = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
        cursor += 8;

        let vectors_added = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
        cursor += 8;

        let m = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;

        let ef_construction = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;

        let ef_search = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;

        // v2 state fields
        let vectors_deleted = if version >= 2 {
            let v = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
            cursor += 8;
            v
        } else {
            0
        };

        let (ttl_seconds, created_at) = if version >= 2 {
            let has_ttl = *data.get(cursor)?;
            cursor += 1;
            let ttl = if has_ttl == 1 {
                let t = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
                cursor += 8;
                Some(t)
            } else {
                None
            };
            let ca = u64::from_le_bytes(data.get(cursor..cursor + 8)?.try_into().ok()?);
            cursor += 8;
            (ttl, ca)
        } else {
            (None, current_timestamp_secs())
        };

        // Quantization
        let (quantization, quantized_vectors, quantization_params, pq, pq_codes) = if version >= 2 {
            let qm = match *data.get(cursor)? {
                0 => QuantizationMethod::None,
                1 => QuantizationMethod::Int8,
                2 => QuantizationMethod::ProductQuantize,
                _ => return None,
            };
            cursor += 1;

            let mut qv = Vec::new();
            let mut qp = Vec::new();
            let mut pq_inst = None;
            let mut pq_c = Vec::new();

            match qm {
                QuantizationMethod::Int8 => {
                    let count = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                    cursor += 4;
                    for _ in 0..count {
                        let data_len =
                            u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                        cursor += 4;
                        let qd =
                            Bytes::copy_from_slice(data.get(cursor..cursor + data_len as usize)?);
                        cursor += data_len as usize;
                        let scale =
                            f32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                        cursor += 4;
                        let zp = *data.get(cursor)? as i8;
                        cursor += 1;
                        qv.push(qd.to_vec());
                        qp.push(QuantizationParams {
                            scale,
                            zero_point: zp,
                        });
                    }
                }
                QuantizationMethod::ProductQuantize => {
                    let trained = *data.get(cursor)?;
                    cursor += 1;
                    if trained == 1 {
                        let num_subspaces =
                            u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?)
                                as usize;
                        cursor += 4;
                        let bits_per_code =
                            u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?)
                                as usize;
                        cursor += 4;
                        let mut codebooks = Vec::with_capacity(num_subspaces);
                        for _ in 0..num_subspaces {
                            let centroid_count =
                                u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?)
                                    as usize;
                            cursor += 4;
                            let mut centroids = Vec::with_capacity(centroid_count);
                            for _ in 0..centroid_count {
                                let dim = u32::from_le_bytes(
                                    data.get(cursor..cursor + 4)?.try_into().ok()?,
                                ) as usize;
                                cursor += 4;
                                let mut centroid = Vec::with_capacity(dim);
                                for _ in 0..dim {
                                    let v = f32::from_le_bytes(
                                        data.get(cursor..cursor + 4)?.try_into().ok()?,
                                    );
                                    cursor += 4;
                                    centroid.push(v);
                                }
                                centroids.push(centroid);
                            }
                            codebooks.push(PQCodebook { centroids });
                        }
                        pq_inst = Some(ProductQuantizer {
                            num_subspaces,
                            bits_per_code,
                            codebooks,
                            trained: true,
                        });
                    }
                    let pq_count =
                        u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                    cursor += 4;
                    for _ in 0..pq_count {
                        let len =
                            u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                        cursor += 4;
                        let codes =
                            Bytes::copy_from_slice(data.get(cursor..cursor + len as usize)?);
                        cursor += len as usize;
                        pq_c.push(codes.to_vec());
                    }
                }
                QuantizationMethod::None => {}
            }
            (qm, qv, qp, pq_inst, pq_c)
        } else {
            (
                QuantizationMethod::None,
                Vec::new(),
                Vec::new(),
                None,
                Vec::new(),
            )
        };

        // BM25
        let (bm25, hybrid_weight_vector, hybrid_weight_bm25) = if version >= 2 {
            let doc_count =
                u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
            cursor += 4;
            // total_term_count added in v3
            let total_term_count = if version >= 3 {
                let v = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                cursor += 4;
                v
            } else {
                // Reconstruct from entries during deserialization
                0
            };
            let avg_doc_len = f32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
            cursor += 4;

            let entries_count =
                u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
            cursor += 4;
            let mut entries = Vec::with_capacity(entries_count);
            for _ in 0..entries_count {
                let doc_id =
                    u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                cursor += 4;
                let terms_count =
                    u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                cursor += 4;
                let mut terms = Vec::with_capacity(terms_count);
                for _ in 0..terms_count {
                    let len = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                    cursor += 4;
                    let s = String::from_utf8(data.get(cursor..cursor + len as usize)?.to_vec())
                        .ok()?;
                    cursor += len as usize;
                    terms.push(s);
                }
                let tf_count =
                    u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
                cursor += 4;
                let mut term_freq = std::collections::HashMap::with_capacity(tf_count);
                for _ in 0..tf_count {
                    let len = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                    cursor += 4;
                    let k = String::from_utf8(data.get(cursor..cursor + len as usize)?.to_vec())
                        .ok()?;
                    cursor += len as usize;
                    let v = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                    cursor += 4;
                    term_freq.insert(k, v);
                }
                entries.push(BM25Entry {
                    doc_id,
                    terms,
                    term_freq,
                });
            }

            let idf_count =
                u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?) as usize;
            cursor += 4;
            let mut idf = std::collections::HashMap::with_capacity(idf_count);
            for _ in 0..idf_count {
                let len = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                cursor += 4;
                let k =
                    String::from_utf8(data.get(cursor..cursor + len as usize)?.to_vec()).ok()?;
                cursor += len as usize;
                let v = f32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                cursor += 4;
                idf.insert(k, v);
            }

            let hwv = f32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
            cursor += 4;
            let hwb = f32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
            cursor += 4;

            // v2 compat: reconstruct total_term_count from entries if not stored
            let resolved_total_term_count = if total_term_count > 0 {
                total_term_count
            } else {
                entries.iter().map(|e| e.terms.len()).sum()
            };

            (
                BM25Index {
                    entries,
                    avg_doc_len,
                    total_term_count: resolved_total_term_count,
                    doc_count,
                    idf,
                    df: std::collections::HashMap::new(),
                    k1: 1.5,
                    b: 0.75,
                },
                hwv,
                hwb,
            )
        } else {
            (BM25Index::new(), 0.5, 0.5)
        };

        let count = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
        cursor += 4;

        let m_usize = m as usize;
        let ef_construction_usize = ef_construction as usize;

        // Auto-normalize for Cosine metric
        let normalize_vectors = metric == DistanceMetric::Cosine;

        let last_expiry_check = created_at;

        let mut sv = Self {
            dimension,
            metric,
            max_capacity,
            vectors_added,
            m: m_usize,
            ef_construction: ef_construction_usize,
            ef_search: ef_search as usize,
            entries: Vec::with_capacity(count as usize),
            vectors_raw: Vec::with_capacity(count as usize),
            id_to_index: HashMap::with_capacity(count as usize),
            hnsw: HnswIndex::new(m_usize, ef_construction_usize, ef_search as usize),
            vectors_deleted,
            normalize_vectors,
            quantization,
            quantized_vectors,
            quantization_params,
            pq,
            pq_codes,
            ttl_seconds,
            created_at,
            last_expiry_check,
            bm25,
            hybrid_weight_vector,
            hybrid_weight_bm25,
            metadata_index: HashMap::new(),
        };

        for _ in 0..count {
            let id_len = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
            cursor += 4;
            let id = Bytes::copy_from_slice(data.get(cursor..cursor + id_len as usize)?);
            cursor += id_len as usize;

            let vec_len = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
            cursor += 4;
            let mut vector = Vec::with_capacity(vec_len as usize);
            for _ in 0..vec_len {
                let v = f32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
                cursor += 4;
                vector.push(v);
            }

            let meta_len = u32::from_le_bytes(data.get(cursor..cursor + 4)?.try_into().ok()?);
            cursor += 4;
            let metadata = if meta_len > 0 {
                let m = Bytes::copy_from_slice(data.get(cursor..cursor + meta_len as usize)?);
                cursor += meta_len as usize;
                Some(m)
            } else {
                None
            };

            // Validate dimension
            if vector.len() != dimension as usize {
                return None;
            }

            let idx = sv.entries.len();
            sv.entries.push(VectorEntry {
                id: id.clone(),
                vector: vector.clone(),
                metadata: metadata.clone(),
                deleted: false,
            });
            sv.vectors_raw.push(vector);
            sv.id_to_index.insert(id, idx);
            if let Some(ref meta) = metadata {
                sv.index_metadata(idx, meta);
            }
            sv.hnsw.insert(idx, &sv.vectors_raw, sv.metric);
        }

        Some(sv)
    }
}

/// A single search result entry.
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub id: Bytes,
    pub distance: f32,
    pub vector: Vec<f32>,
    pub metadata: Option<Bytes>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vector(vals: &[f32]) -> Vec<f32> {
        vals.to_vec()
    }

    #[test]
    fn test_l2_distance_same_vector() {
        let v = make_vector(&[1.0, 2.0, 3.0]);
        assert!((l2_distance(&v, &v)).abs() < f32::EPSILON);
    }

    #[test]
    fn test_l2_distance_known() {
        let a = make_vector(&[0.0, 0.0]);
        let b = make_vector(&[3.0, 4.0]);
        assert!((l2_distance(&a, &b) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_distance_same_vector() {
        let v = make_vector(&[1.0, 2.0, 3.0]);
        let dist = cosine_distance(&v, &v);
        assert!(dist.abs() < 1e-6, "distance: {}", dist);
    }

    #[test]
    fn test_cosine_distance_orthogonal() {
        let a = make_vector(&[1.0, 0.0]);
        let b = make_vector(&[0.0, 1.0]);
        assert!((cosine_distance(&a, &b) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_inner_product_distance() {
        let a = make_vector(&[1.0, 2.0]);
        let b = make_vector(&[3.0, 4.0]);
        // dot = 3 + 8 = 11, negated = -11
        assert!((inner_product_distance(&a, &b) - (-11.0)).abs() < 1e-6);
    }

    #[test]
    fn test_spinel_vector_new() {
        let sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        assert_eq!(sv.dimension(), 3);
        assert_eq!(sv.metric(), DistanceMetric::L2);
        assert!(sv.is_empty());
        assert_eq!(sv.len(), 0);
    }

    #[test]
    fn test_add_single_vector() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let id = Bytes::from_static(b"v1");
        let result = sv.add(id.clone(), make_vector(&[1.0, 2.0, 3.0]), None);
        assert!(result.is_ok());
        assert!(result.unwrap()); // new
        assert_eq!(sv.len(), 1);
        assert!(!sv.is_empty());
        let entry = sv.get(&id).unwrap();
        assert_eq!(entry.id, id);
    }

    #[test]
    fn test_add_overwrite() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let id = Bytes::from_static(b"v1");
        sv.add(id.clone(), make_vector(&[1.0, 2.0, 3.0]), None)
            .unwrap();
        let result = sv.add(id.clone(), make_vector(&[4.0, 5.0, 6.0]), None);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // overwrite
        assert_eq!(sv.len(), 1);
        let entry = sv.get(&id).unwrap();
        assert_eq!(entry.vector, make_vector(&[4.0, 5.0, 6.0]));
    }

    #[test]
    fn test_add_dimension_mismatch() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let result = sv.add(Bytes::from_static(b"v1"), make_vector(&[1.0, 2.0]), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_add_capacity_full() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 2, 16, 200, 10);
        sv.add(
            Bytes::from_static(b"v1"),
            make_vector(&[1.0, 2.0, 3.0]),
            None,
        )
        .unwrap();
        sv.add(
            Bytes::from_static(b"v2"),
            make_vector(&[4.0, 5.0, 6.0]),
            None,
        )
        .unwrap();
        let result = sv.add(
            Bytes::from_static(b"v3"),
            make_vector(&[7.0, 8.0, 9.0]),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_add_with_metadata() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let meta = Bytes::from_static(b"some metadata");
        sv.add(
            Bytes::from_static(b"v1"),
            make_vector(&[1.0, 2.0, 3.0]),
            Some(meta.clone()),
        )
        .unwrap();
        let entry = sv.get(&Bytes::from_static(b"v1")).unwrap();
        assert_eq!(entry.metadata, Some(meta));
    }

    #[test]
    fn test_madd() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let entries = vec![
            (
                Bytes::from_static(b"v1"),
                make_vector(&[1.0, 2.0, 3.0]),
                None,
            ),
            (
                Bytes::from_static(b"v2"),
                make_vector(&[4.0, 5.0, 6.0]),
                None,
            ),
        ];
        let results = sv.madd(entries);
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        assert!(results[1].is_ok());
        assert_eq!(sv.len(), 2);
    }

    #[test]
    fn test_del() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(
            Bytes::from_static(b"v1"),
            make_vector(&[1.0, 2.0, 3.0]),
            None,
        )
        .unwrap();
        sv.add(
            Bytes::from_static(b"v2"),
            make_vector(&[4.0, 5.0, 6.0]),
            None,
        )
        .unwrap();
        assert!(sv.del(&Bytes::from_static(b"v1")));
        assert_eq!(sv.len(), 1);
        assert!(sv.get(&Bytes::from_static(b"v1")).is_none());
        assert!(sv.get(&Bytes::from_static(b"v2")).is_some());
    }

    #[test]
    fn test_del_nonexistent() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        assert!(!sv.del(&Bytes::from_static(b"nope")));
    }

    #[test]
    fn test_search_simple() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(
            Bytes::from_static(b"origin"),
            make_vector(&[0.0, 0.0]),
            None,
        )
        .unwrap();
        sv.add(Bytes::from_static(b"far"), make_vector(&[10.0, 10.0]), None)
            .unwrap();

        let results = sv.search(&make_vector(&[0.0, 0.1]), 1, None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, Bytes::from_static(b"origin"));
    }

    #[test]
    fn test_search_empty() {
        let sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let results = sv.search(&make_vector(&[1.0, 2.0, 3.0]), 10, None).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_dimension_mismatch() {
        let sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let result = sv.search(&make_vector(&[1.0, 2.0]), 10, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let mut sv = SpinelVector::new(4, DistanceMetric::Cosine, 5000, 32, 400, 20);
        for i in 0..50 {
            let id = Bytes::from(format!("vec-{}", i));
            let vector: Vec<f32> = (0..4).map(|j| (i * 4 + j) as f32).collect();
            let meta = Some(Bytes::from(format!("meta-{}", i)));
            sv.add(id, vector, meta).unwrap();
        }
        sv.set_ttl(3600);

        let bytes = sv.serialize();
        let restored = SpinelVector::deserialize(&bytes).expect("deserialize should succeed");

        assert_eq!(restored.dimension(), sv.dimension());
        assert_eq!(restored.metric(), sv.metric());
        assert_eq!(restored.len(), sv.len());
        assert_eq!(restored.vectors_added(), sv.vectors_added());
        assert_eq!(restored.get_ttl(), Some(3600));
        assert_eq!(restored.vectors_deleted(), sv.vectors_deleted());

        for i in 0..50 {
            let id = Bytes::from(format!("vec-{}", i));
            let orig = sv.get(&id).unwrap();
            let rest = restored.get(&id).unwrap();
            assert_eq!(orig.vector, rest.vector);
            assert_eq!(orig.metadata, rest.metadata);
        }
    }

    #[test]
    fn test_serialize_deserialize_v1_compat() {
        // Manually construct a v1 payload (no v2 state fields)
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"SPINELVEC");
        bytes.push(1u8); // version 1
        bytes.extend_from_slice(&3u32.to_le_bytes()); // dimension
        bytes.push(0u8); // metric L2
        bytes.extend_from_slice(&1000u64.to_le_bytes()); // max_capacity
        bytes.extend_from_slice(&2u64.to_le_bytes()); // vectors_added
        bytes.extend_from_slice(&16u32.to_le_bytes()); // m
        bytes.extend_from_slice(&200u32.to_le_bytes()); // ef_construction
        bytes.extend_from_slice(&10u32.to_le_bytes()); // ef_search
        // v1: no vectors_deleted, no ttl, no created_at, no quantization, no bm25
        bytes.extend_from_slice(&2u32.to_le_bytes()); // count
        // vector 1
        bytes.extend_from_slice(&2u32.to_le_bytes()); // id_len
        bytes.extend_from_slice(b"v1");
        bytes.extend_from_slice(&3u32.to_le_bytes()); // vec_len
        for &v in &[1.0f32, 2.0, 3.0] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        bytes.extend_from_slice(&0u32.to_le_bytes()); // meta_len
        // vector 2
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(b"v2");
        bytes.extend_from_slice(&3u32.to_le_bytes());
        for &v in &[4.0f32, 5.0, 6.0] {
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        bytes.extend_from_slice(&0u32.to_le_bytes());

        let data = Bytes::from(bytes);
        let restored = SpinelVector::deserialize(&data).expect("v1 compat should work");
        assert_eq!(restored.len(), 2);
        assert_eq!(restored.get_ttl(), None);
        assert_eq!(restored.vectors_deleted(), 0);
    }

    #[test]
    fn test_deserialize_rejects_bad_magic() {
        let mut bad = Vec::from(b"BAD MAGIC");
        bad.extend_from_slice(&[0u8; 100]);
        assert!(SpinelVector::deserialize(&Bytes::from(bad)).is_none());
    }

    #[test]
    fn test_deserialize_rejects_future_version() {
        let mut bad = Vec::from(b"SPINELVEC");
        bad.push(255u8); // future version
        bad.extend_from_slice(&[0u8; 200]);
        assert!(SpinelVector::deserialize(&Bytes::from(bad)).is_none());
    }

    #[test]
    fn test_memory_usage() {
        let mut sv = SpinelVector::new(3, DistanceMetric::L2, 1000, 16, 200, 10);
        let baseline = sv.memory_usage();
        sv.add(
            Bytes::from_static(b"v1"),
            make_vector(&[1.0, 2.0, 3.0]),
            None,
        )
        .unwrap();
        assert!(sv.memory_usage() > baseline);
    }

    #[test]
    fn test_search_cosine() {
        let mut sv = SpinelVector::new(2, DistanceMetric::Cosine, 1000, 16, 200, 10);
        sv.add(Bytes::from_static(b"east"), make_vector(&[1.0, 0.0]), None)
            .unwrap();
        sv.add(Bytes::from_static(b"north"), make_vector(&[0.0, 1.0]), None)
            .unwrap();

        // Query pointing east should find "east" as nearest
        let results = sv.search(&make_vector(&[1.0, 0.1]), 1, None).unwrap();
        assert_eq!(results[0].id, Bytes::from_static(b"east"));
    }

    #[test]
    fn test_search_inner_product() {
        let mut sv = SpinelVector::new(2, DistanceMetric::InnerProduct, 1000, 16, 200, 10);
        sv.add(Bytes::from_static(b"a"), make_vector(&[1.0, 0.0]), None)
            .unwrap();
        sv.add(Bytes::from_static(b"b"), make_vector(&[0.5, 0.5]), None)
            .unwrap();

        // Query [1, 0] has higher dot product with [1, 0] than [0.5, 0.5]
        let results = sv.search(&make_vector(&[1.0, 0.0]), 2, None).unwrap();
        assert_eq!(results[0].id, Bytes::from_static(b"a"));
        assert!(results[0].distance < results[1].distance);
    }

    #[test]
    fn test_search_k_limits_results() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        for i in 0..10 {
            sv.add(
                Bytes::from(format!("v{}", i)),
                make_vector(&[i as f32, 0.0]),
                None,
            )
            .unwrap();
        }
        let results = sv.search(&make_vector(&[0.0, 0.0]), 3, None).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_metric_as_str() {
        assert_eq!(DistanceMetric::L2.as_str(), "L2");
        assert_eq!(DistanceMetric::Cosine.as_str(), "COSINE");
        assert_eq!(DistanceMetric::InnerProduct.as_str(), "IP");
    }

    #[test]
    fn test_metric_parse() {
        assert_eq!(DistanceMetric::parse("L2"), Some(DistanceMetric::L2));
        assert_eq!(
            DistanceMetric::parse("COSINE"),
            Some(DistanceMetric::Cosine)
        );
        assert_eq!(
            DistanceMetric::parse("IP"),
            Some(DistanceMetric::InnerProduct)
        );
        assert_eq!(DistanceMetric::parse("INVALID"), None);
    }

    #[test]
    fn test_search_with_threshold() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(Bytes::from_static(b"close"), make_vector(&[0.0, 0.0]), None)
            .unwrap();
        sv.add(Bytes::from_static(b"far"), make_vector(&[10.0, 10.0]), None)
            .unwrap();

        // Tight threshold should only return the close vector
        let results = sv
            .search_with_filter(&make_vector(&[0.0, 0.0]), 10, None, None, Some(1.0))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, Bytes::from_static(b"close"));
    }

    #[test]
    fn test_search_with_metadata_filter() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        let meta_a = Some(Bytes::from(r#"{"category":"cat","score":"9"}"#));
        let meta_b = Some(Bytes::from(r#"{"category":"dog","score":"5"}"#));
        sv.add(
            Bytes::from_static(b"vec_a"),
            make_vector(&[1.0, 0.0]),
            meta_a,
        )
        .unwrap();
        sv.add(
            Bytes::from_static(b"vec_b"),
            make_vector(&[0.9, 0.1]),
            meta_b,
        )
        .unwrap();

        // Filter by category=cat
        let filter = MetadataFilter::parse_expr("category=cat").unwrap();
        let results = sv
            .search_with_filter(&make_vector(&[1.0, 0.0]), 10, None, Some(&filter), None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, Bytes::from_static(b"vec_a"));

        // Filter by category=dog
        let filter = MetadataFilter::parse_expr("category=dog").unwrap();
        let results = sv
            .search_with_filter(&make_vector(&[1.0, 0.0]), 10, None, Some(&filter), None)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, Bytes::from_static(b"vec_b"));
    }

    #[test]
    fn test_search_with_filter_and_threshold() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(
            Bytes::from_static(b"v1"),
            make_vector(&[0.0, 0.0]),
            Some(Bytes::from(r#"{"type":"a"}"#)),
        )
        .unwrap();
        sv.add(
            Bytes::from_static(b"v2"),
            make_vector(&[0.1, 0.0]),
            Some(Bytes::from(r#"{"type":"b"}"#)),
        )
        .unwrap();
        sv.add(
            Bytes::from_static(b"v3"),
            make_vector(&[10.0, 10.0]),
            Some(Bytes::from(r#"{"type":"a"}"#)),
        )
        .unwrap();

        // Filter type=a AND threshold=1.0
        let filter = MetadataFilter::parse_expr("type=a").unwrap();
        let results = sv
            .search_with_filter(
                &make_vector(&[0.0, 0.0]),
                10,
                None,
                Some(&filter),
                Some(1.0),
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, Bytes::from_static(b"v1"));
    }

    #[test]
    fn test_msearch_batch() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(Bytes::from_static(b"east"), make_vector(&[1.0, 0.0]), None)
            .unwrap();
        sv.add(Bytes::from_static(b"north"), make_vector(&[0.0, 1.0]), None)
            .unwrap();

        let queries = vec![make_vector(&[1.0, 0.0]), make_vector(&[0.0, 1.0])];
        let results = sv.msearch(&queries, 1, None, None, None).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0][0].id, Bytes::from_static(b"east"));
        assert_eq!(results[1][0].id, Bytes::from_static(b"north"));
    }

    #[test]
    fn test_update_vector() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(
            Bytes::from_static(b"v1"),
            make_vector(&[0.0, 0.0]),
            Some(Bytes::from("old")),
        )
        .unwrap();

        // Update vector
        sv.update(
            &Bytes::from_static(b"v1"),
            Some(make_vector(&[1.0, 1.0])),
            None,
        )
        .unwrap();

        let entry = sv.get(&Bytes::from_static(b"v1")).unwrap();
        assert_eq!(entry.vector, make_vector(&[1.0, 1.0]));
        assert_eq!(entry.metadata, Some(Bytes::from("old")));
    }

    #[test]
    fn test_update_metadata() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(
            Bytes::from_static(b"v1"),
            make_vector(&[0.0, 0.0]),
            Some(Bytes::from("old")),
        )
        .unwrap();

        sv.update(
            &Bytes::from_static(b"v1"),
            None,
            Some(Some(Bytes::from("new"))),
        )
        .unwrap();

        let entry = sv.get(&Bytes::from_static(b"v1")).unwrap();
        assert_eq!(entry.metadata, Some(Bytes::from("new")));
    }

    #[test]
    fn test_update_not_found() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        let result = sv.update(
            &Bytes::from_static(b"missing"),
            Some(make_vector(&[1.0, 1.0])),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_update_dimension_mismatch() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(Bytes::from_static(b"v1"), make_vector(&[0.0, 0.0]), None)
            .unwrap();

        let result = sv.update(
            &Bytes::from_static(b"v1"),
            Some(make_vector(&[1.0, 2.0, 3.0])),
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_filter_parse() {
        let f = MetadataFilter::parse_expr("category=cat").unwrap();
        let meta = Some(Bytes::from(r#"{"category":"cat"}"#));
        assert!(f.matches(meta.as_ref()));

        let meta2 = Some(Bytes::from(r#"{"category":"dog"}"#));
        assert!(!f.matches(meta2.as_ref()));
    }

    #[test]
    fn test_metadata_filter_not_equals() {
        let f = MetadataFilter::parse_expr("status!=deleted").unwrap();
        let meta = Some(Bytes::from(r#"{"status":"active"}"#));
        assert!(f.matches(meta.as_ref()));
        let meta2 = Some(Bytes::from(r#"{"status":"deleted"}"#));
        assert!(!f.matches(meta2.as_ref()));
    }

    #[test]
    fn test_metadata_filter_and() {
        let f = MetadataFilter::parse_expr("type=a AND status=active").unwrap();
        let meta = Some(Bytes::from(r#"{"type":"a","status":"active"}"#));
        assert!(f.matches(meta.as_ref()));
        let meta2 = Some(Bytes::from(r#"{"type":"a","status":"deleted"}"#));
        assert!(!f.matches(meta2.as_ref()));
    }

    #[test]
    fn test_metadata_filter_or() {
        let f = MetadataFilter::parse_expr("type=a OR type=b").unwrap();
        let meta = Some(Bytes::from(r#"{"type":"a"}"#));
        assert!(f.matches(meta.as_ref()));
        let meta2 = Some(Bytes::from(r#"{"type":"b"}"#));
        assert!(f.matches(meta2.as_ref()));
        let meta3 = Some(Bytes::from(r#"{"type":"c"}"#));
        assert!(!f.matches(meta3.as_ref()));
    }

    #[test]
    fn test_metadata_filter_not() {
        let f = MetadataFilter::parse_expr("NOT type=cat").unwrap();
        let meta = Some(Bytes::from(r#"{"type":"dog"}"#));
        assert!(f.matches(meta.as_ref()));
        let meta2 = Some(Bytes::from(r#"{"type":"cat"}"#));
        assert!(!f.matches(meta2.as_ref()));
    }

    #[test]
    fn test_max_dimension() {
        assert_eq!(SpinelVector::MAX_DIMENSION, 65536);
    }

    #[test]
    fn test_quantize_int8_extreme_values_no_panic() {
        // Vector with values that would cause zero_point to overflow i8
        let v = vec![1e6, -1e6, 1e6, -1e6];
        let (quantized, scale, zero_point) = quantize_int8(&v);
        assert_eq!(quantized.len(), 4);
        assert!(scale > 0.0);
        // zero_point should be clamped to i8 range (i8::MIN..=i8::MAX is always true for i8)
        // Just verify it was computed without panic

        // Roundtrip: dequantize should approximate original
        let restored = dequantize_int8(&quantized, scale, zero_point);
        for (orig, rest) in v.iter().zip(restored.iter()) {
            // Quantization loses precision, but sign should be preserved
            assert_eq!(orig.signum(), rest.signum());
        }
    }

    #[test]
    fn test_quantize_int8_all_same_values() {
        let v = vec![5.0, 5.0, 5.0, 5.0];
        let (quantized, _scale, _zp) = quantize_int8(&v);
        // All same values should quantize to same value
        assert!(quantized.windows(2).all(|w| w[0] == w[1]));
    }

    #[test]
    fn test_metadata_filter_equals_missing_key_returns_false() {
        // Metadata exists but doesn't contain the filter key
        let f = MetadataFilter::parse_expr("nonexistent=value").unwrap();
        let meta = Some(Bytes::from(r#"{"other_key":"value"}"#));
        assert!(!f.matches(meta.as_ref()));
    }

    #[test]
    fn test_metadata_filter_equals_no_json_returns_false() {
        // Metadata is not valid JSON
        let f = MetadataFilter::parse_expr("key=value").unwrap();
        let meta = Some(Bytes::from_static(b"not json at all"));
        assert!(!f.matches(meta.as_ref()));
    }

    #[test]
    fn test_metadata_filter_equals_empty_metadata_returns_false() {
        let f = MetadataFilter::parse_expr("key=value").unwrap();
        assert!(!f.matches(None));
    }

    #[test]
    fn test_metadata_filter_equals_partial_match_rejected() {
        // "cat" should NOT match "category" key
        let f = MetadataFilter::parse_expr("type=cat").unwrap();
        let meta = Some(Bytes::from(r#"{"type":"category"}"#));
        assert!(!f.matches(meta.as_ref()));
    }

    #[test]
    fn test_hybrid_search_empty_bm25_no_panic() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(Bytes::from_static(b"v1"), make_vector(&[1.0, 0.0]), None)
            .unwrap();

        // Hybrid search with no BM25 documents indexed should not panic
        let results = sv.hybrid_search(&make_vector(&[1.0, 0.0]), "test query", 1, None, None);
        assert!(results.is_ok());
        assert_eq!(results.unwrap().len(), 1);
    }

    #[test]
    fn test_hybrid_search_with_bm25_documents() {
        let mut sv = SpinelVector::new(2, DistanceMetric::L2, 1000, 16, 200, 10);
        sv.add(Bytes::from_static(b"v1"), make_vector(&[1.0, 0.0]), None)
            .unwrap();
        sv.add(Bytes::from_static(b"v2"), make_vector(&[0.0, 1.0]), None)
            .unwrap();

        // Index BM25 documents
        sv.add_hybrid_document(0, "red apple fruit");
        sv.add_hybrid_document(1, "blue ocean water");

        // Query matching v1's text
        let results = sv
            .hybrid_search(&make_vector(&[1.0, 0.0]), "apple", 2, None, None)
            .unwrap();
        assert_eq!(results.len(), 2);
        // v1 should rank higher due to BM25 match
        assert_eq!(results[0].id, Bytes::from_static(b"v1"));
    }
}
