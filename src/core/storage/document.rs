// src/core/storage/document.rs

use crate::core::types::SpinelString;
use std::collections::HashMap;

/// Represents a document that can be indexed and searched.
/// It consists of a unique ID, a ranking score, and a collection of fields.
#[derive(Debug, Clone)]
pub struct Document {
    pub id: SpinelString,
    pub score: f64,
    pub fields: HashMap<SpinelString, SpinelString>,
}
