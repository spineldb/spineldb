// src/core/search/index.rs

use super::schema::{FieldOption, FieldType, Schema};
use crate::core::SpinelDBError;
use crate::core::storage::document::Document;
use crate::core::types::{BytesExt, SpinelString};
use anyhow::Result;
use dashmap::DashMap;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

/// A struct holding term occurrence information for scoring
#[derive(Debug, Clone)]
pub struct TermInfo {
    pub positions: Vec<u32>,
    pub frequency: u32,
}

/// A map from a term (e.g., a word or a tag) to a map of internal document IDs to term occurrence information.
pub type InvertedIndex = DashMap<SpinelString, HashMap<u64, TermInfo>>;

/// A map from a numeric value to a list of internal document IDs.
pub type NumericIndex = BTreeMap<OrderedFloat<f64>, Vec<u64>>;

/// A map from an internal document ID to the actual document.
pub type DocumentStore = DashMap<u64, Document>;

#[derive(Debug)]
pub struct SearchIndex {
    pub name: String,
    pub prefix: String,
    pub schema: Schema,
    pub documents: DocumentStore,
    /// A map from an indexed field name to its inverted index.
    pub inverted_indexes: HashMap<String, InvertedIndex>,
    /// A map from an indexed field name to its numeric index.
    pub numeric_indexes: HashMap<String, NumericIndex>,
    /// A map from a document's external ID (doc_id) to its internal ID.
    pub doc_id_map: DashMap<SpinelString, u64>,
    pub doc_count: AtomicU64,
    next_doc_id: AtomicU64,
}

impl SearchIndex {
    pub fn new(name: String, prefix: String, schema: Schema) -> Self {
        let mut inverted_indexes = HashMap::new();
        let mut numeric_indexes = HashMap::new();
        for (field_name, field) in &schema.fields {
            if !field.options.contains(&FieldOption::NoIndex) {
                match field.field_type {
                    FieldType::Text | FieldType::Tag | FieldType::Geo | FieldType::Vector => {
                        inverted_indexes.insert(field_name.clone(), InvertedIndex::new());
                    }
                    FieldType::Numeric => {
                        numeric_indexes.insert(field_name.clone(), NumericIndex::new());
                    }
                }
            }
        }

        Self {
            name,
            prefix,
            schema,
            documents: DocumentStore::new(),
            inverted_indexes,
            numeric_indexes,
            doc_id_map: DashMap::new(),
            doc_count: AtomicU64::new(0),
            next_doc_id: AtomicU64::new(0),
        }
    }

    /// Adds a document to the index.
    pub fn add(&mut self, document: Document, replace: bool) -> Result<(), SpinelDBError> {
        println!("Adding document: {:?} (replace: {})", document.id, replace);
        let doc_id = document.id.clone();

        let should_remove_old = if let Some(_old_internal_id) = self.doc_id_map.get(&doc_id) {
            if !replace {
                return Ok(());
            }
            true
        } else {
            false
        };

        if should_remove_old {
            self.remove(&doc_id)?;
        }

        let internal_id = self.next_doc_id.fetch_add(1, Ordering::SeqCst);
        self.doc_id_map.insert(doc_id.clone(), internal_id);
        self.doc_count.fetch_add(1, Ordering::SeqCst);

        // Collect fields to index first to avoid borrowing issues
        let fields_to_index: Vec<(String, FieldType, SpinelString)> = document
            .fields
            .iter()
            .filter_map(|(field_name_bytes, field_value)| {
                field_name_bytes
                    .string_from_bytes()
                    .ok()
                    .and_then(|field_name_str| {
                        self.schema.fields.get(&field_name_str).map(|field_schema| {
                            (
                                field_name_str,
                                field_schema.field_type.clone(),
                                field_value.clone(),
                            )
                        })
                    })
            })
            .collect();

        // Index fields
        for (field_name, field_type, field_value) in fields_to_index {
            self.index_field(&field_name, field_type, &field_value, internal_id);
        }

        self.documents.insert(internal_id, document);

        Ok(())
    }

    /// Removes a document from the index by its external ID.
    pub fn remove(&mut self, doc_id: &SpinelString) -> Result<Option<Document>, SpinelDBError> {
        if let Some((_, internal_id)) = self.doc_id_map.remove(doc_id)
            && let Some((_, document)) = self.documents.remove(&internal_id)
        {
            self.doc_count.fetch_sub(1, Ordering::SeqCst);
            // Collect fields to deindex first to avoid borrowing issues
            let fields_to_deindex: Vec<(String, FieldType, SpinelString)> = document
                .fields
                .iter()
                .filter_map(|(field_name_bytes, field_value)| {
                    field_name_bytes
                        .string_from_bytes()
                        .ok()
                        .and_then(|field_name_str| {
                            self.schema.fields.get(&field_name_str).map(|field_schema| {
                                (
                                    field_name_str,
                                    field_schema.field_type.clone(),
                                    field_value.clone(),
                                )
                            })
                        })
                })
                .collect();

            // Deindex fields
            for (field_name, field_type, field_value) in fields_to_deindex {
                self.deindex_field(&field_name, field_type, &field_value, internal_id);
            }
            return Ok(Some(document));
        }
        Ok(None)
    }

    /// Indexes a single field based on its type.
    fn index_field(
        &mut self,
        field_name: &str,
        field_type: FieldType,
        value: &SpinelString,
        internal_id: u64,
    ) {
        println!(
            "Indexing field: {} (type: {:?}) for doc_id: {}",
            field_name, field_type, internal_id
        );
        match field_type {
            FieldType::Text | FieldType::Tag => {
                let terms = self.tokenize_text(field_type, value);

                if let Some(inverted_index) = self.inverted_indexes.get_mut(field_name) {
                    for (term, pos) in terms {
                        let mut posting_list = inverted_index.entry(term).or_default();
                        let term_info =
                            posting_list.entry(internal_id).or_insert_with(|| TermInfo {
                                positions: Vec::new(),
                                frequency: 0,
                            });
                        term_info.positions.push(pos);
                        term_info.frequency += 1;
                    }
                }
            }

            FieldType::Numeric => {
                if let Some(numeric_index) = self.numeric_indexes.get_mut(field_name)
                    && let Ok(num_val) = String::from_utf8_lossy(value).parse::<f64>()
                {
                    numeric_index
                        .entry(OrderedFloat(num_val))
                        .or_default()
                        .push(internal_id);
                }
            }

            // Placeholder for Geo and Vector indexing
            FieldType::Geo => {
                // TODO: Implement geospatial indexing
            }

            FieldType::Vector => {
                // TODO: Implement vector similarity indexing
            }
        }
    }

    /// Removes a field's terms from the indexes.
    fn deindex_field(
        &mut self,
        field_name: &str,
        field_type: FieldType,
        value: &SpinelString,
        internal_id: u64,
    ) {
        match field_type {
            FieldType::Text | FieldType::Tag => {
                // Call tokenize_text before getting a mutable borrow of inverted_indexes
                let terms = self.tokenize_text(field_type, value);

                if let Some(inverted_index) = self.inverted_indexes.get_mut(field_name) {
                    for (term, _) in terms {
                        if let Some(mut posting_list) = inverted_index.get_mut(&term) {
                            posting_list.remove(&internal_id);
                        }
                    }
                }
            }
            FieldType::Numeric => {
                // For numeric fields, we need to iterate through the index to find entries with internal_id
                if let Some(numeric_index) = self.numeric_indexes.get_mut(field_name) {
                    for (_value, doc_ids) in numeric_index.iter_mut() {
                        doc_ids.retain(|&id| id != internal_id);
                    }
                }
            }
            // Placeholder for Geo and Vector deindexing
            FieldType::Geo | FieldType::Vector => {
                // TODO: Implement geospatial and vector deindexing
            }
        }
    }

    /// Simple tokenizer for text and tag fields.
    pub fn tokenize_text(
        &self,
        field_type: FieldType,
        value: &SpinelString,
    ) -> Vec<(SpinelString, u32)> {
        let mut tokens = Vec::new();
        match field_type {
            FieldType::Text => {
                let text = String::from_utf8_lossy(value).to_lowercase();
                for (i, token) in text
                    .split(|c: char| !c.is_alphanumeric())
                    .filter(|s| !s.is_empty())
                    .enumerate()
                {
                    tokens.push((token.to_string().into(), i as u32));
                }
            }
            FieldType::Tag => {
                let text = String::from_utf8_lossy(value);
                for (i, token) in text
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .enumerate()
                {
                    tokens.push((token.to_string().into(), i as u32));
                }
            }
            FieldType::Numeric => {
                // Numeric fields are not tokenized into text terms.
            }
            FieldType::Geo | FieldType::Vector => {
                // Geo and Vector fields are not tokenized into text terms.
            }
        }
        tokens
    }
}
