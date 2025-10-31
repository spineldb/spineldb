// src/core/search/index.rs

use super::schema::{Field, FieldOption, FieldType, Schema};
use crate::core::SpinelDBError;
use crate::core::storage::document::Document;
use crate::core::types::{BytesExt, SpinelString};
use anyhow::Result;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

/// A map from a term (e.g., a word or a tag) to a list of internal document IDs.
pub type InvertedIndex = DashMap<SpinelString, Vec<u64>>;

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
    /// A map from a document's external ID (doc_id) to its internal ID.
    pub doc_id_map: DashMap<SpinelString, u64>,
    next_doc_id: AtomicU64,
}

impl SearchIndex {
    pub fn new(name: String, prefix: String, schema: Schema) -> Self {
        let mut inverted_indexes = HashMap::new();
        for (field_name, field) in &schema.fields {
            if !field.options.contains(&FieldOption::NoIndex) {
                inverted_indexes.insert(field_name.clone(), InvertedIndex::new());
            }
        }

        Self {
            name,
            prefix,
            schema,
            documents: DocumentStore::new(),
            inverted_indexes,
            doc_id_map: DashMap::new(),
            next_doc_id: AtomicU64::new(0),
        }
    }

    /// 3. Tokenizing the relevant fields and updating the inverted indexes.
    pub fn add(&mut self, document: Document, replace: bool) -> Result<(), SpinelDBError> {
        let doc_id = document.id.clone();

        // If the document already exists, we may need to remove the old one first.
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

        // Tokenize and update inverted indexes
        for (field_name, field_value) in &document.fields {
            if let Some(field_schema) = self.schema.fields.get(&field_name.string_from_bytes()?)
                && let Some(inverted_index) = self.inverted_indexes.get(&field_schema.name)
            {
                let terms = self.tokenize(field_schema, field_value);
                for term in terms {
                    inverted_index.entry(term).or_default().push(internal_id);
                }
            }
        }

        self.documents.insert(internal_id, document);

        Ok(())
    }

    /// Removes a document from the index by its external ID.
    pub fn remove(&mut self, doc_id: &SpinelString) -> Result<Option<Document>, SpinelDBError> {
        if let Some((_, internal_id)) = self.doc_id_map.remove(doc_id)
            && let Some((_, document)) = self.documents.remove(&internal_id)
        {
            // Remove terms from inverted indexes
            // We need to clone document.fields here because `document` is moved later.
            let fields_to_process = document.fields.clone();
            for (field_name, field_value) in fields_to_process.iter() {
                if let Ok(field_name_str) = field_name.string_from_bytes()
                    && let Some(field_schema) = self.schema.fields.get(&field_name_str)
                {
                    let terms = self.tokenize(field_schema, field_value);
                    if let Some(index) = self.inverted_indexes.get_mut(&field_name_str) {
                        for term in terms {
                            if let Some(mut mut_vec) = index.get_mut(&term) {
                                mut_vec.retain(|&x| x != internal_id);
                            }
                        }
                    }
                }
            }
            return Ok(Some(document));
        }
        Ok(None)
    }

    /// Simple tokenizer.
    /// In a real implementation, this would be much more complex, involving
    /// stemming, stop-word removal, and handling different languages.
    fn tokenize(&self, field: &Field, value: &SpinelString) -> HashSet<SpinelString> {
        let mut tokens = HashSet::new();
        match field.field_type {
            FieldType::Text => {
                // Simple whitespace and punctuation tokenizer
                let text = String::from_utf8_lossy(value).to_lowercase();
                for token in text
                    .split(|c: char| !c.is_alphanumeric())
                    .filter(|s| !s.is_empty())
                {
                    tokens.insert(token.to_string().into());
                }
            }
            FieldType::Tag => {
                // Tags are treated as a whole, but we can have multiple tags separated by a comma
                let text = String::from_utf8_lossy(value);
                for token in text.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    tokens.insert(token.to_string().into());
                }
            }
            FieldType::Numeric => {
                // For now, we don't tokenize numeric fields in the inverted index,
                // as they are used for range queries, which require a different structure (e.g., a B-Tree).
                // We will add this later.
            }
        }
        tokens
    }
}
