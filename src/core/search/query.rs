// src/core/search/query.rs

use crate::core::SpinelDBError;
use crate::core::search::schema::{FieldType, Schema};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Term {
    Field(String, String), // field_name, value
    General(String),       // value
}

#[derive(Debug, Clone, Default)]
pub struct Query {
    pub terms: Vec<Term>,
}

impl Query {
    pub fn matches(&self, doc_fields: &HashMap<String, String>, schema: &Schema) -> bool {
        for term in &self.terms {
            let found_match = match term {
                Term::General(value) => {
                    // Check all TEXT fields in the document for a partial match
                    schema
                        .fields
                        .iter()
                        .filter(|(_, field)| field.field_type == FieldType::Text)
                        .any(|(field_name, _)| {
                            doc_fields
                                .get(field_name)
                                .is_some_and(|doc_val| doc_val.contains(value))
                        })
                }
                Term::Field(field_name, value) => {
                    // Check a specific field for a partial match
                    doc_fields
                        .get(field_name)
                        .is_some_and(|doc_val| doc_val.contains(value))
                }
            };
            // All terms must match (AND logic)
            if !found_match {
                return false;
            }
        }
        true
    }
}

pub struct QueryParser;

impl QueryParser {
    pub fn parse(query_str: &str) -> Result<Query, SpinelDBError> {
        let mut terms = Vec::new();
        let parts = query_str.split_whitespace();

        for part in parts {
            if part.starts_with('@') {
                let mut field_split = part.splitn(2, ':');
                let field_name = field_split.next().map(|s| s.trim_start_matches('@'));
                let value = field_split.next();

                if let (Some(name), Some(val)) = (field_name, value) {
                    if !name.is_empty() && !val.is_empty() {
                        terms.push(Term::Field(name.to_string(), val.to_lowercase()));
                    }
                } else {
                    // If format is invalid, treat as a general term
                    terms.push(Term::General(part.to_lowercase()));
                }
            } else {
                terms.push(Term::General(part.to_lowercase()));
            }
        }

        Ok(Query { terms })
    }
}
