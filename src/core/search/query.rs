// src/core/search/query.rs

use crate::core::SpinelDBError;
use crate::core::search::schema::{FieldType, Schema};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum Term {
    Field(String, String),            // field_name, value
    General(String),                  // value
    NumericRange(String, f64, f64),   // field_name, min, max
    FieldPhrase(String, Vec<String>), // field_name, words
    GeneralPhrase(Vec<String>),       // words
}

#[derive(Debug, Clone, Default)]
pub struct Query {
    pub terms: Vec<Term>,
}

impl Query {
    pub fn matches(&self, doc_fields: &HashMap<String, String>, schema: &Schema) -> bool {
        for term in &self.terms {
            let found_match = match term {
                Term::General(value) => schema
                    .fields
                    .iter()
                    .filter(|(_, field)| field.field_type == FieldType::Text)
                    .any(|(field_name, _)| {
                        doc_fields
                            .get(field_name)
                            .is_some_and(|doc_val| doc_val.contains(value))
                    }),
                Term::Field(field_name, value) => doc_fields
                    .get(field_name)
                    .is_some_and(|doc_val| doc_val.contains(value)),
                Term::NumericRange(field_name, min, max) => doc_fields
                    .get(field_name)
                    .and_then(|s| s.parse::<f64>().ok())
                    .is_some_and(|num| num >= *min && num <= *max),
                Term::FieldPhrase(field_name, words) => doc_fields
                    .get(field_name)
                    .is_some_and(|doc_val| doc_val.contains(&words.join(" "))),
                Term::GeneralPhrase(words) => schema
                    .fields
                    .iter()
                    .filter(|(_, field)| field.field_type == FieldType::Text)
                    .any(|(field_name, _)| {
                        doc_fields
                            .get(field_name)
                            .is_some_and(|doc_val| doc_val.contains(&words.join(" ")))
                    }),
            };
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
        let mut current_term = String::new();
        let mut in_quotes = false;

        for c in query_str.chars() {
            match c {
                '"' => {
                    in_quotes = !in_quotes;
                    current_term.push(c);
                }
                ' ' if !in_quotes => {
                    if !current_term.is_empty() {
                        terms.push(current_term.clone());
                        current_term.clear();
                    }
                }
                _ => {
                    current_term.push(c);
                }
            }
        }
        if !current_term.is_empty() {
            terms.push(current_term);
        }

        let mut query_terms = Vec::new();
        for part in terms {
            if part.starts_with('@') {
                let mut field_split = part.splitn(2, ':');
                let field_name = field_split.next().map(|s| s.trim_start_matches('@'));
                let value = field_split.next();

                if let (Some(name), Some(val)) = (field_name, value) {
                    if val.starts_with('[') && val.ends_with(']') {
                        let range_str = val.trim_matches(|p| p == '[' || p == ']');
                        let mut range_parts = range_str.splitn(2, "..");
                        let min_str = range_parts.next();
                        let max_str = range_parts.next();

                        if let (Some(min_s), Some(max_s)) = (min_str, max_str) {
                            let min = if min_s == "-inf" {
                                f64::NEG_INFINITY
                            } else {
                                min_s.parse::<f64>()?
                            };
                            let max = if max_s == "inf" {
                                f64::INFINITY
                            } else {
                                max_s.parse::<f64>()?
                            };
                            query_terms.push(Term::NumericRange(name.to_string(), min, max));
                        }
                    } else if val.starts_with('"') && val.ends_with('"') {
                        let phrase = val.trim_matches('"').to_string();
                        let words = phrase
                            .split_whitespace()
                            .map(|s| s.to_lowercase())
                            .collect();
                        query_terms.push(Term::FieldPhrase(name.to_string(), words));
                    } else {
                        query_terms.push(Term::Field(name.to_string(), val.to_lowercase()));
                    }
                } else {
                    query_terms.push(Term::General(part.to_lowercase()));
                }
            } else if part.starts_with('"') && part.ends_with('"') {
                let phrase = part.trim_matches('"').to_string();
                let words = phrase
                    .split_whitespace()
                    .map(|s| s.to_lowercase())
                    .collect();
                query_terms.push(Term::GeneralPhrase(words));
            } else {
                query_terms.push(Term::General(part.to_lowercase()));
            }
        }

        Ok(Query { terms: query_terms })
    }
}
