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
    Not(Box<Term>),                   // negation
    Prefix(String, String),           // field_name, prefix_value
    Suffix(String, String),           // field_name, suffix_value
    Fuzzy(String, String, u8),        // field_name, value, max_edit_distance
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
                Term::Not(inner_term) => !Self::term_matches(inner_term, doc_fields, schema),
                Term::Prefix(field_name, prefix) => doc_fields
                    .get(field_name)
                    .is_some_and(|doc_val| doc_val.starts_with(prefix)),
                Term::Suffix(field_name, suffix) => doc_fields
                    .get(field_name)
                    .is_some_and(|doc_val| doc_val.ends_with(suffix)),
                Term::Fuzzy(field_name, value, max_edit_distance) => {
                    if let Some(doc_val) = doc_fields.get(field_name) {
                        levenshtein_distance(value, doc_val) <= *max_edit_distance as usize
                    } else {
                        false
                    }
                }
            };
            if !found_match {
                return false;
            }
        }
        true
    }

    fn term_matches(term: &Term, doc_fields: &HashMap<String, String>, schema: &Schema) -> bool {
        match term {
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
            Term::Not(inner_term) => !Self::term_matches(inner_term, doc_fields, schema),
            Term::Prefix(field_name, prefix) => doc_fields
                .get(field_name)
                .is_some_and(|doc_val| doc_val.starts_with(prefix)),
            Term::Suffix(field_name, suffix) => doc_fields
                .get(field_name)
                .is_some_and(|doc_val| doc_val.ends_with(suffix)),
            Term::Fuzzy(field_name, value, max_edit_distance) => {
                if let Some(doc_val) = doc_fields.get(field_name) {
                    levenshtein_distance(value, doc_val) <= *max_edit_distance as usize
                } else {
                    false
                }
            }
        }
    }
}

// Calculate the Levenshtein distance (edit distance) between two strings
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let len1 = s1_chars.len();
    let len2 = s2_chars.len();

    if len1 == 0 {
        return len2;
    }
    if len2 == 0 {
        return len1;
    }

    let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

    for (i, row) in matrix.iter_mut().enumerate().take(len1 + 1) {
        row[0] = i;
    }
    for j in 0..=len2 {
        matrix[0][j] = j;
    }

    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] {
                0
            } else {
                1
            };
            matrix[i][j] = std::cmp::min(
                std::cmp::min(matrix[i - 1][j] + 1, matrix[i][j - 1] + 1),
                matrix[i - 1][j - 1] + cost,
            );
        }
    }

    matrix[len1][len2]
}

pub struct QueryParser;

impl QueryParser {
    pub fn parse(query_str: &str) -> Result<Query, SpinelDBError> {
        let mut terms = Vec::new();
        let mut current_term = String::new();
        let mut in_quotes = false;
        let mut escape_next = false;

        for c in query_str.chars() {
            if escape_next {
                current_term.push(c);
                escape_next = false;
                continue;
            }

            match c {
                '\\' => {
                    escape_next = true;
                }
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
            if part.starts_with('-') && part.len() > 1 {
                // Negation operator
                let negated_part = &part[1..];
                let negated_term = Self::parse_single_term(negated_part)?;
                query_terms.push(Term::Not(Box::new(negated_term)));
            } else if part.starts_with('@') {
                let mut field_split = part.splitn(2, ':');
                let field_name = field_split.next().map(|s| s.trim_start_matches('@'));
                let value = field_split.next();

                if let (Some(name), Some(val)) = (field_name, value) {
                    // Handle prefix/suffix matching using special syntax
                    if val.starts_with('*') && val.len() > 1 {
                        // Suffix matching: @field:*suffix
                        let suffix = &val[1..];
                        query_terms.push(Term::Suffix(name.to_string(), suffix.to_lowercase()));
                    } else if val.ends_with('*') && val.len() > 1 {
                        // Prefix matching: @field:prefix*
                        let prefix = val.trim_end_matches('*');
                        query_terms.push(Term::Prefix(name.to_string(), prefix.to_lowercase()));
                    } else if val.starts_with('[') && val.ends_with(']') {
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

    // Helper function to parse a single term (used for negation)
    fn parse_single_term(term_str: &str) -> Result<Term, SpinelDBError> {
        let mut query = Self::parse(term_str)?;
        if query.terms.len() == 1 {
            Ok(query.terms.remove(0))
        } else {
            // For multiple terms, wrap them in a general search
            Ok(Term::General(term_str.to_lowercase()))
        }
    }
}
