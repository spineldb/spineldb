use crate::core::commands::command_spec::CommandSpec;
use crate::core::commands::command_trait::{CommandFlags, ExecutableCommand, WriteOutcome};
use crate::core::search::query::{QueryParser, Term};
use crate::core::types::{BytesExt, SpinelString};
use crate::core::{RespValue, SpinelDBError};
use async_trait::async_trait;
use bytes::Bytes;
use ordered_float::OrderedFloat;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct FtSearchCommand {
    pub index_name: String,
    pub query: String,
    pub offset: usize,
    pub count: usize,
}

impl Default for FtSearchCommand {
    fn default() -> Self {
        Self {
            index_name: String::new(),
            query: String::new(),
            offset: 0,
            count: 10,
        }
    }
}

#[async_trait]
impl ExecutableCommand for FtSearchCommand {
    async fn execute<'a>(
        &self,
        ctx: &mut crate::core::storage::db::ExecutionContext<'a>,
    ) -> Result<(RespValue, WriteOutcome), SpinelDBError> {
        let search_index_arc = ctx
            .state
            .search_indexes
            .get(&self.index_name)
            .ok_or_else(|| SpinelDBError::Internal("Index does not exist".to_string()))?; // Changed to avoid information disclosure
        let index = search_index_arc.lock().await;

        let query = QueryParser::parse(&self.query)?;

        // Validate query terms against schema
        for term in &query.terms {
            match term {
                Term::Field(field_name, _)
                | Term::NumericRange(field_name, _, _)
                | Term::FieldPhrase(field_name, _) => {
                    if !index.schema.fields.contains_key(field_name) {
                        return Err(SpinelDBError::InvalidRequest(format!(
                            "Invalid field: {}",
                            field_name
                        )));
                    }
                }
                Term::General(_) | Term::GeneralPhrase(_) => {
                    // General terms are allowed to search across all text fields
                }
            }
        }

        let query_terms = query.terms.clone();

        let mut text_results: Option<HashSet<u64>> = None;
        let mut numeric_results: Option<HashSet<u64>> = None;
        let mut phrase_results: Option<HashSet<u64>> = None;

        // 1. Separate and process text and numeric terms
        for term in &query_terms {
            match term {
                Term::General(value) => {
                    let value_spinel: SpinelString = value.clone().into();
                    let current_term_docs = index
                        .inverted_indexes
                        .iter()
                        .filter_map(|(_, inverted_index)| inverted_index.get(&value_spinel))
                        .flat_map(|entry| entry.value().keys().cloned().collect::<HashSet<u64>>())
                        .collect::<HashSet<u64>>();

                    if let Some(results) = &mut text_results {
                        results.retain(|id| current_term_docs.contains(id));
                    } else {
                        text_results = Some(current_term_docs);
                    }
                }
                Term::Field(field_name, value) => {
                    // Verify the field exists in the schema before processing
                    if !index.schema.fields.contains_key(field_name) {
                        continue; // Skip invalid fields
                    }

                    let value_spinel: SpinelString = value.clone().into();
                    let current_term_docs = index
                        .inverted_indexes
                        .get(field_name)
                        .and_then(|inverted_index| inverted_index.get(&value_spinel))
                        .map(|entry| entry.value().keys().cloned().collect::<HashSet<u64>>())
                        .unwrap_or_default();

                    if let Some(results) = &mut text_results {
                        results.retain(|id| current_term_docs.contains(id));
                    } else {
                        text_results = Some(current_term_docs);
                    }
                }
                Term::NumericRange(field_name, min, max) => {
                    // Verify the field exists and is of correct type before processing
                    if let (Some(field_schema), Some(numeric_index)) = (
                        index.schema.fields.get(field_name),
                        index.numeric_indexes.get(field_name),
                    ) && field_schema.field_type
                        == crate::core::search::schema::FieldType::Numeric
                    {
                        let current_term_docs = numeric_index
                            .range(OrderedFloat(*min)..=OrderedFloat(*max))
                            .flat_map(|(_, ids)| ids.clone())
                            .collect::<HashSet<u64>>();

                        if let Some(results) = &mut numeric_results {
                            results.retain(|id| current_term_docs.contains(id));
                        } else {
                            numeric_results = Some(current_term_docs);
                        }
                    }
                }
                Term::GeneralPhrase(words) => {
                    let mut current_phrase_docs: Option<HashSet<u64>> = None;
                    for field_name in index.inverted_indexes.keys() {
                        // Only search text fields for general phrases
                        if let Some(field_schema) = index.schema.fields.get(field_name)
                            && field_schema.field_type
                                == crate::core::search::schema::FieldType::Text
                        {
                            let docs = Self::find_phrase_in_field(&index, field_name, words)
                                .unwrap_or_default();
                            if let Some(results) = &mut current_phrase_docs {
                                results.extend(docs);
                            } else {
                                current_phrase_docs = Some(docs);
                            }
                        }
                    }

                    if let Some(results) = &mut phrase_results {
                        results.retain(|id| {
                            current_phrase_docs
                                .as_ref()
                                .is_some_and(|cpd| cpd.contains(id))
                        });
                    } else {
                        phrase_results = current_phrase_docs;
                    }
                }
                Term::FieldPhrase(field_name, words) => {
                    // Verify the field exists before processing phrase search
                    if !index.schema.fields.contains_key(field_name) {
                        continue; // Skip invalid fields
                    }

                    let current_phrase_docs =
                        Self::find_phrase_in_field(&index, field_name, words).unwrap_or_default();

                    if let Some(results) = &mut phrase_results {
                        results.retain(|id| current_phrase_docs.contains(id));
                    } else {
                        phrase_results = Some(current_phrase_docs);
                    }
                }
            }
        }

        // 2. Combine results from all searches
        let mut final_ids: Option<HashSet<u64>> = None;
        let all_results = [text_results, numeric_results, phrase_results];
        for result_set in all_results.iter().flatten() {
            if let Some(final_set) = &mut final_ids {
                final_set.retain(|id| result_set.contains(id));
            } else {
                final_ids = Some(result_set.clone());
            }
        }
        let final_ids = final_ids.unwrap_or_default();

        // 3. Retrieve documents and format response
        // Add safety check for the number of results to prevent memory exhaustion
        let ids_to_process = final_ids.iter().skip(self.offset).take(self.count);
        let mut matching_docs = Vec::new();
        let max_docs_to_return = std::cmp::min(self.count, 1000); // Additional safety limit

        for (idx, id) in ids_to_process.enumerate() {
            if idx >= max_docs_to_return {
                break; // Safety limit reached
            }

            if let Some(doc) = index.documents.get(id) {
                matching_docs.push(RespValue::BulkString(doc.id.clone()));
                let mut hash_resp_array = Vec::new();
                for (field, value) in &doc.fields {
                    hash_resp_array.push(RespValue::BulkString(field.clone()));
                    hash_resp_array.push(RespValue::BulkString(value.clone()));
                }
                matching_docs.push(RespValue::Array(hash_resp_array));
            }
        }

        let mut result = vec![RespValue::Integer(matching_docs.len() as i64 / 2)];
        result.extend(matching_docs);

        Ok((RespValue::Array(result), WriteOutcome::DidNotWrite))
    }
}

// ... (rest of the file)

impl FtSearchCommand {
    // ... (existing methods)

    fn find_phrase_in_field(
        index: &crate::core::search::index::SearchIndex,
        field_name: &str,
        words: &[String],
    ) -> Result<HashSet<u64>, SpinelDBError> {
        if words.is_empty() {
            return Ok(HashSet::new());
        }

        let inverted_index = match index.inverted_indexes.get(field_name) {
            Some(ii) => ii,
            None => {
                return Ok(HashSet::new());
            }
        };

        // Get the first word's postings to start with
        let first_word = &words[0];
        let first_word_spinel: SpinelString = first_word.clone().into();
        let first_word_postings = match inverted_index.get(&first_word_spinel) {
            Some(postings) => postings,
            None => {
                // First word not in index, so phrase can't match
                return Ok(HashSet::new());
            }
        };

        let mut phrase_matching_docs = HashSet::new();

        // For each document that contains the first word
        for (doc_id, first_word_positions) in first_word_postings.value().iter() {
            // Check if subsequent words appear in sequence after the first word
            let mut found_phrase = false;

            'pos_loop: for &pos in first_word_positions {
                let mut current_pos = pos;
                let mut word_idx = 1; // Start checking from second word

                while word_idx < words.len() {
                    let next_word = &words[word_idx];
                    let next_word_spinel: SpinelString = next_word.clone().into();

                    // Get the next word's postings and access the underlying HashMap
                    if let Some(next_word_postings_ref) = inverted_index.get(&next_word_spinel) {
                        if let Some(next_word_positions) =
                            next_word_postings_ref.value().get(doc_id)
                        {
                            // Look for a position that is exactly current_pos + 1
                            let expected_pos = current_pos + 1;
                            let mut found_next_word_at_expected_pos = false;
                            for &pos_next_word in next_word_positions {
                                if pos_next_word == expected_pos {
                                    current_pos = pos_next_word;
                                    word_idx += 1;
                                    found_next_word_at_expected_pos = true;
                                    break;
                                }
                            }
                            if !found_next_word_at_expected_pos {
                                break;
                            }
                        } else {
                            // This document doesn't have the next word, so phrase doesn't match
                            break;
                        }
                    } else {
                        // Next word not in index, phrase can't match
                        break;
                    }
                }

                if word_idx == words.len() {
                    // All words found in sequence
                    found_phrase = true;
                    break 'pos_loop;
                }
            }

            if found_phrase {
                phrase_matching_docs.insert(*doc_id);
            }
        }

        Ok(phrase_matching_docs)
    }
}

impl FtSearchCommand {
    pub fn parse(args: &[SpinelString]) -> Result<Self, SpinelDBError> {
        if args.len() < 2 {
            return Err(SpinelDBError::WrongArgumentCount("FT.SEARCH".to_string()));
        }

        let index_name = args[0].string_from_bytes()?;
        let query = args[1].string_from_bytes()?;

        let mut offset = 0;
        let mut count = 10;

        let mut i = 2;
        while i < args.len() {
            let arg = args[i].string_from_bytes()?.to_ascii_lowercase();
            match arg.as_str() {
                "limit" => {
                    if i + 2 >= args.len() {
                        return Err(SpinelDBError::WrongArgumentCount("FT.SEARCH".to_string()));
                    }

                    // Parse and validate offset
                    let parsed_offset = args[i + 1]
                        .string_from_bytes()?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::SyntaxError)?;

                    // Add a reasonable upper limit to prevent resource exhaustion
                    if parsed_offset > 10_000_000 {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    offset = parsed_offset;

                    // Parse and validate count
                    let parsed_count = args[i + 2]
                        .string_from_bytes()?
                        .parse::<usize>()
                        .map_err(|_| SpinelDBError::SyntaxError)?;

                    // Add a reasonable upper limit to prevent resource exhaustion
                    if parsed_count > 1000 {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    count = parsed_count;

                    i += 3;
                }
                _ => {
                    return Err(SpinelDBError::SyntaxError);
                }
            }
        }

        Ok(Self {
            index_name,
            query,
            offset,
            count,
        })
    }
}

impl CommandSpec for FtSearchCommand {
    fn name(&self) -> &'static str {
        "ft.search"
    }

    fn arity(&self) -> i64 {
        -3 // FT.SEARCH index_name query ...
    }

    fn flags(&self) -> CommandFlags {
        CommandFlags::READONLY
    }

    fn first_key(&self) -> i64 {
        1
    }

    fn last_key(&self) -> i64 {
        1
    }

    fn step(&self) -> i64 {
        0
    }

    fn get_keys(&self) -> Vec<Bytes> {
        vec![] // This command doesn't have keys in the traditional sense
    }

    fn to_resp_args(&self) -> Vec<Bytes> {
        let mut args = vec![
            Bytes::from_static(b"SEARCH"),
            Bytes::from(self.index_name.clone()),
            Bytes::from(self.query.clone()),
        ];
        if self.offset != 0 || self.count != 10 {
            args.push(Bytes::from_static(b"LIMIT"));
            args.push(Bytes::from(self.offset.to_string()));
            args.push(Bytes::from(self.count.to_string()));
        }
        args
    }
}
