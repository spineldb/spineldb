// src/core/commands/json/helpers.rs

//! Contains shared logic for parsing JSON paths and manipulating serde_json::Value.

use crate::core::SpinelDBError;
use jsonpath_lib::select as find_values_with_jsonpath;
use serde_json::{Map, Number, Value};
use std::str::FromStr;

/// Represents a single segment of a simple JSON path, e.g., an object key or an array index.
#[derive(Debug)]
pub enum PathSegment {
    Key(String),
    Index(usize),
}

/// Parses a simple JSONPath string (e.g., "root.user[0]") into segments.
/// This simplified parser is used for write operations (SET, DEL, etc.) that require a single, concrete path.
pub fn parse_path(path_str: &str) -> Result<Vec<PathSegment>, SpinelDBError> {
    if path_str.is_empty() {
        return Err(SpinelDBError::SyntaxError);
    }

    let mut segments = Vec::new();
    let mut current = path_str;

    // Handle leading '$' or '.' as root indicator
    if current.starts_with('$') || current.starts_with('.') {
        current = &current[1..]; // Skip the '$' or '.'
    }

    // If after skipping, it's empty, it means the path was just "$" or "."
    if current.is_empty() {
        return Ok(vec![]); // Represents the root
    }

    // Now parse the rest of the path
    while !current.is_empty() {
        if current.starts_with('.') {
            let remainder = &current[1..];
            let end = remainder.find(['.', '[']).unwrap_or(remainder.len());
            if end == 0 {
                return Err(SpinelDBError::SyntaxError);
            }
            segments.push(PathSegment::Key(remainder[..end].to_string()));
            current = &remainder[end..];
        } else if current.starts_with('[') {
            let remainder = &current[1..];
            let Some(end) = remainder.find(']') else {
                return Err(SpinelDBError::SyntaxError);
            };
            let index =
                usize::from_str(&remainder[..end]).map_err(|_| SpinelDBError::SyntaxError)?;
            segments.push(PathSegment::Index(index));
            current = &remainder[end + 1..];
        } else {
            // This case handles the very first segment if it doesn't start with '.' or '['
            // e.g., "key.nested" or "key[0]"
            let end = current.find(['.', '[']).unwrap_or(current.len());
            segments.push(PathSegment::Key(current[..end].to_string()));
            current = &current[end..];
        }
    }
    Ok(segments)
}

/// Recursively traverses a JSON Value to find a mutable reference at a specific path and applies an operation.
pub fn find_and_modify<F>(
    root: &mut Value,
    path: &[PathSegment],
    op: F,
    create_if_not_exist: bool,
) -> Result<Value, SpinelDBError>
where
    F: FnOnce(&mut Value) -> Result<Value, SpinelDBError>,
{
    if path.is_empty() {
        return op(root);
    }

    if !create_if_not_exist {
        let mut current = root;
        let (last_segment, parent_path) = path.split_last().unwrap();

        for segment in parent_path {
            current = match segment {
                PathSegment::Key(key) => current.get_mut(key),
                PathSegment::Index(index) => current.get_mut(*index),
            }
            .ok_or_else(|| SpinelDBError::InvalidState("path does not exist".to_string()))?;
        }

        let target = match last_segment {
            PathSegment::Key(key) => current.get_mut(key),
            PathSegment::Index(index) => current.get_mut(*index),
        }
        .ok_or_else(|| SpinelDBError::InvalidState("path does not exist".to_string()))?;

        return op(target);
    }

    // Path creation logic (for SET)
    let mut current = root;
    for (i, segment) in path.iter().enumerate() {
        let is_last = i == path.len() - 1;
        match segment {
            PathSegment::Key(key) => {
                if !current.is_object() {
                    if current.is_null() {
                        *current = Value::Object(Map::new());
                    } else {
                        return Err(SpinelDBError::InvalidState(
                            "Path segment is not an object".into(),
                        ));
                    }
                }
                let map = current.as_object_mut().unwrap();
                if is_last {
                    return op(map.entry(key).or_insert(Value::Null));
                }
                current = map.entry(key.clone()).or_insert_with(|| {
                    if let Some(PathSegment::Index(_)) = path.get(i + 1) {
                        Value::Array(vec![])
                    } else {
                        Value::Object(Map::new())
                    }
                });
            }
            PathSegment::Index(index) => {
                if !current.is_array() {
                    if current.is_null() {
                        *current = Value::Array(vec![]);
                    } else {
                        return Err(SpinelDBError::InvalidState(
                            "Path segment is not an array".into(),
                        ));
                    }
                }
                let arr = current.as_array_mut().unwrap();

                if *index >= arr.len() {
                    arr.resize(*index + 1, Value::Null);
                }

                if is_last {
                    return op(&mut arr[*index]);
                }
                current = &mut arr[*index];
            }
        }
    }

    unreachable!();
}

/// Finds and removes a value at a specific path, returning the removed value.
pub fn find_and_remove(root: &mut Value, path: &[PathSegment]) -> Result<Value, SpinelDBError> {
    if path.is_empty() {
        return Err(SpinelDBError::InvalidState(
            "Cannot delete root object".into(),
        ));
    }

    let mut current = root;
    let (last_segment, parent_path) = path.split_last().unwrap();

    for segment in parent_path {
        current = match segment {
            PathSegment::Key(key) => current.get_mut(key).ok_or(SpinelDBError::KeyNotFound)?,
            PathSegment::Index(index) => {
                current.get_mut(*index).ok_or(SpinelDBError::KeyNotFound)?
            }
        };
    }

    let removed = match (last_segment, current) {
        (PathSegment::Key(key), Value::Object(map)) => map.remove(key).unwrap_or(Value::Null),
        (PathSegment::Index(index), Value::Array(arr)) if *index < arr.len() => arr.remove(*index),
        _ => Value::Null,
    };
    Ok(removed)
}

/// Finds a single value in a JSON document using a simple, non-wildcard path.
/// Used for commands that expect a single target, like `ARRLEN`.
pub fn find_value_by_segments<'a>(root: &'a Value, path: &[PathSegment]) -> Option<&'a Value> {
    let mut current = root;
    for segment in path {
        current = match segment {
            PathSegment::Key(key) => current.as_object()?.get(key)?,
            PathSegment::Index(index) => current.as_array()?.get(*index)?,
        };
    }
    Some(current)
}

/// Finds values in a JSON document using a full JSONPath expression.
/// Used for read-only query commands like `JSON.GET`.
pub fn find_values_by_jsonpath<'a>(
    root: &'a Value,
    path_str: &str,
) -> Result<Vec<&'a Value>, SpinelDBError> {
    let final_path = if path_str == "." {
        "$".to_string()
    } else if path_str.starts_with('.') {
        format!("${path_str}")
    } else {
        path_str.to_string()
    };

    find_values_with_jsonpath(root, &final_path).map_err(|e| {
        // Sanitize the error message to prevent protocol errors
        let sanitized_error = e.to_string().replace('\n', " ");
        SpinelDBError::InvalidState(format!("Invalid JSONPath: {sanitized_error}"))
    })
}

/// Formats a serde_json::Number to a string, omitting trailing `.0` for whole numbers.
pub fn format_json_number(num: &Number) -> String {
    if num.is_f64() && num.as_f64().unwrap().fract() == 0.0 {
        num.as_i64()
            .unwrap_or_else(|| num.as_f64().unwrap() as i64)
            .to_string()
    } else {
        num.to_string()
    }
}

/// Recursively estimates the memory usage of a `serde_json::Value` without serialization.
pub fn estimate_json_memory(val: &serde_json::Value) -> usize {
    use serde_json::Value;
    match val {
        Value::Null | Value::Bool(_) => std::mem::size_of::<Value>(),
        Value::Number(n) => std::mem::size_of::<Value>() + n.to_string().len(),
        Value::String(s) => std::mem::size_of::<Value>() + s.capacity(),
        Value::Array(arr) => {
            std::mem::size_of::<Value>()
                + arr.capacity() * std::mem::size_of::<Value>()
                + arr.iter().map(estimate_json_memory).sum::<usize>()
        }
        Value::Object(map) => {
            std::mem::size_of::<Value>()
                + map
                    .iter()
                    .map(|(k, v)| k.capacity() + estimate_json_memory(v))
                    .sum::<usize>()
        }
    }
}
