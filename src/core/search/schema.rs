// src/core/search/schema.rs

use crate::core::SpinelDBError;
use anyhow::Result;
use ordered_float::OrderedFloat;
use std::collections::{HashMap, HashSet};
use strum_macros::{Display, EnumString};
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumString, Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum FieldType {
    Text,
    Tag,
    Numeric,
    Geo,
    Vector,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Display)]
pub enum FieldOption {
    Sortable,
    NoIndex,
    Weight(OrderedFloat<f64>),
    WithSuffixTrie,
}

impl std::str::FromStr for FieldOption {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "SORTABLE" => Ok(FieldOption::Sortable),
            "NOINDEX" => Ok(FieldOption::NoIndex),
            "WEIGHT" => Err(()), // Special handling needed for Weight with value
            "WITHSUFFIXTRIE" => Ok(FieldOption::WithSuffixTrie),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub field_type: FieldType,
    pub options: HashSet<FieldOption>,
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub fields: HashMap<String, Field>,
}

impl Schema {
    /// Parses the schema definition from `FT.CREATE` arguments.
    ///
    /// The arguments should be in the format:
    /// `SCHEMA {field_name} {type} [OPTIONS...] ...`
    pub fn from_args(args: &[String]) -> Result<Self, SpinelDBError> {
        if args.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "FT.CREATE SCHEMA".to_string(),
            ));
        }

        let mut fields = HashMap::new();
        let mut i = 0;

        while i < args.len() {
            // Every field must have a name and a type.
            if i + 1 >= args.len() {
                return Err(SpinelDBError::SyntaxError);
            }

            let field_name = args[i].clone();
            let field_type_str = &args[i + 1];
            i += 2;

            let field_type = field_type_str
                .parse::<FieldType>()
                .map_err(|_| SpinelDBError::SyntaxError)?;

            let mut options = HashSet::new();
            while i < args.len() {
                let arg_upper = args[i].to_uppercase();
                if arg_upper == "WEIGHT" {
                    if i + 1 >= args.len() {
                        return Err(SpinelDBError::SyntaxError);
                    }
                    let weight_value = args[i + 1]
                        .parse::<f64>()
                        .map_err(|_| SpinelDBError::SyntaxError)?;
                    options.insert(FieldOption::Weight(OrderedFloat(weight_value)));
                    i += 2; // Skip both WEIGHT and its value
                } else if let Ok(option) = args[i].parse::<FieldOption>() {
                    options.insert(option);
                    i += 1;
                } else {
                    // This token is not an option, so it must be the start of the next field.
                    break;
                }
            }

            fields.insert(
                field_name.clone(),
                Field {
                    name: field_name,
                    field_type,
                    options,
                },
            );
        }

        if fields.is_empty() {
            return Err(SpinelDBError::WrongArgumentCount(
                "FT.CREATE SCHEMA".to_string(),
            ));
        }

        Ok(Schema { fields })
    }
}
