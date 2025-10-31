// src/core/search/schema.rs

use crate::core::SpinelDBError;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use strum_macros::{Display, EnumString};
#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumString, Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum FieldType {
    Text,
    Tag,
    Numeric,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumString, Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum FieldOption {
    Sortable,
    NoIndex,
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
                if let Ok(option) = args[i].parse::<FieldOption>() {
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
