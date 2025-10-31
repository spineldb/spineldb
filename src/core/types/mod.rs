// src/core/types/mod.rs

use crate::core::SpinelDBError;
use bytes::Bytes;

pub type SpinelString = Bytes;

pub trait BytesExt {
    fn string_from_bytes(&self) -> Result<String, SpinelDBError>;
    fn to_uppercase_string(&self) -> String;
}

impl BytesExt for Bytes {
    fn string_from_bytes(&self) -> Result<String, SpinelDBError> {
        String::from_utf8(self.to_vec()).map_err(|_| SpinelDBError::WrongType)
    }

    fn to_uppercase_string(&self) -> String {
        String::from_utf8_lossy(self).to_uppercase()
    }
}
