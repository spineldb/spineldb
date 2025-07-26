// src/core/cluster/secure_gossip.rs

use crate::core::cluster::gossip::GossipMessage;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

// Tipe alias untuk HMAC-SHA256
type HmacSha256 = Hmac<Sha256>;
pub type Signature = [u8; 32]; // SHA256 menghasilkan 32 byte

/// Wrapper untuk pesan gossip yang menyertakan signature.
/// Ini adalah struktur yang sebenarnya dikirim melalui UDP.
#[derive(Serialize, Deserialize, bincode::Encode, bincode::Decode, Debug, Clone)]
pub struct SecureGossipMessage {
    pub message: GossipMessage,
    pub signature: Signature,
}

impl SecureGossipMessage {
    /// Membuat pesan aman baru dengan menandatangani pesan gossip.
    pub fn new(message: GossipMessage, password: &Option<String>) -> Result<Self, &'static str> {
        // Jika tidak ada password, signature adalah nol.
        let Some(pass) = password else {
            return Ok(Self {
                message,
                signature: [0u8; 32],
            });
        };

        let bincode_config = bincode::config::standard();
        let message_bytes = bincode::encode_to_vec(&message, bincode_config)
            .map_err(|_| "Failed to encode gossip message for signing")?;

        let mut mac = HmacSha256::new_from_slice(pass.as_bytes())
            .map_err(|_| "Failed to create HMAC instance")?;
        mac.update(&message_bytes);

        let signature_bytes = mac.finalize().into_bytes();
        let signature: Signature = signature_bytes
            .as_slice()
            .try_into()
            .map_err(|_| "Invalid signature length")?;

        Ok(Self { message, signature })
    }

    /// Memverifikasi signature dari pesan yang diterima.
    pub fn verify(&self, password: &Option<String>) -> Result<bool, &'static str> {
        // Jika tidak ada password, kita anggap valid jika signature-nya nol.
        let Some(pass) = password else {
            return Ok(self.signature == [0u8; 32]);
        };

        // Jika password ada, signature tidak boleh nol.
        if self.signature == [0u8; 32] {
            return Ok(false);
        }

        let bincode_config = bincode::config::standard();
        let message_bytes = bincode::encode_to_vec(&self.message, bincode_config)
            .map_err(|_| "Failed to encode received gossip message for verification")?;

        let mut mac = HmacSha256::new_from_slice(pass.as_bytes())
            .map_err(|_| "Failed to create HMAC instance for verification")?;
        mac.update(&message_bytes);

        Ok(mac.verify_slice(&self.signature).is_ok())
    }
}
