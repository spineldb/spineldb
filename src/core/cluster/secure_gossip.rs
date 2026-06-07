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
        let signature: Signature = (&signature_bytes[..])
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meet() -> GossipMessage {
        GossipMessage::Meet { timestamp_ms: 1000 }
    }

    #[test]
    fn test_no_password_produces_zero_signature() {
        let msg = make_meet();
        let signed = SecureGossipMessage::new(msg, &None).unwrap();
        assert_eq!(signed.signature, [0u8; 32]);
    }

    #[test]
    fn test_no_password_verifies_zero_signature() {
        let msg = make_meet();
        let signed = SecureGossipMessage::new(msg, &None).unwrap();
        assert!(signed.verify(&None).unwrap());
    }

    #[test]
    fn test_no_password_rejects_nonzero_signature() {
        let msg = make_meet();
        // Tamper with the signature.
        let mut signed = SecureGossipMessage::new(msg, &None).unwrap();
        signed.signature[0] = 1;
        assert!(!signed.verify(&None).unwrap());
    }

    #[test]
    fn test_password_produces_nonzero_signature() {
        let msg = make_meet();
        let signed = SecureGossipMessage::new(msg, &Some("secret".to_string())).unwrap();
        assert_ne!(signed.signature, [0u8; 32]);
    }

    #[test]
    fn test_password_verifies_correct_signature() {
        let msg = make_meet();
        let pass = Some("secret".to_string());
        let signed = SecureGossipMessage::new(msg, &pass).unwrap();
        assert!(signed.verify(&pass).unwrap());
    }

    #[test]
    fn test_password_rejects_tampered_signature() {
        let msg = make_meet();
        let pass = Some("secret".to_string());
        let mut signed = SecureGossipMessage::new(msg, &pass).unwrap();
        signed.signature[0] ^= 0xFF;
        assert!(!signed.verify(&pass).unwrap());
    }

    #[test]
    fn test_password_rejects_tampered_message() {
        let msg1 = GossipMessage::Meet { timestamp_ms: 1000 };
        let pass = Some("secret".to_string());
        let signed = SecureGossipMessage::new(msg1, &pass).unwrap();
        // Build a wrapper with a different message but the original signature.
        let msg2 = GossipMessage::Meet { timestamp_ms: 9999 };
        let tampered = SecureGossipMessage {
            message: msg2,
            signature: signed.signature,
        };
        assert!(!tampered.verify(&pass).unwrap());
    }

    #[test]
    fn test_password_rejects_wrong_password() {
        let msg = make_meet();
        let signed = SecureGossipMessage::new(msg, &Some("secret".to_string())).unwrap();
        assert!(!signed.verify(&Some("different".to_string())).unwrap());
    }

    #[test]
    fn test_password_rejects_zero_signature() {
        // Build a message with a zero signature; verification should fail
        // because a password was configured but the signature is zero.
        let msg = make_meet();
        let tampered = SecureGossipMessage {
            message: msg,
            signature: [0u8; 32],
        };
        let pass = Some("secret".to_string());
        assert!(!tampered.verify(&pass).unwrap());
    }
}
