use crate::crypto::Cipher;
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Result};
use chacha20poly1305::{ChaCha20Poly1305, Nonce as ChaChaNonce, XChaCha20Poly1305, XNonce};
use rand::{thread_rng, RngCore};

#[derive(Default)]
pub struct Aes256GcmCipher {}

impl Cipher for Aes256GcmCipher {
    fn name(&self) -> &'static str {
        "aes-256-gcm"
    }

    fn nonce_size(&self) -> usize {
        12
    }

    fn tag_size(&self) -> usize {
        16
    }

    fn encrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let cipher =
            Aes256Gcm::new_from_slice(key).map_err(|e| anyhow!("Invalid key length: {}", e))?;

        let mut nonce_bytes = [0u8; 12];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let payload = Payload {
            msg: plaintext,
            aad: ad,
        };

        let ciphertext = cipher
            .encrypt(nonce, payload)
            .map_err(|e| anyhow!("Encryption failure: {}", e))?;

        let tag_pos = ciphertext.len() - 16;
        let actual_ciphertext = ciphertext[..tag_pos].to_vec();
        let tag_bytes = ciphertext[tag_pos..].to_vec();

        Ok((actual_ciphertext, nonce_bytes.to_vec(), tag_bytes))
    }

    fn decrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        nonce: &[u8],
        tag: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        let cipher =
            Aes256Gcm::new_from_slice(key).map_err(|e| anyhow!("Invalid key length: {}", e))?;

        let nonce = Nonce::from_slice(nonce);

        let mut full_ciphertext = ciphertext.to_vec();
        full_ciphertext.extend_from_slice(tag);

        let payload = Payload {
            msg: &full_ciphertext,
            aad: ad,
        };

        cipher
            .decrypt(nonce, payload)
            .map_err(|e| anyhow!("Decryption failure: {}", e))
    }
}

#[derive(Default)]
pub struct ChaCha20Poly1305Cipher {}

impl Cipher for ChaCha20Poly1305Cipher {
    fn name(&self) -> &'static str {
        "chacha20-poly1305"
    }

    fn nonce_size(&self) -> usize {
        12
    }

    fn tag_size(&self) -> usize {
        16
    }

    fn encrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let cipher = ChaCha20Poly1305::new_from_slice(key)
            .map_err(|e| anyhow!("Invalid key length: {}", e))?;

        let mut nonce_bytes = [0u8; 12];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = ChaChaNonce::from_slice(&nonce_bytes);

        let payload = Payload {
            msg: plaintext,
            aad: ad,
        };

        let ciphertext = cipher
            .encrypt(nonce, payload)
            .map_err(|e| anyhow!("Encryption failure: {}", e))?;

        let tag_pos = ciphertext.len() - 16;
        let actual_ciphertext = ciphertext[..tag_pos].to_vec();
        let tag_bytes = ciphertext[tag_pos..].to_vec();

        Ok((actual_ciphertext, nonce_bytes.to_vec(), tag_bytes))
    }

    fn decrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        nonce: &[u8],
        tag: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        let cipher = ChaCha20Poly1305::new_from_slice(key)
            .map_err(|e| anyhow!("Invalid key length: {}", e))?;

        let nonce = ChaChaNonce::from_slice(nonce);

        let mut full_ciphertext = ciphertext.to_vec();
        full_ciphertext.extend_from_slice(tag);

        let payload = Payload {
            msg: &full_ciphertext,
            aad: ad,
        };

        cipher
            .decrypt(nonce, payload)
            .map_err(|e| anyhow!("Decryption failure: {}", e))
    }
}

#[derive(Default)]
pub struct XChaCha20Poly1305Cipher {}

impl Cipher for XChaCha20Poly1305Cipher {
    fn name(&self) -> &'static str {
        "xchacha20-poly1305"
    }

    fn nonce_size(&self) -> usize {
        24
    }

    fn tag_size(&self) -> usize {
        16
    }

    fn encrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
        let cipher = XChaCha20Poly1305::new_from_slice(key)
            .map_err(|e| anyhow!("Invalid key length: {}", e))?;

        let mut nonce_bytes = [0u8; 24];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = XNonce::from_slice(&nonce_bytes);

        let payload = Payload {
            msg: plaintext,
            aad: ad,
        };

        let ciphertext = cipher
            .encrypt(nonce, payload)
            .map_err(|e| anyhow!("Encryption failure: {}", e))?;

        let tag_pos = ciphertext.len() - 16;
        let actual_ciphertext = ciphertext[..tag_pos].to_vec();
        let tag_bytes = ciphertext[tag_pos..].to_vec();

        Ok((actual_ciphertext, nonce_bytes.to_vec(), tag_bytes))
    }

    fn decrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        nonce: &[u8],
        tag: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>> {
        let cipher = XChaCha20Poly1305::new_from_slice(key)
            .map_err(|e| anyhow!("Invalid key length: {}", e))?;

        let nonce = XNonce::from_slice(nonce);

        let mut full_ciphertext = ciphertext.to_vec();
        full_ciphertext.extend_from_slice(tag);

        let payload = Payload {
            msg: &full_ciphertext,
            aad: ad,
        };

        cipher
            .decrypt(nonce, payload)
            .map_err(|e| anyhow!("Decryption failure: {}", e))
    }
}
