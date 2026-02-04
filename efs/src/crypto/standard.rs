use super::{Cipher, Hasher, Kdf};
use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Result};
use argon2::Argon2;
use blake3;
use rand::{thread_rng, RngCore};

pub struct StandardCipher;

impl Cipher for StandardCipher {
    fn encrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, [u8; 12], [u8; 16])> {
        let cipher =
            Aes256Gcm::new_from_slice(key).map_err(|e| anyhow!("Invalid key length: {}", e))?;

        let mut nonce_bytes = [0u8; 12];
        thread_rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Hash AD with key to prevent metadata leaks in AEAD implementation or side channels
        let mut hasher = blake3::Hasher::new();
        hasher.update(key);
        hasher.update(ad);
        let hashed_ad = hasher.finalize();

        let payload = Payload {
            msg: plaintext,
            aad: hashed_ad.as_bytes(),
        };

        let ciphertext = cipher
            .encrypt(nonce, payload)
            .map_err(|e| anyhow!("Encryption failure: {}", e))?;

        // aes-gcm crate returns ciphertext + tag appended. We need to separate them if we want to follow the trait exactly.
        // Actually, the trait says (Vec<u8>, [u8; 12], [u8; 16]) -> (ciphertext, nonce, tag)
        let tag_pos = ciphertext.len() - 16;
        let actual_ciphertext = ciphertext[..tag_pos].to_vec();
        let mut tag_bytes = [0u8; 16];
        tag_bytes.copy_from_slice(&ciphertext[tag_pos..]);

        Ok((actual_ciphertext, nonce_bytes, tag_bytes))
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

        // Re-hash AD with key
        let mut hasher = blake3::Hasher::new();
        hasher.update(key);
        hasher.update(ad);
        let hashed_ad = hasher.finalize();

        let mut full_ciphertext = ciphertext.to_vec();
        full_ciphertext.extend_from_slice(tag);

        let payload = Payload {
            msg: &full_ciphertext,
            aad: hashed_ad.as_bytes(),
        };

        cipher
            .decrypt(nonce, payload)
            .map_err(|e| anyhow!("Decryption failure: {}", e))
    }
}

pub struct StandardKdf;

impl Kdf for StandardKdf {
    fn derive(&self, password: &[u8], salt: &[u8], output: &mut [u8]) -> Result<()> {
        let argon2 = Argon2::default();
        argon2
            .hash_password_into(password, salt, output)
            .map_err(|e| anyhow!("KDF failure: {}", e))
    }
}

pub struct StandardHasher;

impl Hasher for StandardHasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        blake3::hash(data).as_bytes().to_vec()
    }
}
