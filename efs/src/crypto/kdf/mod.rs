use crate::crypto::Kdf;
use anyhow::{anyhow, Result};
use argon2::Argon2;
use hmac::Hmac;
use pbkdf2::pbkdf2;
use sha2::Sha256;

pub struct Argon2Kdf;

impl Kdf for Argon2Kdf {
    fn derive(&self, password: &[u8], salt: &[u8], output: &mut [u8]) -> Result<()> {
        let argon2 = Argon2::default();
        argon2
            .hash_password_into(password, salt, output)
            .map_err(|e| anyhow!("KDF failure: {}", e))
    }
}

pub struct Pbkdf2Sha256Kdf;

impl Kdf for Pbkdf2Sha256Kdf {
    fn derive(&self, password: &[u8], salt: &[u8], output: &mut [u8]) -> Result<()> {
        pbkdf2::<Hmac<Sha256>>(password, salt, 100_000, output)
            .map_err(|e| anyhow!("PBKDF2 failure: {}", e))
    }
}
