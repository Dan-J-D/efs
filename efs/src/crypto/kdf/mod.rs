use crate::crypto::Kdf;
use anyhow::{anyhow, Result};
use argon2::Argon2;
use hmac::Hmac;
use pbkdf2::pbkdf2;
use sha2::Sha256;

pub struct Argon2Kdf {
    pub m_cost: u32,
    pub t_cost: u32,
    pub p_cost: u32,
}

impl Default for Argon2Kdf {
    fn default() -> Self {
        Self {
            m_cost: 65536, // 64MB
            t_cost: 3,
            p_cost: 4,
        }
    }
}

impl Kdf for Argon2Kdf {
    fn derive(&self, password: &[u8], salt: &[u8], output: &mut [u8]) -> Result<()> {
        let params = argon2::ParamsBuilder::new()
            .m_cost(self.m_cost)
            .t_cost(self.t_cost)
            .p_cost(self.p_cost)
            .build()
            .map_err(|e| anyhow!("Argon2 params failure: {}", e))?;
        let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
        argon2
            .hash_password_into(password, salt, output)
            .map_err(|e| anyhow!("KDF failure: {}", e))
    }
}

pub struct Pbkdf2Sha256Kdf {
    pub iterations: u32,
}

impl Default for Pbkdf2Sha256Kdf {
    fn default() -> Self {
        Self {
            iterations: 600_000,
        }
    }
}

impl Kdf for Pbkdf2Sha256Kdf {
    fn derive(&self, password: &[u8], salt: &[u8], output: &mut [u8]) -> Result<()> {
        pbkdf2::<Hmac<Sha256>>(password, salt, self.iterations, output)
            .map_err(|e| anyhow!("PBKDF2 failure: {}", e))
    }
}
