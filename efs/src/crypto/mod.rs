use anyhow::Result;

pub trait Cipher: Send + Sync {
    fn nonce_size(&self) -> usize;
    fn tag_size(&self) -> usize;
    fn encrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)>;
    fn decrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        nonce: &[u8],
        tag: &[u8],
        ciphertext: &[u8],
    ) -> Result<Vec<u8>>;
}

pub trait Kdf: Send + Sync {
    fn derive(&self, password: &[u8], salt: &[u8], output: &mut [u8]) -> Result<()>;
}

pub trait Hasher: Send + Sync {
    fn hash(&self, data: &[u8]) -> Vec<u8>;
}

pub mod cipher;
pub mod hash;
pub mod kdf;

pub use cipher::*;
pub use hash::*;
pub use kdf::*;
