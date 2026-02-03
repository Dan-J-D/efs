use anyhow::Result;

pub trait Cipher: Send + Sync {
    fn encrypt(
        &self,
        key: &[u8],
        ad: &[u8],
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, [u8; 12], [u8; 16])>;
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

pub mod standard;
