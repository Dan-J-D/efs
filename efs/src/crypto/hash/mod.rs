use crate::crypto::Hasher;
use blake2::Blake2b512;
use sha2::{Digest, Sha256, Sha512};
use sha3::Sha3_256;

#[derive(Default)]
pub struct Blake3Hasher {}

impl Hasher for Blake3Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        blake3::hash(data).as_bytes().to_vec()
    }
}

#[derive(Default)]
pub struct Sha3_256Hasher {}

impl Hasher for Sha3_256Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha3_256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

#[derive(Default)]
pub struct Sha256Hasher {}

impl Hasher for Sha256Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

#[derive(Default)]
pub struct Sha512Hasher {}

impl Hasher for Sha512Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha512::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

#[derive(Default)]
pub struct Blake2b512Hasher {}

impl Hasher for Blake2b512Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Blake2b512::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

pub struct Blake2bHasher {
    pub output_size: usize,
}

impl Default for Blake2bHasher {
    fn default() -> Self {
        Self { output_size: 64 }
    }
}

impl Hasher for Blake2bHasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        use blake2::digest::{Update, VariableOutput};
        use blake2::Blake2bVar;
        let mut hasher = Blake2bVar::new(self.output_size).expect("Invalid Blake2b output size");
        hasher.update(data);
        let mut out = vec![0u8; self.output_size];
        hasher
            .finalize_variable(&mut out)
            .expect("Blake2b finalize failed");
        out
    }
}
