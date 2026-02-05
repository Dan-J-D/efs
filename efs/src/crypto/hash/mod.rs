use crate::crypto::Hasher;
use blake2::Blake2b512;
use sha2::{Digest, Sha256, Sha512};
use sha3::Sha3_256;

pub struct Blake3Hasher;

impl Hasher for Blake3Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        blake3::hash(data).as_bytes().to_vec()
    }
}

pub struct Sha3_256Hasher;

impl Hasher for Sha3_256Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha3_256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

pub struct Sha256Hasher;

impl Hasher for Sha256Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

pub struct Sha512Hasher;

impl Hasher for Sha512Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha512::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}

pub struct Blake2b512Hasher;

impl Hasher for Blake2b512Hasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        let mut hasher = Blake2b512::new();
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}
