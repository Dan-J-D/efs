use anyhow::{anyhow, Result};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks
pub const ENVELOPE_OVERHEAD: usize = 12 + 16 + 8; // nonce + tag + bincode vec len

#[derive(Serialize, Deserialize)]
pub struct UniformEnvelope {
    pub nonce: [u8; 12],
    pub tag: [u8; 16],
    pub ciphertext: Vec<u8>,
}

impl UniformEnvelope {
    pub fn new(nonce: [u8; 12], tag: [u8; 16], ciphertext: Vec<u8>) -> Self {
        Self {
            nonce,
            tag,
            ciphertext,
        }
    }

    pub fn payload_size(chunk_size: usize) -> usize {
        chunk_size - ENVELOPE_OVERHEAD
    }

    pub fn serialize(&self, chunk_size: usize) -> Result<Vec<u8>> {
        let bytes = bincode::serialize(self)?;
        if bytes.len() != chunk_size {
            return Err(anyhow!(
                "Serialized envelope size mismatch: {} != {}",
                bytes.len(),
                chunk_size
            ));
        }
        Ok(bytes)
    }

    pub fn deserialize(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
    }
}

pub struct Chunker {
    chunk_size: usize,
}

impl Chunker {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    pub fn pad(mut data: Vec<u8>, target_size: usize) -> Vec<u8> {
        if data.len() < target_size {
            let mut padding = vec![0u8; target_size - data.len()];
            thread_rng().fill_bytes(&mut padding);
            data.extend_from_slice(&padding);
        }
        data
    }

    pub fn split(&self, data: &[u8]) -> Vec<Vec<u8>> {
        let payload_size = UniformEnvelope::payload_size(self.chunk_size);
        data.chunks(payload_size).map(|c| c.to_vec()).collect()
    }
}
