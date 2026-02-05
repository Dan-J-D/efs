use anyhow::{anyhow, Result};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};

pub const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024; // 1MB chunks

#[derive(Serialize, Deserialize)]
pub struct UniformEnvelope {
    pub nonce: Vec<u8>,
    pub tag: Vec<u8>,
    pub ciphertext: Vec<u8>,
}

impl UniformEnvelope {
    pub fn new(nonce: Vec<u8>, tag: Vec<u8>, ciphertext: Vec<u8>) -> Self {
        Self {
            nonce,
            tag,
            ciphertext,
        }
    }

    pub fn payload_size(chunk_size: usize, nonce_size: usize, tag_size: usize) -> usize {
        let dummy_empty = Self {
            nonce: vec![0u8; nonce_size],
            tag: vec![0u8; tag_size],
            ciphertext: vec![],
        };
        let empty_size = bincode::serialized_size(&dummy_empty).unwrap() as usize;

        // Base struct size is the size of the envelope with an empty ciphertext,
        // minus the size of the empty vector's length encoding.
        // For bincode 1.x, serialized_size(vec![0; p]) = base_struct_size + serialized_size(len(p)) + p.
        let empty_vec: Vec<u8> = vec![];
        let empty_vec_len_size = bincode::serialized_size(&empty_vec).unwrap() as usize;
        let base_struct_size = empty_size - empty_vec_len_size;

        // Binary search to find the maximum payload size p such that:
        // base_struct_size + serialized_size(len(p)) + p <= chunk_size
        let mut low = 0;
        let mut high = chunk_size.saturating_sub(base_struct_size);
        let mut best_p = 0;

        while low <= high {
            let mid = low + (high - low) / 2;
            // bincode serializes Vec length as a u64.
            let len_len = bincode::serialized_size(&(mid as u64)).unwrap() as usize;
            let total = base_struct_size + len_len + mid;

            if total <= chunk_size {
                best_p = mid;
                low = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            }
        }
        best_p
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_envelope_size() {
        // Test various chunk sizes to ensure the payload calculation is correct
        // and robust against potential bincode configuration changes.
        let nonce_size = 12;
        let tag_size = 16;
        for chunk_size in [128, 1024, 1024 * 1024] {
            let p_size = UniformEnvelope::payload_size(chunk_size, nonce_size, tag_size);
            let env = UniformEnvelope::new(
                vec![0u8; nonce_size],
                vec![0u8; tag_size],
                vec![0u8; p_size],
            );
            let serialized = env.serialize(chunk_size).unwrap();
            assert_eq!(serialized.len(), chunk_size);

            // Verify that p_size + 1 would exceed chunk_size
            let env_too_big = UniformEnvelope::new(
                vec![0u8; nonce_size],
                vec![0u8; tag_size],
                vec![0u8; p_size + 1],
            );
            let serialized_too_big = bincode::serialize(&env_too_big).unwrap();
            assert!(serialized_too_big.len() > chunk_size);
        }
    }
}

pub struct Chunker {
    chunk_size: usize,
    nonce_size: usize,
    tag_size: usize,
}

impl Chunker {
    pub fn new(chunk_size: usize, nonce_size: usize, tag_size: usize) -> Self {
        Self {
            chunk_size,
            nonce_size,
            tag_size,
        }
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
        let payload_size =
            UniformEnvelope::payload_size(self.chunk_size, self.nonce_size, self.tag_size);
        data.chunks(payload_size).map(|c| c.to_vec()).collect()
    }
}
