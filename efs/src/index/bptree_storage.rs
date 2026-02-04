use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::Cipher;
use crate::storage::{RegionId, StorageBackend, ALLOCATOR_STATE_BLOCK_ID, METADATA_REGION_ID};
use anyhow::Result;
use blake3;
use bptree::storage::{BlockId, BlockStorage, StorageError};
use hex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct BPTreeStorage {
    backend: Arc<dyn StorageBackend>,
    cipher: Arc<dyn Cipher>,
    key: Vec<u8>,
    next_id: Arc<AtomicU64>,
    chunk_size: usize,
    region_id: RegionId,
}

impl BPTreeStorage {
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        cipher: Arc<dyn Cipher>,
        key: Vec<u8>,
        next_id: Arc<AtomicU64>,
        chunk_size: usize,
        region_id: RegionId,
    ) -> Self {
        Self {
            backend,
            cipher,
            key,
            next_id,
            chunk_size,
            region_id,
        }
    }

    pub fn with_region(&self, region_id: RegionId) -> Self {
        let mut cloned = self.clone();
        cloned.region_id = region_id;
        cloned
    }

    pub fn next_id(&self) -> Arc<AtomicU64> {
        self.next_id.clone()
    }

    pub fn key(&self) -> Vec<u8> {
        self.key.clone()
    }

    fn block_name(&self, id: BlockId) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.key);
        hasher.update(&self.region_id.to_le_bytes());
        hasher.update(&id.to_le_bytes());
        let hash = hasher.finalize();
        hex::encode(hash.as_bytes())
    }

    pub fn persist_next_id(&mut self, next_id: u64) -> Result<(), StorageError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.key);
        hasher.update(&METADATA_REGION_ID.to_le_bytes());
        hasher.update(&ALLOCATOR_STATE_BLOCK_ID.to_le_bytes());
        let hash = hasher.finalize();
        let name = hex::encode(hash.as_bytes());

        let padded_data = Chunker::pad(
            next_id.to_le_bytes().to_vec(),
            UniformEnvelope::payload_size(self.chunk_size),
        );

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&METADATA_REGION_ID.to_le_bytes());
        ad.extend_from_slice(&ALLOCATOR_STATE_BLOCK_ID.to_le_bytes());

        let (ciphertext, nonce, tag) = self
            .cipher
            .encrypt(&self.key, &ad, &padded_data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
        let envelope_bytes = envelope
            .serialize(self.chunk_size)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.backend.put(&name, envelope_bytes))
            }),
            Err(_) => futures::executor::block_on(self.backend.put(&name, envelope_bytes)),
        };

        result.map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok(())
    }
}

impl BlockStorage for BPTreeStorage {
    type Error = StorageError;

    fn read_block(&self, id: BlockId) -> Result<Vec<u8>, Self::Error> {
        let name = self.block_name(id);

        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(self.backend.get(&name))),
            Err(_) => futures::executor::block_on(self.backend.get(&name)),
        };

        let envelope_bytes = result.map_err(|_| StorageError::BlockNotFound(id))?;

        let envelope = UniformEnvelope::deserialize(&envelope_bytes)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&self.region_id.to_le_bytes());
        ad.extend_from_slice(&id.to_le_bytes());

        let plaintext = self
            .cipher
            .decrypt(
                &self.key,
                &ad,
                &envelope.nonce,
                &envelope.tag,
                &envelope.ciphertext,
            )
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok(plaintext)
    }

    fn write_block(&mut self, id: BlockId, data: &[u8]) -> Result<(), Self::Error> {
        let name = self.block_name(id);

        let padded_data = Chunker::pad(
            data.to_vec(),
            UniformEnvelope::payload_size(self.chunk_size),
        );

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&self.region_id.to_le_bytes());
        ad.extend_from_slice(&id.to_le_bytes());

        let (ciphertext, nonce, tag) = self
            .cipher
            .encrypt(&self.key, &ad, &padded_data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
        let envelope_bytes = envelope
            .serialize(self.chunk_size)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.backend.put(&name, envelope_bytes))
            }),
            Err(_) => futures::executor::block_on(self.backend.put(&name, envelope_bytes)),
        };

        result.map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok(())
    }

    fn allocate_block(&mut self) -> Result<BlockId, Self::Error> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.persist_next_id(id + 1)?;
        Ok(id)
    }

    fn deallocate_block(&mut self, id: BlockId) -> Result<(), Self::Error> {
        let name = self.block_name(id);

        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                tokio::task::block_in_place(|| handle.block_on(self.backend.delete(&name)))
            }
            Err(_) => futures::executor::block_on(self.backend.delete(&name)),
        };

        result.map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok(())
    }

    fn block_size(&self) -> usize {
        UniformEnvelope::payload_size(self.chunk_size)
    }

    fn sync(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
