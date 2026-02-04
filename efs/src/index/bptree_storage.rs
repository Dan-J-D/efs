use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::Cipher;
use crate::storage::{RegionId, StorageBackend, ALLOCATOR_STATE_BLOCK_ID, METADATA_REGION_ID};
use anyhow::Result;
use bptree::storage::{BlockId, BlockStorage, StorageError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct BPTreeStorage {
    pub backend: Arc<dyn StorageBackend>,
    pub cipher: Arc<dyn Cipher>,
    pub key: Vec<u8>,
    pub next_id: Arc<AtomicU64>,
    pub chunk_size: usize,
    pub region_id: RegionId,
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

    fn persist_next_id(&mut self, next_id: u64) -> Result<(), StorageError> {
        let old_region = self.region_id;
        self.region_id = METADATA_REGION_ID;
        let res = self.write_block(ALLOCATOR_STATE_BLOCK_ID, &next_id.to_le_bytes());
        self.region_id = old_region;
        res
    }

    fn block_name(&self, id: BlockId) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.key);
        hasher.update(&self.region_id.to_le_bytes());
        hasher.update(&id.to_le_bytes());
        let hash = hasher.finalize();
        hex::encode(hash.as_bytes())
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

        let envelope_bytes = result.map_err(|e| {
            if crate::storage::is_not_found(&e) {
                StorageError::BlockNotFound(id)
            } else {
                StorageError::Serialization(format!("Storage error: {}", e))
            }
        })?;
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
        let current_id = self.next_id.load(Ordering::SeqCst);
        let new_next_id = current_id + 1;
        self.persist_next_id(new_next_id)?;
        self.next_id.store(new_next_id, Ordering::SeqCst);
        Ok(current_id)
    }

    fn deallocate_block(&mut self, id: BlockId) -> Result<(), Self::Error> {
        let name = self.block_name(id);
        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                tokio::task::block_in_place(|| handle.block_on(self.backend.delete(&name)))
            }
            Err(_) => futures::executor::block_on(self.backend.delete(&name)),
        };
        let _ = result;
        Ok(())
    }

    fn deallocate_blocks(&mut self, ids: Vec<BlockId>) -> Result<(), Self::Error> {
        for id in &ids {
            let name = self.block_name(*id);
            let _ = match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    tokio::task::block_in_place(|| handle.block_on(self.backend.delete(&name)))
                }
                Err(_) => futures::executor::block_on(self.backend.delete(&name)),
            };
        }
        Ok(())
    }

    fn block_size(&self) -> usize {
        UniformEnvelope::payload_size(self.chunk_size)
    }

    fn sync(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
