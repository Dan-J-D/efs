use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::Cipher;
use crate::storage::{RegionId, StorageBackend, ALLOCATOR_STATE_BLOCK_ID, METADATA_REGION_ID};
use anyhow::{Context, Result};
use blake3;
use bptree::storage::BlockId;
use hex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct EfsBlockStorage {
    backend: Arc<dyn StorageBackend>,
    cipher: Arc<dyn Cipher>,
    key: Vec<u8>,
    next_id: Arc<AtomicU64>,
    chunk_size: usize,
}

impl EfsBlockStorage {
    pub fn new(
        backend: Arc<dyn StorageBackend>,
        cipher: Arc<dyn Cipher>,
        key: Vec<u8>,
        chunk_size: usize,
    ) -> Self {
        Self {
            backend,
            cipher,
            key,
            next_id: Arc::new(AtomicU64::new(2)), // Block 1 is reserved for KvIndex root
            chunk_size,
        }
    }

    pub fn block_name(&self, region_id: RegionId, id: BlockId) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.key);
        hasher.update(&region_id.to_le_bytes());
        hasher.update(&id.to_le_bytes());
        let hash = hasher.finalize();
        hex::encode(hash.as_bytes())
    }

    pub fn next_id(&self) -> Arc<AtomicU64> {
        self.next_id.clone()
    }

    pub fn set_next_id(&self, id: u64) {
        self.next_id.store(id, Ordering::SeqCst);
    }

    pub fn read_block(&self, region_id: RegionId, id: BlockId) -> Result<Vec<u8>> {
        let name = self.block_name(region_id, id);

        let result = match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(self.backend.get(&name))),
            Err(_) => futures::executor::block_on(self.backend.get(&name)),
        };

        let envelope_bytes = result.context("Failed to get block from backend")?;
        let envelope = UniformEnvelope::deserialize(&envelope_bytes)
            .context("Failed to deserialize uniform envelope")?;

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&region_id.to_le_bytes());
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
            .context("Failed to decrypt block")?;

        Ok(plaintext)
    }

    pub fn write_block(&mut self, region_id: RegionId, id: BlockId, data: &[u8]) -> Result<()> {
        let name = self.block_name(region_id, id);

        let padded_data = Chunker::pad(
            data.to_vec(),
            UniformEnvelope::payload_size(self.chunk_size),
        );

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&region_id.to_le_bytes());
        ad.extend_from_slice(&id.to_le_bytes());

        let (ciphertext, nonce, tag) = self
            .cipher
            .encrypt(&self.key, &ad, &padded_data)
            .context("Failed to encrypt block")?;

        let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
        let envelope_bytes = envelope
            .serialize(self.chunk_size)
            .context("Failed to serialize uniform envelope")?;

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.backend.put(&name, envelope_bytes))
            }),
            Err(_) => futures::executor::block_on(self.backend.put(&name, envelope_bytes)),
        }
        .context("Failed to put block to backend")?;

        Ok(())
    }

    pub fn load_next_id(&self) -> Result<u64> {
        match self.read_block(METADATA_REGION_ID, ALLOCATOR_STATE_BLOCK_ID) {
            Ok(data) => {
                if data.len() >= 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&data[..8]);
                    Ok(u64::from_le_bytes(bytes))
                } else {
                    Ok(2)
                }
            }
            Err(_) => Ok(2),
        }
    }

    pub fn persist_next_id(&mut self, next_id: u64) -> Result<()> {
        self.write_block(
            METADATA_REGION_ID,
            ALLOCATOR_STATE_BLOCK_ID,
            &next_id.to_le_bytes(),
        )
    }

    pub fn allocate_blocks(&mut self, _region_id: RegionId, count: usize) -> Result<Vec<BlockId>> {
        let mut ids = Vec::with_capacity(count);
        let first_id = self.next_id.fetch_add(count as u64, Ordering::SeqCst);

        for i in 0..count {
            let id = first_id + i as u64;
            ids.push(id);
        }

        self.persist_next_id(first_id + count as u64)
            .context("Failed to persist next_id after allocation")?;

        Ok(ids)
    }

    pub fn allocate_block(&mut self, region_id: RegionId) -> Result<BlockId> {
        let ids = self.allocate_blocks(region_id, 1)?;
        Ok(ids[0])
    }

    pub fn deallocate_block(&mut self, region_id: RegionId, id: BlockId) -> Result<()> {
        let name = self.block_name(region_id, id);

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                tokio::task::block_in_place(|| handle.block_on(self.backend.delete(&name)))
            }
            Err(_) => futures::executor::block_on(self.backend.delete(&name)),
        }
        .context("Failed to delete block from backend")?;

        Ok(())
    }

    pub fn deallocate_blocks(&mut self, region_id: RegionId, ids: Vec<BlockId>) -> Result<()> {
        for id in ids {
            self.deallocate_block(region_id, id)?;
        }
        Ok(())
    }
}
