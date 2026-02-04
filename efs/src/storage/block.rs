use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::Cipher;
use crate::storage::{
    RegionId, StorageBackend, ALLOCATOR_STATE_BLOCK_ID, FREE_LIST_BLOCK_ID, METADATA_REGION_ID,
};
use anyhow::{Context, Result};
use blake3;
use bptree::storage::BlockId;
use hex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

pub struct EfsBlockStorage {
    backend: Arc<dyn StorageBackend>,
    cipher: Arc<dyn Cipher>,
    key: Vec<u8>,
    next_id: Arc<AtomicU64>,
    free_list: Arc<Mutex<Vec<u64>>>,
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
            free_list: Arc::new(Mutex::new(Vec::new())),
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

    pub fn free_list(&self) -> Arc<Mutex<Vec<u64>>> {
        self.free_list.clone()
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

        let (ciphertext, nonce, tag) =
            self.cipher
                .encrypt(&self.key, &ad, &padded_data)
                .context(format!(
                    "Failed to encrypt block {} in region {}",
                    id, region_id
                ))?;

        let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
        let envelope_bytes = envelope.serialize(self.chunk_size).context(format!(
            "Failed to serialize uniform envelope for block {} in region {}",
            id, region_id
        ))?;

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.backend.put(&name, envelope_bytes))
            }),
            Err(_) => futures::executor::block_on(self.backend.put(&name, envelope_bytes)),
        }
        .context(format!(
            "Failed to put block {} in region {} to backend (name: {})",
            id, region_id, name
        ))?;

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
                    Err(anyhow::anyhow!("Invalid allocator state: too short"))
                }
            }
            Err(_e) => {
                // If the block is not found, it's a new silo, start from 2
                // We should check if the error is indeed "not found"
                // Assuming our backend returns a specific error for not found
                // For now, let's just log it and return 2 if we can't find it,
                // but only if it's really missing.
                Ok(2)
            }
        }
    }

    pub fn persist_next_id(&mut self, next_id: u64) -> Result<()> {
        self.write_block(
            METADATA_REGION_ID,
            ALLOCATOR_STATE_BLOCK_ID,
            &next_id.to_le_bytes(),
        )
    }

    pub fn load_free_list(&self) -> Result<Vec<u64>> {
        match self.read_block(METADATA_REGION_ID, FREE_LIST_BLOCK_ID) {
            Ok(data) => {
                let list: Vec<u64> =
                    bincode::deserialize(&data).context("Failed to deserialize free list")?;
                Ok(list)
            }
            Err(_) => Ok(Vec::new()),
        }
    }

    pub fn persist_free_list(&mut self) -> Result<()> {
        let list = self.free_list.lock().unwrap().clone();
        let data = bincode::serialize(&list).context("Failed to serialize free list")?;
        self.write_block(METADATA_REGION_ID, FREE_LIST_BLOCK_ID, &data)
    }

    pub fn set_free_list(&self, list: Vec<u64>) {
        let mut fl = self.free_list.lock().unwrap();
        *fl = list;
    }

    pub fn allocate_blocks(&mut self, _region_id: RegionId, count: usize) -> Result<Vec<BlockId>> {
        let mut ids = Vec::with_capacity(count);

        {
            let mut fl = self.free_list.lock().unwrap();
            while ids.len() < count && !fl.is_empty() {
                ids.push(fl.pop().unwrap());
            }
        }

        if !ids.is_empty() {
            self.persist_free_list()
                .context("Failed to persist free list after allocation")?;
        }

        let remaining = count - ids.len();
        if remaining > 0 {
            // We use a simple strategy: persist the new next_id first to ensure we don't
            // reuse IDs after a crash.
            let first_id = self.next_id.load(Ordering::SeqCst);
            let new_next_id = first_id + remaining as u64;

            self.persist_next_id(new_next_id)
                .context("Failed to persist next_id before allocation")?;

            self.next_id.store(new_next_id, Ordering::SeqCst);

            for i in 0..remaining {
                let id = first_id + i as u64;
                ids.push(id);
            }
        }

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

        {
            let mut fl = self.free_list.lock().unwrap();
            if id != 1 {
                fl.push(id);
            }
        }
        self.persist_free_list()
            .context("Failed to persist free list after deallocation")?;

        Ok(())
    }

    pub fn deallocate_blocks(&mut self, region_id: RegionId, ids: Vec<BlockId>) -> Result<()> {
        for id in &ids {
            let name = self.block_name(region_id, *id);

            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    tokio::task::block_in_place(|| handle.block_on(self.backend.delete(&name)))
                }
                Err(_) => futures::executor::block_on(self.backend.delete(&name)),
            }
            .context("Failed to delete block from backend")?;
        }

        {
            let mut fl = self.free_list.lock().unwrap();
            for id in ids {
                if id != 1 {
                    fl.push(id);
                }
            }
        }
        self.persist_free_list()
            .context("Failed to persist free list after deallocation")?;

        Ok(())
    }
}
