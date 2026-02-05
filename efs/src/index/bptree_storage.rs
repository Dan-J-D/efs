use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::{Cipher, Hasher, Key32};
use crate::storage::{RegionId, StorageBackend, ALLOCATOR_STATE_BLOCK_ID, METADATA_REGION_ID};
use anyhow::Result;
use async_trait::async_trait;
use bptree::storage::{BlockId, BlockStorage, StorageError};
use futures::future::join_all;
use secrecy::{ExposeSecret, SecretBox};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::storage::block::EfsBlockStorage;

#[derive(Clone)]
pub struct BPlusTreeStorage {
    pub backend: Arc<dyn StorageBackend>,
    pub cipher: Arc<dyn Cipher>,
    pub hasher: Arc<dyn Hasher>,
    pub key: SecretBox<Key32>,
    pub next_id: Arc<AtomicU64>,
    pub persisted_id: Arc<AtomicU64>,
    pub chunk_size: usize,
    pub region_id: RegionId,
    pub context_salt: [u8; 32],
    pub allocation_lock: Arc<Mutex<()>>,
}

impl BPlusTreeStorage {
    pub fn new(
        storage: EfsBlockStorage,
        region_id: RegionId,
    ) -> Self {
        let mut data = Vec::new();
        data.extend_from_slice(storage.key.expose_secret().as_ref());
        data.extend_from_slice(&region_id.to_le_bytes());
        let hash = storage.hasher.hash(&data);
        
        let mut context_salt = [0u8; 32];
        let len = hash.len().min(32);
        context_salt[..len].copy_from_slice(&hash[..len]);

        Self {
            backend: storage.backend.clone(),
            cipher: storage.cipher.clone(),
            hasher: storage.hasher.clone(),
            key: storage.key.clone(),
            next_id: storage.next_id.clone(),
            persisted_id: storage.persisted_id.clone(),
            chunk_size: storage.chunk_size,
            region_id,
            context_salt,
            allocation_lock: storage.allocation_lock.clone(),
        }
    }

    pub fn with_context(&self, region_id: RegionId, context_salt: [u8; 32]) -> Self {
        let mut cloned = self.clone();
        cloned.region_id = region_id;
        cloned.context_salt = context_salt;
        cloned
    }

    pub fn with_region(&self, region_id: RegionId) -> Self {
        let mut cloned = self.clone();
        cloned.region_id = region_id;
        cloned
    }

    async fn persist_next_id(&mut self, next_id: u64) -> Result<(), StorageError> {
        let old_region = self.region_id;
        self.region_id = METADATA_REGION_ID;
        let res = self
            .write_block(ALLOCATOR_STATE_BLOCK_ID, &next_id.to_le_bytes())
            .await;
        self.region_id = old_region;
        res
    }

    fn block_name(&self, id: BlockId) -> String {
        let mut data = Vec::new();
        data.extend_from_slice(self.key.expose_secret().as_ref());
        data.extend_from_slice(&self.context_salt);
        data.extend_from_slice(&id.to_le_bytes());
        let hash = self.hasher.hash(&data);
        hex::encode(hash)
    }
}

#[async_trait]
impl BlockStorage for BPlusTreeStorage {
    type Error = StorageError;

    #[tracing::instrument(skip(self))]
    async fn read_block(&self, id: BlockId) -> Result<Vec<u8>, Self::Error> {
        let name = self.block_name(id);
        let result = self.backend.get(&name).await;

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
                self.key.expose_secret().as_ref(),
                &ad,
                &envelope.nonce,
                &envelope.tag,
                &envelope.ciphertext,
            )
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        Ok(plaintext)
    }

    #[tracing::instrument(skip(self, data))]
    async fn write_block(&mut self, id: BlockId, data: &[u8]) -> Result<(), Self::Error> {
        let name = self.block_name(id);
        let padded_data = Chunker::pad(
            data.to_vec(),
            UniformEnvelope::payload_size(
                self.chunk_size,
                self.cipher.nonce_size(),
                self.cipher.tag_size(),
            ),
        );

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&self.region_id.to_le_bytes());
        ad.extend_from_slice(&id.to_le_bytes());

        let (ciphertext, nonce, tag) = self
            .cipher
            .encrypt(self.key.expose_secret().as_ref(), &ad, &padded_data)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
        let envelope_bytes = envelope
            .serialize(self.chunk_size)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let result = self.backend.put(&name, envelope_bytes).await;

        result.map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn allocate_block(&mut self) -> Result<BlockId, Self::Error> {
        let lock = self.allocation_lock.clone();
        let _guard = lock.lock().await;
        let current_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let new_next_id = current_id + 1;

        let persisted = self.persisted_id.load(Ordering::SeqCst);
        if new_next_id > persisted {
            let reservation_jump = 1000;
            let to_persist = current_id + reservation_jump;
            self.persist_next_id(to_persist).await?;
            self.persisted_id.store(to_persist, Ordering::SeqCst);
        }

        Ok(current_id)
    }

    async fn deallocate_block(&mut self, id: BlockId) -> Result<(), Self::Error> {
        let name = self.block_name(id);
        let _ = self.backend.delete(&name).await;
        Ok(())
    }

    async fn deallocate_blocks(&mut self, ids: Vec<BlockId>) -> Result<(), Self::Error> {
        let mut delete_futures = Vec::new();
        for id in ids {
            let name = self.block_name(id);
            let backend = self.backend.clone();
            delete_futures.push(async move {
                let _ = backend.delete(&name).await;
            });
        }
        join_all(delete_futures).await;
        Ok(())
    }

    fn block_size(&self) -> usize {
        UniformEnvelope::payload_size(
            self.chunk_size,
            self.cipher.nonce_size(),
            self.cipher.tag_size(),
        )
    }

    async fn sync(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
