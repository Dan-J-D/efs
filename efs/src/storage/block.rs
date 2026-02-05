use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::Cipher;
use crate::storage::{RegionId, StorageBackend, ALLOCATOR_STATE_BLOCK_ID, METADATA_REGION_ID};
use anyhow::{Context, Result};
use blake3;
use bptree::storage::BlockId;
use futures::future::join_all;
use hex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct EfsBlockStorage {
    pub(crate) backend: Arc<dyn StorageBackend>,
    pub(crate) cipher: Arc<dyn Cipher>,
    pub(crate) key: Vec<u8>,
    pub(crate) next_id: Arc<AtomicU64>,
    pub(crate) persisted_id: Arc<AtomicU64>,
    pub(crate) chunk_size: usize,
    pub(crate) allocation_lock: Arc<Mutex<()>>,
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
            next_id: Arc::new(AtomicU64::new(10)), // Start from 10 to avoid collisions with reserved regions (0, 1, 2)
            persisted_id: Arc::new(AtomicU64::new(10)),
            chunk_size,
            allocation_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn new_with_shared_state(
        backend: Arc<dyn StorageBackend>,
        cipher: Arc<dyn Cipher>,
        key: Vec<u8>,
        chunk_size: usize,
        next_id: Arc<AtomicU64>,
        persisted_id: Arc<AtomicU64>,
        allocation_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            backend,
            cipher,
            key,
            next_id,
            persisted_id,
            chunk_size,
            allocation_lock,
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

    pub fn persisted_id(&self) -> Arc<AtomicU64> {
        self.persisted_id.clone()
    }

    pub fn allocation_lock(&self) -> Arc<Mutex<()>> {
        self.allocation_lock.clone()
    }

    pub fn set_next_id(&self, id: u64) {
        self.next_id.store(id, Ordering::SeqCst);
        self.persisted_id.store(id, Ordering::SeqCst);
    }

    pub async fn read_block(&self, region_id: RegionId, id: BlockId) -> Result<Vec<u8>> {
        let name = self.block_name(region_id, id);

        let result = self.backend.get(&name).await;

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

    pub async fn write_block(
        &self,
        region_id: RegionId,
        id: BlockId,
        data: &[u8],
    ) -> Result<()> {
        let name = self.block_name(region_id, id);

        let padded_data = Chunker::pad(
            data.to_vec(),
            UniformEnvelope::payload_size(
                self.chunk_size,
                self.cipher.nonce_size(),
                self.cipher.tag_size(),
            ),
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

        self.backend
            .put(&name, envelope_bytes)
            .await
            .context(format!(
                "Failed to put block {} in region {} to backend (name: {})",
                id, region_id, name
            ))?;

        Ok(())
    }

    pub async fn load_next_id(&self) -> Result<u64> {
        match self
            .read_block(METADATA_REGION_ID, ALLOCATOR_STATE_BLOCK_ID)
            .await
        {
            Ok(data) => {
                if data.len() >= 8 {
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&data[..8]);
                    Ok(u64::from_le_bytes(bytes))
                } else {
                    Err(anyhow::anyhow!("Invalid allocator state: too short"))
                }
            }
            Err(e) => {
                if crate::storage::is_not_found(&e) {
                    Ok(10)
                } else {
                    Err(e).context("Failed to load next_id from allocator state")
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn persist_next_id(&self, next_id: u64) -> Result<()> {
        self.write_block(
            METADATA_REGION_ID,
            ALLOCATOR_STATE_BLOCK_ID,
            &next_id.to_le_bytes(),
        )
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn allocate_blocks(
        &self,
        _region_id: RegionId,
        count: usize,
    ) -> Result<Vec<BlockId>> {
        let lock = self.allocation_lock.clone();
        let _guard = lock.lock().await;

        let first_id = self.next_id.fetch_add(count as u64, Ordering::SeqCst);
        let new_next_id = first_id + count as u64;

        let persisted = self.persisted_id.load(Ordering::SeqCst);
        if new_next_id > persisted {
            // Reserve a bunch of blocks at once to reduce sync overhead
            let reservation_jump = 1000.max(count as u64);
            let to_persist = first_id + reservation_jump;
            self.persist_next_id(to_persist)
                .await
                .context("Failed to persist next_id before allocation")?;
            self.persisted_id.store(to_persist, Ordering::SeqCst);
        }

        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            ids.push(first_id + i as u64);
        }

        Ok(ids)
    }

    pub async fn allocate_block(&self, region_id: RegionId) -> Result<BlockId> {
        let ids = self.allocate_blocks(region_id, 1).await?;
        Ok(ids[0])
    }

    pub async fn deallocate_block(&self, region_id: RegionId, id: BlockId) -> Result<()> {
        let name = self.block_name(region_id, id);

        self.backend
            .delete(&name)
            .await
            .context("Failed to delete block from backend")?;

        Ok(())
    }

    pub async fn deallocate_blocks(
        &self,
        region_id: RegionId,
        ids: Vec<BlockId>,
    ) -> Result<()> {
        let mut delete_futures = Vec::new();
        for id in ids {
            let name = self.block_name(region_id, id);
            let backend = self.backend.clone();
            delete_futures.push(async move {
                backend.delete(&name).await.context(format!(
                    "Failed to delete block {} from backend (name: {})",
                    id, name
                ))
            });
        }

        let results = join_all(delete_futures).await;
        let mut errors = Vec::new();
        for res in results {
            if let Err(e) = res {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Failed to deallocate one or more blocks: {:?}",
                errors
            ))
        }
    }
}
