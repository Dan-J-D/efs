pub mod chunk;
pub mod crypto;
pub mod index;
pub mod mirror;
pub mod silo;
pub mod storage;

pub use crate::chunk::{Chunker, UniformEnvelope, DEFAULT_CHUNK_SIZE};
pub use crate::crypto::{Cipher, Hasher, Kdf};
pub use crate::index::EfsBlockStorage;
pub use crate::mirror::MirrorOrchestrator;
pub use crate::silo::{SiloConfig, SiloManager};
pub use crate::storage::StorageBackend;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait EfsIndex: Send + Sync {
    async fn insert(&self, path: &str, block_ids: Vec<u64>, total_size: u64) -> Result<()>;
    async fn get(&self, path: &str) -> Result<Option<(Vec<u64>, u64)>>;
    async fn list(&self) -> Result<Vec<String>>;
    async fn delete(&self, path: &str) -> Result<()>;
}

pub struct Efs {
    pub index: Arc<dyn EfsIndex>,
    pub storage_adapter: EfsBlockStorage,
    pub storage: Arc<dyn StorageBackend>,
    pub cipher: Arc<dyn Cipher>,
    pub key: Vec<u8>,
    pub chunk_size: usize,
}

impl Efs {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        cipher: Arc<dyn Cipher>,
        key: Vec<u8>,
        chunk_size: usize,
    ) -> Result<Self> {
        let storage_adapter =
            EfsBlockStorage::new(storage.clone(), cipher.clone(), key.clone(), chunk_size);

        let next_id = storage_adapter.load_next_id()?;
        storage_adapter.set_next_id(next_id);

        let index_storage = crate::index::BTreeStorage::new(
            storage.clone(),
            cipher.clone(),
            key.clone(),
            storage_adapter.next_id(),
            chunk_size,
            crate::index::BTREE_REGION_ID,
        );
        let index = Arc::new(crate::index::KvIndex::new(index_storage)?);

        Ok(Self {
            index,
            storage_adapter,
            storage,
            cipher,
            key,
            chunk_size,
        })
    }

    pub async fn put(&mut self, path: &str, data: &[u8]) -> Result<()> {
        let payload_size = UniformEnvelope::payload_size(self.chunk_size);
        let mut block_ids = Vec::new();
        let total_size = data.len() as u64;

        for chunk in data.chunks(payload_size) {
            let id = self
                .storage_adapter
                .allocate_block(crate::index::FILE_DATA_REGION_ID)?;
            block_ids.push(id);

            let padded = Chunker::pad(chunk.to_vec(), payload_size);
            let mut ad = Vec::with_capacity(16);
            ad.extend_from_slice(&crate::index::FILE_DATA_REGION_ID.to_le_bytes());
            ad.extend_from_slice(&id.to_le_bytes());

            let (ciphertext, nonce, tag) = self.cipher.encrypt(&self.key, &ad, &padded)?;
            let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
            let envelope_bytes = envelope.serialize(self.chunk_size)?;

            let name = self
                .storage_adapter
                .block_name(crate::index::FILE_DATA_REGION_ID, id);
            self.storage.put(&name, envelope_bytes).await?;
        }

        self.index.insert(path, block_ids, total_size).await?;

        Ok(())
    }

    pub async fn get(&self, path: &str) -> Result<Vec<u8>> {
        let (block_ids, total_size) = self
            .index
            .get(path)
            .await?
            .ok_or_else(|| anyhow!("File not found: {}", path))?;

        let mut data = Vec::new();
        for id in block_ids {
            let name = self
                .storage_adapter
                .block_name(crate::index::FILE_DATA_REGION_ID, id);
            let envelope_bytes = self.storage.get(&name).await?;
            let envelope = UniformEnvelope::deserialize(&envelope_bytes)?;

            let mut ad = Vec::with_capacity(16);
            ad.extend_from_slice(&crate::index::FILE_DATA_REGION_ID.to_le_bytes());
            ad.extend_from_slice(&id.to_le_bytes());

            let plaintext = self.cipher.decrypt(
                &self.key,
                &ad,
                &envelope.nonce,
                &envelope.tag,
                &envelope.ciphertext,
            )?;

            data.extend(plaintext);
        }

        data.truncate(total_size as usize);
        Ok(data)
    }

    pub async fn delete(&mut self, path: &str) -> Result<()> {
        let (block_ids, _) = self
            .index
            .get(path)
            .await?
            .ok_or_else(|| anyhow!("File not found: {}", path))?;

        self.index.delete(path).await?;

        for id in block_ids {
            let name = self
                .storage_adapter
                .block_name(crate::index::FILE_DATA_REGION_ID, id);
            self.storage.delete(&name).await?;
        }

        Ok(())
    }
}
