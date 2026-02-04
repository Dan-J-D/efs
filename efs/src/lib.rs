pub mod chunk;
pub mod crypto;
pub mod index;
pub mod mirror;
pub mod silo;
pub mod storage;

pub use crate::chunk::{Chunker, UniformEnvelope, DEFAULT_CHUNK_SIZE};
pub use crate::crypto::{Cipher, Hasher, Kdf};
pub use crate::storage::block::EfsBlockStorage;
pub use crate::mirror::MirrorOrchestrator;
pub use crate::silo::{SiloConfig, SiloManager};
pub use crate::storage::{StorageBackend, RegionId, FILE_DATA_REGION_ID, BTREE_REGION_ID};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use getset::{Getters, Setters};
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

#[derive(Getters, Setters)]
pub struct EfsBuilder {
    #[getset(get = "pub", set = "pub")]
    storage: Arc<dyn StorageBackend>,
    #[getset(get = "pub", set = "pub")]
    cipher: Arc<dyn Cipher>,
    #[getset(get = "pub", set = "pub")]
    key: Vec<u8>,
    #[getset(get = "pub", set = "pub")]
    chunk_size: usize,
    #[getset(get = "pub", set = "pub")]
    index: Option<Arc<dyn EfsIndex>>,
}

impl Default for EfsBuilder {
    fn default() -> Self {
        Self {
            storage: Arc::new(crate::storage::memory::MemoryBackend::new()),
            cipher: Arc::new(crate::crypto::standard::StandardCipher),
            key: vec![0u8; 32],
            chunk_size: DEFAULT_CHUNK_SIZE,
            index: None,
        }
    }
}

impl EfsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_storage(mut self, storage: Arc<dyn StorageBackend>) -> Self {
        self.storage = storage;
        self
    }

    pub fn with_cipher(mut self, cipher: Arc<dyn Cipher>) -> Self {
        self.cipher = cipher;
        self
    }

    pub fn with_key(mut self, key: Vec<u8>) -> Self {
        self.key = key;
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn with_index(mut self, index: Arc<dyn EfsIndex>) -> Self {
        self.index = Some(index);
        self
    }

    pub fn build(self) -> Result<Efs> {
        let storage_adapter = EfsBlockStorage::new(
            self.storage.clone(),
            self.cipher.clone(),
            self.key.clone(),
            self.chunk_size,
        );

        let next_id = storage_adapter
            .load_next_id()
            .context("Failed to load next_id from storage")?;
        storage_adapter.set_next_id(next_id);

        let index = if let Some(index) = self.index {
            index
        } else {
            let index_storage = crate::index::BPTreeStorage::new(
                self.storage.clone(),
                self.cipher.clone(),
                self.key.clone(),
                storage_adapter.next_id(),
                self.chunk_size,
                BTREE_REGION_ID,
            );
            Arc::new(
                crate::index::KvIndex::new(index_storage).context("Failed to create KvIndex")?,
            )
        };

        Ok(Efs {
            index,
            storage_adapter,
            storage: self.storage,
            cipher: self.cipher,
            key: self.key,
            chunk_size: self.chunk_size,
        })
    }
}

impl Efs {
    pub fn builder() -> EfsBuilder {
        EfsBuilder::default()
    }

    pub fn new(
        storage: Arc<dyn StorageBackend>,
        cipher: Arc<dyn Cipher>,
        key: Vec<u8>,
        chunk_size: usize,
    ) -> Result<Self> {
        Self::builder()
            .with_storage(storage)
            .with_cipher(cipher)
            .with_key(key)
            .with_chunk_size(chunk_size)
            .build()
    }

    pub async fn put(&mut self, path: &str, data: &[u8]) -> Result<()> {
        let payload_size = UniformEnvelope::payload_size(self.chunk_size);
        let total_size = data.len() as u64;
        let chunks: Vec<_> = data.chunks(payload_size).collect();
        let chunk_count = chunks.len();

        let block_ids = self
            .storage_adapter
            .allocate_blocks(FILE_DATA_REGION_ID, chunk_count)
            .context("Failed to allocate blocks for file")?;

        let mut upload_futures = Vec::new();

        for (i, chunk) in chunks.into_iter().enumerate() {
            let id = block_ids[i];
            let key = self.key.clone();
            let cipher = self.cipher.clone();
            let storage = self.storage.clone();
            let chunk_size = self.chunk_size;
            let chunk_data = chunk.to_vec();
            
            let name = self.storage_adapter.block_name(FILE_DATA_REGION_ID, id);

            upload_futures.push(async move {
                let padded = Chunker::pad(chunk_data, payload_size);
                let mut ad = Vec::with_capacity(16);
                ad.extend_from_slice(&FILE_DATA_REGION_ID.to_le_bytes());
                ad.extend_from_slice(&id.to_le_bytes());

                let (ciphertext, nonce, tag) = cipher
                    .encrypt(&key, &ad, &padded)
                    .context("Encryption failed")?;
                
                let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
                let envelope_bytes = envelope
                    .serialize(chunk_size)
                    .context("Serialization failed")?;

                storage
                    .put(&name, envelope_bytes)
                    .await
                    .context("Storage put failed")?;
                
                Ok::<(), anyhow::Error>(())
            });
        }

        stream::iter(upload_futures)
            .buffer_unordered(8) // Process up to 8 chunks in parallel
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .context("One or more chunk uploads failed")?;

        self.index
            .insert(path, block_ids, total_size)
            .await
            .context("Failed to insert file into index")?;

        Ok(())
    }

    pub async fn get(&self, path: &str) -> Result<Vec<u8>> {
        let (block_ids, total_size) = self
            .index
            .get(path)
            .await
            .context("Failed to query index")?
            .ok_or_else(|| anyhow!("File not found: {}", path))?;

        let mut download_futures = Vec::new();

        for id in block_ids {
            let key = self.key.clone();
            let cipher = self.cipher.clone();
            let storage = self.storage.clone();
            let name = self.storage_adapter.block_name(FILE_DATA_REGION_ID, id);

            download_futures.push(async move {
                let envelope_bytes = storage
                    .get(&name)
                    .await
                    .context("Storage get failed")?;
                
                let envelope = UniformEnvelope::deserialize(&envelope_bytes)
                    .context("Deserialization failed")?;

                let mut ad = Vec::with_capacity(16);
                ad.extend_from_slice(&FILE_DATA_REGION_ID.to_le_bytes());
                ad.extend_from_slice(&id.to_le_bytes());

                let plaintext = cipher
                    .decrypt(
                        &key,
                        &ad,
                        &envelope.nonce,
                        &envelope.tag,
                        &envelope.ciphertext,
                    )
                    .context("Decryption failed")?;

                Ok::<Vec<u8>, anyhow::Error>(plaintext)
            });
        }

        let results = stream::iter(download_futures)
            .buffered(8) // Download up to 8 chunks in parallel, maintaining order
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<Vec<u8>>>>()
            .context("One or more chunk downloads failed")?;

        let mut data = Vec::with_capacity(total_size as usize);
        for chunk in results {
            data.extend(chunk);
        }

        data.truncate(total_size as usize);
        Ok(data)
    }

    pub async fn delete(&mut self, path: &str) -> Result<()> {
        let (block_ids, _) = self
            .index
            .get(path)
            .await
            .context("Failed to query index")?
            .ok_or_else(|| anyhow!("File not found: {}", path))?;

        self.index
            .delete(path)
            .await
            .context("Failed to delete from index")?;

        let mut delete_futures = Vec::new();

        for id in block_ids {
            let storage = self.storage.clone();
            let name = self.storage_adapter.block_name(FILE_DATA_REGION_ID, id);

            delete_futures.push(async move {
                storage
                    .delete(&name)
                    .await
                    .context("Storage delete failed")?;
                Ok::<(), anyhow::Error>(())
            });
        }

        stream::iter(delete_futures)
            .buffer_unordered(8)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .context("One or more chunk deletions failed")?;

        Ok(())
    }
}
