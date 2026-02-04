pub mod chunk;
pub mod crypto;
pub mod index;
pub mod mirror;
pub mod path;
pub mod silo;
pub mod storage;

pub use crate::chunk::{Chunker, UniformEnvelope, DEFAULT_CHUNK_SIZE};
pub use crate::crypto::{Cipher, Hasher, Kdf};
pub use crate::mirror::MirrorOrchestrator;
pub use crate::silo::{SiloConfig, SiloManager};
pub use crate::storage::block::EfsBlockStorage;
pub use crate::storage::{RegionId, StorageBackend, BTREE_REGION_ID, FILE_DATA_REGION_ID};

use anyhow::{anyhow, Context, Result};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use getset::{Getters, Setters};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum EfsEntry {
    File {
        block_ids: Vec<u64>,
        total_size: u64,
    },
    Directory,
}

#[async_trait]
pub trait EfsIndex: Send + Sync {
    async fn insert(&self, path: &str, block_ids: Vec<u64>, total_size: u64) -> Result<()>;
    async fn get(&self, path: &str) -> Result<Option<(Vec<u64>, u64)>>;
    async fn get_entry(&self, path: &str) -> Result<Option<EfsEntry>>;
    async fn list(&self) -> Result<Vec<String>>;
    async fn list_dir(&self, path: &str) -> Result<Vec<(String, EfsEntry)>>;
    async fn delete(&self, path: &str) -> Result<()>;
    async fn delete_region(&self, path: &str) -> Result<()>;
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

    pub async fn build(self) -> Result<Efs> {
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

        let free_list = storage_adapter
            .load_free_list()
            .context("Failed to load free_list from storage")?;
        storage_adapter.set_free_list(free_list);

        let index = if let Some(index) = self.index {
            index
        } else {
            let index_storage = crate::index::BPTreeStorage::new(
                self.storage.clone(),
                self.cipher.clone(),
                self.key.clone(),
                storage_adapter.next_id(),
                storage_adapter.free_list(),
                self.chunk_size,
                BTREE_REGION_ID,
            );
            Arc::new(
                crate::index::BtreeIndex::new(index_storage)
                    .context("Failed to create BtreeIndex")?,
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

    pub async fn new(
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
            .await
    }

    pub async fn put(&mut self, path: &str, data: &[u8]) -> Result<()> {
        let path = crate::path::normalize_path(path)?;
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

        if let Err(e) = self.index
            .insert(&path, block_ids.clone(), total_size)
            .await
        {
            // Try to cleanup allocated blocks on index failure to prevent leakage
            for id in block_ids {
                let _ = self.storage_adapter.deallocate_block(FILE_DATA_REGION_ID, id);
            }
            return Err(e).context("Failed to insert file into index; cleaned up allocated blocks");
        }

        Ok(())
    }

    pub async fn get(&self, path: &str) -> Result<Vec<u8>> {
        let path = crate::path::normalize_path(path)?;
        let (block_ids, total_size) = self
            .index
            .get(&path)
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
                let envelope_bytes = storage.get(&name).await.context("Storage get failed")?;

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
        let path = crate::path::normalize_path(path)?;
        let (block_ids, _) = self
            .index
            .get(&path)
            .await
            .context("Failed to query index")?
            .ok_or_else(|| anyhow!("File not found: {}", path))?;

        self.index
            .delete(&path)
            .await
            .context("Failed to delete from index")?;

        self.storage_adapter
            .deallocate_blocks(FILE_DATA_REGION_ID, block_ids)
            .context("Failed to deallocate blocks")?;

        Ok(())
    }

    pub async fn delete_recursive(&mut self, path: &str) -> Result<()> {
        let path = crate::path::normalize_path(path)?;
        let entry = self
            .index
            .get_entry(&path)
            .await?
            .ok_or_else(|| anyhow!("Path not found: {}", path))?;

        match entry {
            EfsEntry::File { .. } => {
                self.delete(&path).await?;
            }
            EfsEntry::Directory => {
                self.delete_dir_recursive(&path).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn delete_dir_recursive(&mut self, path: &str) -> Result<()> {
        let contents = self.index.list_dir(path).await?;
        
        // Collect all deletion futures to run them in parallel
        // Note: self is &mut, so we can't easily parallelize if we use &mut self methods.
        // But we can collect the tasks and run them.
        // Actually, since we need to modify the index and storage, sequential might be safer
        // but we can at least optimize the directory traversal.
        
        for (name, entry) in contents {
            let full_path = if path.is_empty() {
                name
            } else {
                format!("{}/{}", path, name)
            };
            match entry {
                EfsEntry::File { .. } => {
                    self.delete(&full_path).await?;
                }
                EfsEntry::Directory => {
                    self.delete_dir_recursive(&full_path).await?;
                }
            }
        }
        // Delete the region chunks if the index supports it (e.g. BtreeIndex)
        // Currently BtreeIndex::delete_region is a no-op, leading to leaks of B-tree nodes.
        self.index.delete_region(path).await?;
        // Finally delete the directory entry from parent index
        self.index.delete(path).await?;
        Ok(())
    }

    pub async fn put_recursive(&mut self, local_path: &str, remote_path: &str) -> Result<()> {
        let path = std::path::Path::new(local_path);
        if !path.exists() {
            return Err(anyhow!("Local path does not exist: {}", local_path));
        }

        if path.is_dir() {
            for entry in walkdir::WalkDir::new(path)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_file())
            {
                let rel_path = entry.path().strip_prefix(path).unwrap();
                let mut remote_file_path = remote_path.to_string();
                if !remote_file_path.ends_with('/') {
                    remote_file_path.push('/');
                }
                remote_file_path.push_str(rel_path.to_str().unwrap());

                let data = std::fs::read(entry.path())?;
                self.put(&remote_file_path, &data).await?;
            }
        } else {
            let data = std::fs::read(local_path)?;
            self.put(remote_path, &data).await?;
        }
        Ok(())
    }
}
