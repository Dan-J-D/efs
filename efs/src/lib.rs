pub mod chunk;
pub mod crypto;
pub mod index;
pub mod mirror;
pub mod path;
pub mod silo;
pub mod storage;

pub use crate::chunk::{Chunker, UniformEnvelope, DEFAULT_CHUNK_SIZE};
pub use crate::crypto::{Cipher, Hasher, Kdf, Key32};
pub use crate::mirror::MirrorOrchestrator;
pub use crate::silo::{SiloConfig, SiloManager};
pub use crate::storage::block::EfsBlockStorage;
pub use crate::storage::{RegionId, StorageBackend, BPLUS_TREE_INDEX_REGION_ID, FILE_DATA_REGION_ID};

use anyhow::{anyhow, Context, Result};
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use getset::{Getters, Setters};
use secrecy::{ExposeSecret, SecretBox};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EfsEntry {
    File {
        file_id: u64,
        total_size: u64,
    },
    Directory,
}

#[async_trait]
pub trait EfsIndex<K, V>: Send + Sync 
where
    K: 'static,
    V: 'static,
{
    async fn put(&self, key: &K, value: V) -> Result<()>;
    async fn get(&self, key: &K) -> Result<Option<V>>;
    async fn list(&self) -> Result<Vec<(K, V)>>;
    async fn list_dir(&self, key: &K) -> Result<Vec<(K, V)>>;
    async fn delete(&self, key: &K) -> Result<()>;
    async fn delete_region(&self, key: &K) -> Result<()>;
}

#[async_trait]
impl<K, V, T: EfsIndex<K, V> + ?Sized> EfsIndex<K, V> for Arc<T>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
{
    async fn put(&self, key: &K, value: V) -> Result<()> {
        (**self).put(key, value).await
    }
    async fn get(&self, key: &K) -> Result<Option<V>> {
        (**self).get(key).await
    }
    async fn list(&self) -> Result<Vec<(K, V)>> {
        (**self).list().await
    }
    async fn list_dir(&self, key: &K) -> Result<Vec<(K, V)>> {
        (**self).list_dir(key).await
    }
    async fn delete(&self, key: &K) -> Result<()> {
        (**self).delete(key).await
    }
    async fn delete_region(&self, key: &K) -> Result<()> {
        (**self).delete_region(key).await
    }
}

pub struct Efs<K: 'static = String, V: 'static = EfsEntry, I = Arc<dyn EfsIndex<K, V>>>
where
    I: EfsIndex<K, V>,
{
    pub index: I,
    pub storage_adapter: EfsBlockStorage,
    pub storage: Arc<dyn StorageBackend>,
    pub cipher: Arc<dyn Cipher>,
    pub hasher: Arc<dyn Hasher>,
    pub key: SecretBox<Key32>,
    pub chunk_size: usize,
    _phantom: PhantomData<(K, V)>,
}

#[derive(Getters, Setters)]
pub struct EfsBuilder<K: 'static = String, V: 'static = EfsEntry, I = Arc<dyn EfsIndex<K, V>>> {
    #[getset(get = "pub", set = "pub")]
    storage: Arc<dyn StorageBackend>,
    #[getset(get = "pub", set = "pub")]
    cipher: Arc<dyn Cipher>,
    #[getset(get = "pub", set = "pub")]
    hasher: Arc<dyn Hasher>,
    #[getset(get = "pub", set = "pub")]
    key: SecretBox<Key32>,
    #[getset(get = "pub", set = "pub")]
    chunk_size: usize,
    #[getset(get = "pub", set = "pub")]
    index: Option<I>,
    _phantom: PhantomData<(K, V)>,
}

impl Default for EfsBuilder<String, EfsEntry, Arc<dyn EfsIndex<String, EfsEntry>>> {
    fn default() -> Self {
        Self {
            storage: Arc::new(crate::storage::memory::MemoryBackend::new()),
            cipher: Arc::new(crate::crypto::Aes256GcmCipher::default()),
            hasher: Arc::new(crate::crypto::Blake3Hasher::default()),
            key: SecretBox::new(Box::new(Key32([0u8; 32]))),
            chunk_size: DEFAULT_CHUNK_SIZE,
            index: None,
            _phantom: PhantomData,
        }
    }
}

impl<K, V, I> EfsBuilder<K, V, I>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: EfsIndex<K, V>,
{
    pub fn new() -> Self
    where
        Self: Default,
    {
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

    pub fn with_hasher(mut self, hasher: Arc<dyn Hasher>) -> Self {
        self.hasher = hasher;
        self
    }

    pub fn with_key(mut self, key: SecretBox<Key32>) -> Self {
        self.key = key;
        self
    }

    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn with_index<NewK, NewV, NewI>(
        self,
        index: NewI,
    ) -> EfsBuilder<NewK, NewV, NewI>
    where
        NewK: Send + Sync + 'static,
        NewV: Send + Sync + 'static,
        NewI: EfsIndex<NewK, NewV>,
    {
        EfsBuilder {
            storage: self.storage,
            cipher: self.cipher,
            hasher: self.hasher,
            key: self.key,
            chunk_size: self.chunk_size,
            index: Some(index),
            _phantom: PhantomData,
        }
    }

    pub async fn build_custom(self) -> Result<Efs<K, V, I>> {
        let storage_adapter = EfsBlockStorage::new(
            self.storage.clone(),
            self.cipher.clone(),
            self.hasher.clone(),
            self.key.clone(),
            self.chunk_size,
        );

        let next_id = storage_adapter
            .load_next_id()
            .await
            .context("Failed to load next_id from storage")?;
        storage_adapter.set_next_id(next_id);

        let index = if let Some(index) = self.index {
            index
        } else {
            // This part is problematic because we don't know how to create a default index for arbitrary K, V
            return Err(anyhow!("Index not provided and no default available for these types"));
        };

        Ok(Efs {
            index,
            storage_adapter,
            storage: self.storage,
            cipher: self.cipher,
            hasher: self.hasher,
            key: self.key,
            chunk_size: self.chunk_size,
            _phantom: PhantomData,
        })
    }
}

impl EfsBuilder<String, EfsEntry, Arc<dyn EfsIndex<String, EfsEntry>>> {
    pub async fn build(self) -> Result<Efs<String, EfsEntry, Arc<dyn EfsIndex<String, EfsEntry>>>> {
        let storage_adapter = EfsBlockStorage::new(
            self.storage.clone(),
            self.cipher.clone(),
            self.hasher.clone(),
            self.key.clone(),
            self.chunk_size,
        );

        let next_id = storage_adapter
            .load_next_id()
            .await
            .context("Failed to load next_id from storage")?;
        storage_adapter.set_next_id(next_id);

        let index = if let Some(index) = self.index {
            index
        } else {
            let cache_backend = Arc::new(crate::storage::memory::MemoryBackend::new());
            let cached_storage = Arc::new(crate::storage::cache::CacheBackend::new(
                cache_backend,
                self.storage.clone(),
            ));
            let cached_adapter = EfsBlockStorage::new_with_shared_state(
                cached_storage,
                self.cipher.clone(),
                self.hasher.clone(),
                self.key.clone(),
                self.chunk_size,
                storage_adapter.next_id(),
                storage_adapter.persisted_id(),
                storage_adapter.allocation_lock(),
            );
            let index_storage = crate::index::BPlusTreeStorage::new(
                cached_adapter,
                BPLUS_TREE_INDEX_REGION_ID,
            );
            Arc::new(
                crate::index::BPlusTreeIndex::new(index_storage)
                    .context("Failed to create BPlusTreeIndex")?,
            ) as Arc<dyn EfsIndex<String, EfsEntry>>
        };

        Ok(Efs {
            index,
            storage_adapter,
            storage: self.storage,
            cipher: self.cipher,
            hasher: self.hasher,
            key: self.key,
            chunk_size: self.chunk_size,
            _phantom: PhantomData,
        })
    }
}

impl Efs<String, EfsEntry, Arc<dyn EfsIndex<String, EfsEntry>>> {
    pub fn builder() -> EfsBuilder<String, EfsEntry, Arc<dyn EfsIndex<String, EfsEntry>>> {
        EfsBuilder::default()
    }

    pub async fn new(
        storage: Arc<dyn StorageBackend>,
        cipher: Arc<dyn Cipher>,
        hasher: Arc<dyn Hasher>,
        key: SecretBox<Key32>,
        chunk_size: usize,
    ) -> Result<Self> {
        Self::builder()
            .with_storage(storage)
            .with_cipher(cipher)
            .with_hasher(hasher)
            .with_key(key)
            .with_chunk_size(chunk_size)
            .build()
            .await
    }
}

impl<K, V, I> Efs<K, V, I>
where
    K: Send + Sync + 'static,
    V: Send + Sync + 'static,
    I: EfsIndex<K, V>,
{
    pub async fn put_kv(&self, key: &K, value: V) -> Result<()> {
        self.index.put(key, value).await
    }

    pub async fn get_kv(&self, key: &K) -> Result<Option<V>> {
        self.index.get(key).await
    }

    pub async fn delete_kv(&self, key: &K) -> Result<()> {
        self.index.delete(key).await
    }

    pub async fn list_kv(&self) -> Result<Vec<(K, V)>> {
        self.index.list().await
    }
}

impl<I> Efs<String, EfsEntry, I>
where
    I: EfsIndex<String, EfsEntry>,
{
    #[tracing::instrument(skip(self, data))]
    pub async fn put(&self, path: &str, data: &[u8]) -> Result<()> {
        let path = crate::path::normalize_path(path)?;

        if let Some(parent) = crate::path::get_parent(&path) {
            self.mkdir_p(parent).await?;
        }

        let mut old_file_info = None;
        // Check if entry already exists to prevent leaks on overwrite
        if let Some(entry) = self.index.get(&path).await? {
            match entry {
                EfsEntry::File { file_id, total_size } => {
                    // Remember old blocks to deallocate them after successful overwrite
                    old_file_info = Some((file_id, total_size));
                }
                EfsEntry::Directory => {
                    return Err(anyhow!(
                        "Cannot overwrite directory with a file at '{}'. Delete the directory first.",
                        path
                    ));
                }
            }
        }

        let payload_size = UniformEnvelope::payload_size(
            self.chunk_size,
            self.cipher.nonce_size(),
            self.cipher.tag_size(),
        );
        let total_size = data.len() as u64;
        let chunks: Vec<_> = data.chunks(payload_size).collect();
        let chunk_count = chunks.len();

        let block_ids = {
            self.storage_adapter
                .allocate_blocks(FILE_DATA_REGION_ID, chunk_count)
                .await
                .context("Failed to allocate blocks for file")?
        };

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
                    .encrypt(key.expose_secret().as_ref(), &ad, &padded)
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

        {
            if let Err(e) = self
                .index
                .put(
                    &path,
                    EfsEntry::File {
                        file_id: block_ids.first().cloned().unwrap_or(0),
                        total_size,
                    },
                )
                .await
            {
                // Try to cleanup allocated blocks on index failure to prevent leakage
                let _ = self
                    .storage_adapter
                    .deallocate_blocks(FILE_DATA_REGION_ID, block_ids)
                    .await;
                return Err(e)
                    .context("Failed to insert file into index; cleaned up allocated blocks");
            }
        }

        // Successfully updated index, now deallocate old blocks if this was an overwrite
        if let Some((old_id, old_size)) = old_file_info {
            let payload_size = UniformEnvelope::payload_size(
            self.chunk_size,
            self.cipher.nonce_size(),
            self.cipher.tag_size(),
        );
            let num_blocks = (old_size as usize).div_ceil(payload_size);
            let old_ids: Vec<u64> = (old_id..old_id + num_blocks as u64).collect();
            let _ = self
                .storage_adapter
                .deallocate_blocks(FILE_DATA_REGION_ID, old_ids)
                .await;
        }

        Ok(())
    }

    pub async fn mkdir(&self, path: &str) -> Result<()> {
        let path = crate::path::normalize_path(path)?;
        if let Some(entry) = self.index.get(&path).await? {
            match entry {
                EfsEntry::Directory => return Ok(()),
                EfsEntry::File { .. } => {
                    return Err(anyhow!(
                        "Cannot create directory: a file already exists at '{}'",
                        path
                    ))
                }
            }
        }
        self.index.put(&path, EfsEntry::Directory).await
    }

    #[tracing::instrument(skip(self))]
    pub async fn mkdir_p(&self, path: &str) -> Result<()> {
        let path = crate::path::normalize_path(path)?;
        if path.is_empty() {
            return Ok(());
        }

        let parts: Vec<&str> = path.split('/').collect();
        let mut current = String::new();
        for part in parts {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(part);
            self.mkdir(&current).await?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn get(&self, path: &str) -> Result<Vec<u8>> {
        let path = crate::path::normalize_path(path)?;
        tracing::debug!("Reading file from {}", path);
        let entry = self
            .index
            .get(&path)
            .await
            .context("Failed to query index")?
            .ok_or_else(|| anyhow!("File not found: {}", path))?;

        let (file_id, total_size) = match entry {
            EfsEntry::File {
                file_id,
                total_size,
            } => (file_id, total_size),
            EfsEntry::Directory => return Err(anyhow!("Path is a directory")),
        };

        let payload_size = UniformEnvelope::payload_size(
            self.chunk_size,
            self.cipher.nonce_size(),
            self.cipher.tag_size(),
        );
                let num_blocks = (total_size as usize).div_ceil(payload_size);
        let block_ids: Vec<u64> = (file_id..file_id + num_blocks as u64).collect();

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
                        key.expose_secret().as_ref(),
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

    #[tracing::instrument(skip(self))]
    pub async fn list(&self) -> Result<Vec<String>> {
        tracing::debug!("Listing all files");
        let entries = self.index.list().await?;
        Ok(entries
            .into_iter()
            .filter_map(|(k, e)| match e {
                EfsEntry::File { .. } => Some(k),
                EfsEntry::Directory => None,
            })
            .collect())
    }

    #[tracing::instrument(skip(self))]
    pub async fn list_dir(&self, path: &str) -> Result<Vec<(String, EfsEntry)>> {
        let path = crate::path::normalize_path(path)?;
        tracing::debug!("Listing directory: {}", path);
        self.index.list_dir(&path).await
    }

    #[tracing::instrument(skip(self))]
    pub async fn delete(&self, path: &str) -> Result<()> {
        let path = crate::path::normalize_path(path)?;
        tracing::info!("Deleting {}", path);
        let entry = self
            .index
            .get(&path)
            .await
            .context("Failed to query index")?
            .ok_or_else(|| anyhow!("File not found: {}", path))?;

        let block_ids = match entry {
            EfsEntry::File {
                file_id,
                total_size,
            } => {
                let payload_size = UniformEnvelope::payload_size(
            self.chunk_size,
            self.cipher.nonce_size(),
            self.cipher.tag_size(),
        );
        let num_blocks = (total_size as usize).div_ceil(payload_size);
                (file_id..file_id + num_blocks as u64).collect()
            }
            EfsEntry::Directory => Vec::new(),
        };

        self.index
            .delete(&path)
            .await
            .context("Failed to delete from index")?;

        if !block_ids.is_empty() {
            self.storage_adapter
                .deallocate_blocks(FILE_DATA_REGION_ID, block_ids)
                .await
                .context("Failed to deallocate blocks")?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn delete_recursive(&self, path: &str) -> Result<()> {
        let path = crate::path::normalize_path(path)?;
        tracing::info!("Deleting recursive: {}", path);
        let entry = self
            .index
            .get(&path)
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
    async fn delete_dir_recursive(&self, path: &str) -> Result<()> {
        let path_str = path.to_string();
        let contents = self.index.list_dir(&path_str).await?;

        let mut futures = Vec::new();
        for (name, entry) in contents {
            let full_path = if path.is_empty() {
                name
            } else {
                format!("{}/{}", path, name)
            };

            // We need to be careful with &mut self.
            // Since we can't easily share &mut self across futures,
            // we'll have to do this in a way that works.
            // One way is to collect all paths and then delete them.
            futures.push((full_path, entry));
        }

        for (full_path, entry) in futures {
            match entry {
                EfsEntry::File { .. } => {
                    self.delete(&full_path).await?;
                }
                EfsEntry::Directory => {
                    self.delete_dir_recursive(&full_path).await?;
                }
            }
        }

        // Delete the region chunks if the index supports it (e.g. BPlusTreeIndex)
        self.index.delete_region(&path_str).await?;
        // Finally delete the directory entry from parent index
        self.index.delete(&path_str).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn put_recursive(&self, local_path: &str, remote_path: &str) -> Result<()> {
        let path = std::path::Path::new(local_path);
        if !path.exists() {
            return Err(anyhow!("Local path does not exist: {}", local_path));
        }

        let mut entries = Vec::new();
        if path.is_dir() {
            for entry in walkdir::WalkDir::new(path)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                let rel_path = entry
                    .path()
                    .strip_prefix(path)
                    .context("Failed to strip prefix")?;
                let mut remote_item_path = remote_path.to_string();
                if !remote_item_path.ends_with('/') && !rel_path.as_os_str().is_empty() {
                    remote_item_path.push('/');
                }
                let rel_path_str = rel_path.to_str().ok_or_else(|| {
                    anyhow!(
                        "Non-UTF8 path component in local filesystem: {:?}",
                        rel_path
                    )
                })?;
                remote_item_path.push_str(rel_path_str);
                entries.push((entry, remote_item_path));
            }
        } else {
            let data = std::fs::read(local_path)?;
            return self.put(remote_path, &data).await;
        }

        let upload_futures = entries.into_iter().map(|(entry, remote_item_path)| {
            async move {
                if entry.file_type().is_dir() {
                    self.mkdir_p(&remote_item_path).await?;
                } else {
                    let data = std::fs::read(entry.path())?;
                    self.put(&remote_item_path, &data).await?;
                }
                Ok::<(), anyhow::Error>(())
            }
        });

        stream::iter(upload_futures)
            .buffer_unordered(16) // Upload up to 16 files in parallel
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }
}
