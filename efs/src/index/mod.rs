use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::Cipher;
use crate::storage::StorageBackend;
use crate::EfsIndex;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use blake3;
use bptree::storage::{BlockId, BlockStorage, StorageError};
use bptree::BPTree;
use hex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

pub type RegionId = u64;

pub const ROOT_BLOCK_ID: BlockId = 0;
pub const METADATA_REGION_ID: RegionId = 0;
pub const BTREE_REGION_ID: RegionId = 2;
pub const FILE_DATA_REGION_ID: RegionId = 1;
pub const SUPERBLOCK_BLOCK_ID: BlockId = 0;

pub struct KvIndex {
    tree: RwLock<BPTree<String, (Vec<u64>, u64), BTreeStorage>>,
}

impl KvIndex {
    pub fn new(storage: BTreeStorage) -> Result<Self> {
        let tree = BPTree::new(storage).map_err(|e| anyhow!("KvIndex init error: {}", e))?;
        Ok(Self {
            tree: RwLock::new(tree),
        })
    }
}

#[async_trait]
impl EfsIndex for KvIndex {
    async fn insert(&self, path: &str, block_ids: Vec<u64>, total_size: u64) -> Result<()> {
        let mut tree = self.tree.write().await;
        tree.insert(path.to_string(), (block_ids, total_size))
            .map_err(|e| anyhow!("KvIndex insert error: {}", e))?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Option<(Vec<u64>, u64)>> {
        let tree = self.tree.read().await;
        tree.get(&path.to_string())
            .map_err(|e| anyhow!("KvIndex get error: {}", e))
    }

    async fn list(&self) -> Result<Vec<String>> {
        let tree = self.tree.read().await;
        let mut paths = Vec::new();
        for result in tree.iter().map_err(|e| anyhow!("{}", e))? {
            let (path, _) = result.map_err(|e| anyhow!("{}", e))?;
            paths.push(path);
        }
        Ok(paths)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let mut tree = self.tree.write().await;
        tree.delete(&path.to_string())
            .map_err(|e| anyhow!("KvIndex delete error: {}", e))?;
        Ok(())
    }
}

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

        let envelope_bytes = result?;
        let envelope = UniformEnvelope::deserialize(&envelope_bytes)?;

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&region_id.to_le_bytes());
        ad.extend_from_slice(&id.to_le_bytes());

        let plaintext = self.cipher.decrypt(
            &self.key,
            &ad,
            &envelope.nonce,
            &envelope.tag,
            &envelope.ciphertext,
        )?;

        Ok(plaintext)
    }

    pub fn load_next_id(&self) -> Result<u64> {
        match self.read_block(METADATA_REGION_ID, SUPERBLOCK_BLOCK_ID) {
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
            SUPERBLOCK_BLOCK_ID,
            &next_id.to_le_bytes(),
        )
    }

    pub fn allocate_block(&mut self, region_id: RegionId) -> Result<BlockId> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.persist_next_id(id + 1)?;
        self.write_block(region_id, id, &id.to_le_bytes())?;
        Ok(id)
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

        let (ciphertext, nonce, tag) = self.cipher.encrypt(&self.key, &ad, &padded_data)?;

        let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
        let envelope_bytes = envelope.serialize(self.chunk_size)?;

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| {
                handle.block_on(self.backend.put(&name, envelope_bytes))
            }),
            Err(_) => futures::executor::block_on(self.backend.put(&name, envelope_bytes)),
        }?;

        Ok(())
    }
}

pub struct BTreeStorage {
    backend: Arc<dyn StorageBackend>,
    cipher: Arc<dyn Cipher>,
    key: Vec<u8>,
    next_id: Arc<AtomicU64>,
    chunk_size: usize,
    region_id: RegionId,
}

impl BTreeStorage {
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
        hasher.update(&SUPERBLOCK_BLOCK_ID.to_le_bytes());
        let hash = hasher.finalize();
        let name = hex::encode(hash.as_bytes());

        let padded_data = Chunker::pad(
            next_id.to_le_bytes().to_vec(),
            UniformEnvelope::payload_size(self.chunk_size),
        );

        let mut ad = Vec::with_capacity(16);
        ad.extend_from_slice(&METADATA_REGION_ID.to_le_bytes());
        ad.extend_from_slice(&SUPERBLOCK_BLOCK_ID.to_le_bytes());

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

impl BlockStorage for BTreeStorage {
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

    fn deallocate_block(&mut self, _id: BlockId) -> Result<(), Self::Error> {
        Ok(())
    }

    fn block_size(&self) -> usize {
        UniformEnvelope::payload_size(self.chunk_size)
    }

    fn sync(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
