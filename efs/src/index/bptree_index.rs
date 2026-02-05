use crate::index::BPTreeStorage;
use crate::storage::BTREE_INDEX_REGION_ID;
use crate::{EfsEntry, EfsIndex};
use anyhow::{anyhow, Result};
use async_recursion::async_recursion;
use async_trait::async_trait;
use bptree::storage::BlockStorage;
use bptree::BPTree;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum IndexEntry {
    File {
        file_id: u64,
        total_size: u64,
    },
    Directory {
        salt: [u8; 32],
    },
}

pub struct BtreeIndex {
    storage: BPTreeStorage,
    lock: tokio::sync::Mutex<()>,
}

impl BtreeIndex {
    pub fn new(storage: BPTreeStorage) -> Result<Self> {
        Ok(Self {
            storage,
            lock: tokio::sync::Mutex::new(()),
        })
    }

    fn derive_salt(&self, parent_salt: &[u8; 32], name: &str) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.storage.key);
        hasher.update(parent_salt);
        hasher.update(name.as_bytes());
        let mut salt = [0u8; 32];
        salt.copy_from_slice(hasher.finalize().as_bytes());
        salt
    }

    async fn get_tree(&self, salt: [u8; 32]) -> Result<BPTree<String, IndexEntry, BPTreeStorage>> {
        let storage = self.storage.with_context(BTREE_INDEX_REGION_ID, salt);
        BPTree::new(storage)
            .await
            .map_err(|e| anyhow!("BPTree error: {}", e))
    }

    fn normalize_path(&self, path: &str) -> Result<Vec<String>> {
        let normalized = crate::path::normalize_path(path)?;
        if normalized.is_empty() {
            return Ok(vec![]);
        }
        Ok(normalized.split('/').map(|s| s.to_string()).collect())
    }
}

#[async_trait]
impl EfsIndex<String, EfsEntry> for BtreeIndex {
    #[tracing::instrument(skip(self))]
    async fn put(&self, path: &String, value: EfsEntry) -> Result<()> {
        let _guard = self.lock.lock().await;
        match value {
            EfsEntry::File {
                file_id,
                total_size,
            } => {
                let parts = self.normalize_path(path)?;
                if parts.is_empty() {
                    return Err(anyhow!("Cannot insert file at root path"));
                }

                let mut current_salt = self.storage.context_salt;

                for part in parts.iter().take(parts.len() - 1) {
                    let tree = self.get_tree(current_salt).await?;

                    match tree.get(part).await.map_err(|e| anyhow!("{}", e))? {
                        Some(IndexEntry::Directory { salt }) => {
                            current_salt = salt;
                        }
                        Some(IndexEntry::File { .. }) => {
                            return Err(anyhow!("Path component '{}' is a file", part));
                        }
                        None => {
                            return Err(anyhow!("Path component '{}' does not exist", part));
                        }
                    }
                }

                let mut tree = self.get_tree(current_salt).await?;
                tree.insert(
                    parts.last().unwrap().clone(),
                    IndexEntry::File {
                        file_id,
                        total_size,
                    },
                )
                .await
                .map_err(|e| anyhow!("Insert error: {}", e))?;

                Ok(())
            }
            EfsEntry::Directory => {
                let parts = self.normalize_path(path)?;
                if parts.is_empty() {
                    return Ok(());
                }

                let mut current_salt = self.storage.context_salt;

                for part in parts.iter().take(parts.len() - 1) {
                    let tree = self.get_tree(current_salt).await?;

                    match tree.get(part).await.map_err(|e| anyhow!("{}", e))? {
                        Some(IndexEntry::Directory { salt }) => {
                            current_salt = salt;
                        }
                        Some(IndexEntry::File { .. }) => {
                            return Err(anyhow!("Path component '{}' is a file", part));
                        }
                        None => {
                            return Err(anyhow!("Path component '{}' does not exist", part));
                        }
                    }
                }

                if let Some(last_part) = parts.last() {
                    let mut tree = self.get_tree(current_salt).await?;
                    match tree.get(last_part).await.map_err(|e| anyhow!("{}", e))? {
                        Some(IndexEntry::Directory { .. }) => {
                            // Idempotent: directory already exists
                            return Ok(());
                        }
                        Some(IndexEntry::File { .. }) => {
                            return Err(anyhow!("File '{}' already exists", last_part));
                        }
                        None => {
                            let new_salt = self.derive_salt(&current_salt, last_part);
                            tree.insert(
                                last_part.clone(),
                                IndexEntry::Directory {
                                    salt: new_salt,
                                },
                            )
                            .await
                            .map_err(|e| anyhow!("{}", e))?;
                        }
                    }
                }
                Ok(())
            }
        }
    }

    async fn get(&self, path: &String) -> Result<Option<EfsEntry>> {
        let _guard = self.lock.lock().await;
        let parts = match self.normalize_path(path) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        if parts.is_empty() {
            return Ok(Some(EfsEntry::Directory));
        }

        let mut current_salt = self.storage.context_salt;
        for i in 0..parts.len() {
            let tree = self.get_tree(current_salt).await?;
            let part = &parts[i];

            match tree.get(part).await.map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { salt }) => {
                    if i == parts.len() - 1 {
                        return Ok(Some(EfsEntry::Directory));
                    }
                    current_salt = salt;
                }
                Some(IndexEntry::File {
                    file_id,
                    total_size,
                }) => {
                    if i == parts.len() - 1 {
                        return Ok(Some(EfsEntry::File {
                            file_id,
                            total_size,
                        }));
                    } else {
                        return Err(anyhow!("Path component '{}' is a file", part));
                    }
                }
                None => {
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    async fn list(&self) -> Result<Vec<(String, EfsEntry)>> {
        let _guard = self.lock.lock().await;
        let mut results = Vec::new();
        self.list_full_recursive(self.storage.context_salt, "", &mut results)
            .await?;
        Ok(results)
    }

    async fn list_dir(&self, path: &String) -> Result<Vec<(String, EfsEntry)>> {
        let _guard = self.lock.lock().await;
        let parts = self.normalize_path(path)?;
        let mut current_salt = self.storage.context_salt;

        if !parts.is_empty() {
            for part in parts {
                let tree = self.get_tree(current_salt).await?;
                match tree.get(&part).await.map_err(|e| anyhow!("{}", e))? {
                    Some(IndexEntry::Directory { salt }) => {
                        current_salt = salt;
                    }
                    _ => return Err(anyhow!("Directory not found")),
                }
            }
        }

        let tree = self.get_tree(current_salt).await?;
        let mut results = Vec::new();
        for (name, entry) in tree.range(..).await.map_err(|e| anyhow!("{}", e))? {
            let efs_entry = match entry {
                IndexEntry::File {
                    file_id,
                    total_size,
                } => EfsEntry::File {
                    file_id,
                    total_size,
                },
                IndexEntry::Directory { .. } => EfsEntry::Directory,
            };
            results.push((name, efs_entry));
        }
        Ok(results)
    }

    async fn delete(&self, path: &String) -> Result<()> {
        let _guard = self.lock.lock().await;
        let parts = self.normalize_path(path)?;
        if parts.is_empty() {
            return Err(anyhow!("Cannot delete root directory"));
        }

        let mut current_salt = self.storage.context_salt;
        for part in parts.iter().take(parts.len() - 1) {
            let tree = self.get_tree(current_salt).await?;
            match tree.get(part).await.map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { salt }) => {
                    current_salt = salt;
                }
                _ => return Err(anyhow!("Path not found")),
            }
        }

        let mut tree = self.get_tree(current_salt).await?;
        tree.delete(parts.last().unwrap())
            .await
            .map_err(|e| anyhow!("Delete error: {}", e))?;
        Ok(())
    }

    async fn delete_region(&self, path: &String) -> Result<()> {
        let _guard = self.lock.lock().await;
        let parts = self.normalize_path(path)?;
        let mut current_salt = self.storage.context_salt;

        // Traverse to find the salt of the directory at path
        for part in &parts {
            let tree = self.get_tree(current_salt).await?;
            match tree.get(part).await.map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { salt }) => {
                    current_salt = salt;
                }
                _ => return Err(anyhow!("Directory not found at {}", path)),
            }
        }

        // Now current_salt is the salt we want to deallocate.
        // We load the tree and collect all blocks.
        let tree = self.get_tree(current_salt).await?;
        let block_ids = tree
            .get_all_block_ids()
            .await
            .map_err(|e| anyhow!("{}", e))?;

        // Deallocate each block.
        let mut storage = self.storage.with_context(BTREE_INDEX_REGION_ID, current_salt);
        storage.deallocate_blocks(block_ids).await.map_err(|e| {
            anyhow!(
                "Failed to deallocate blocks in region {}: {}",
                BTREE_INDEX_REGION_ID,
                e
            )
        })?;

        Ok(())
    }
}

impl BtreeIndex {
    #[async_recursion]
    async fn list_full_recursive(
        &self,
        current_salt: [u8; 32],
        prefix: &str,
        results: &mut Vec<(String, EfsEntry)>,
    ) -> Result<()> {
        let tree = self.get_tree(current_salt).await?;
        for (name, entry) in tree.range(..).await.map_err(|e| anyhow!("{}", e))? {
            let full_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix, name)
            };

            match entry {
                IndexEntry::File {
                    file_id,
                    total_size,
                } => {
                    results.push((
                        full_path,
                        EfsEntry::File {
                            file_id,
                            total_size,
                        },
                    ));
                }
                IndexEntry::Directory { salt } => {
                    results.push((full_path.clone(), EfsEntry::Directory));
                    self.list_full_recursive(salt, &full_path, results)
                        .await?;
                }
            }
        }
        Ok(())
    }
}
