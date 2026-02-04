use crate::index::BPTreeStorage;
use crate::storage::BTREE_REGION_ID;
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
        block_ids: Vec<u64>,
        total_size: u64,
    },
    Directory {
        region_id: u64,
    },
}

pub struct BtreeIndex {
    storage: BPTreeStorage,
}

impl BtreeIndex {
    pub fn new(storage: BPTreeStorage) -> Result<Self> {
        Ok(Self { storage })
    }

    fn get_tree(&self, region_id: u64) -> Result<BPTree<String, IndexEntry, BPTreeStorage>> {
        let storage = self.storage.with_region(region_id);
        BPTree::new(storage).map_err(|e| anyhow!("BPTree error: {}", e))
    }

    fn allocate_region(&self) -> Result<u64> {
        let mut storage = self.storage.clone();
        storage
            .allocate_block()
            .map_err(|e| anyhow!("Failed to allocate region: {}", e))
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
impl EfsIndex for BtreeIndex {
    async fn insert(&self, path: &str, block_ids: Vec<u64>, total_size: u64) -> Result<()> {
        let parts = self.normalize_path(path)?;

        let mut current_region = BTREE_REGION_ID;

        for part in parts.iter().take(parts.len() - 1) {
            let mut tree = self.get_tree(current_region)?;

            match tree.get(part).map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { region_id }) => {
                    current_region = region_id;
                }
                Some(IndexEntry::File { .. }) => {
                    return Err(anyhow!("Path component '{}' is a file", part));
                }
                None => {
                    let new_region = self.allocate_region()?;
                    tree.insert(
                        part.clone(),
                        IndexEntry::Directory {
                            region_id: new_region,
                        },
                    )
                    .map_err(|e| anyhow!("{}", e))?;
                    current_region = new_region;
                }
            }
        }

        let mut tree = self.get_tree(current_region)?;
        tree.insert(
            parts.last().unwrap().clone(),
            IndexEntry::File {
                block_ids,
                total_size,
            },
        )
        .map_err(|e| anyhow!("Insert error: {}", e))?;

        Ok(())
    }

    async fn mkdir(&self, path: &str) -> Result<()> {
        let parts = self.normalize_path(path)?;
        if parts.is_empty() {
            return Ok(());
        }

        let mut current_region = BTREE_REGION_ID;

        for part in parts {
            let mut tree = self.get_tree(current_region)?;

            match tree.get(&part).map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { region_id }) => {
                    current_region = region_id;
                }
                Some(IndexEntry::File { .. }) => {
                    return Err(anyhow!("Path component '{}' is a file", part));
                }
                None => {
                    let new_region = self.allocate_region()?;
                    tree.insert(
                        part.clone(),
                        IndexEntry::Directory {
                            region_id: new_region,
                        },
                    )
                    .map_err(|e| anyhow!("{}", e))?;
                    current_region = new_region;
                }
            }
        }
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Option<(Vec<u64>, u64)>> {
        match self.get_entry(path).await? {
            Some(EfsEntry::File {
                block_ids,
                total_size,
            }) => Ok(Some((block_ids, total_size))),
            Some(EfsEntry::Directory) => Err(anyhow!("Path is a directory")),
            None => Ok(None),
        }
    }

    async fn get_entry(&self, path: &str) -> Result<Option<EfsEntry>> {
        let parts = match self.normalize_path(path) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        if parts.is_empty() {
            return Ok(Some(EfsEntry::Directory));
        }

        let mut current_region = BTREE_REGION_ID;
        for i in 0..parts.len() {
            let tree = self.get_tree(current_region)?;
            let part = &parts[i];

            match tree.get(part).map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { region_id }) => {
                    if i == parts.len() - 1 {
                        return Ok(Some(EfsEntry::Directory));
                    }
                    current_region = region_id;
                }
                Some(IndexEntry::File {
                    block_ids,
                    total_size,
                }) => {
                    if i == parts.len() - 1 {
                        return Ok(Some(EfsEntry::File {
                            block_ids,
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

    async fn list(&self) -> Result<Vec<String>> {
        let mut results = Vec::new();
        self.list_recursive(BTREE_REGION_ID, "", &mut results)
            .await?;
        Ok(results)
    }

    async fn list_dir(&self, path: &str) -> Result<Vec<(String, EfsEntry)>> {
        let parts = self.normalize_path(path)?;
        let mut current_region = BTREE_REGION_ID;

        if !parts.is_empty() {
            for part in parts {
                let tree = self.get_tree(current_region)?;
                match tree.get(&part).map_err(|e| anyhow!("{}", e))? {
                    Some(IndexEntry::Directory { region_id }) => {
                        current_region = region_id;
                    }
                    _ => return Err(anyhow!("Directory not found")),
                }
            }
        }

        let tree = self.get_tree(current_region)?;
        let mut results = Vec::new();
        for result in tree.iter().map_err(|e| anyhow!("{}", e))? {
            let (name, entry) = result.map_err(|e| anyhow!("{}", e))?;
            let efs_entry = match entry {
                IndexEntry::File {
                    block_ids,
                    total_size,
                } => EfsEntry::File {
                    block_ids,
                    total_size,
                },
                IndexEntry::Directory { .. } => EfsEntry::Directory,
            };
            results.push((name, efs_entry));
        }
        Ok(results)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let parts = self.normalize_path(path)?;
        if parts.is_empty() {
            return Err(anyhow!("Cannot delete root directory"));
        }

        let mut current_region = BTREE_REGION_ID;
        for part in parts.iter().take(parts.len() - 1) {
            let tree = self.get_tree(current_region)?;
            match tree.get(part).map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { region_id }) => {
                    current_region = region_id;
                }
                _ => return Err(anyhow!("Path not found")),
            }
        }

        let mut tree = self.get_tree(current_region)?;
        tree.delete(parts.last().unwrap())
            .map_err(|e| anyhow!("Delete error: {}", e))?;
        // BPTree might not have sync, but let's check if there is some other way to persist.
        // If it writes directly to BlockStorage, then it's already persisted.
        Ok(())
    }

    async fn delete_region(&self, path: &str) -> Result<()> {
        let parts = self.normalize_path(path)?;
        let mut current_region = BTREE_REGION_ID;

        // Traverse to find the region_id of the directory at path
        for part in &parts {
            let tree = self.get_tree(current_region)?;
            match tree.get(part).map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { region_id }) => {
                    current_region = region_id;
                }
                _ => return Err(anyhow!("Directory not found at {}", path)),
            }
        }

        // Now current_region is the region_id we want to deallocate.
        // We load the tree and collect all blocks.
        let tree = self.get_tree(current_region)?;
        let block_ids = tree.get_all_block_ids().map_err(|e| anyhow!("{}", e))?;

        // Deallocate each block. We use with_region to ensure we are deleting from the correct logical space.
        let mut storage = self.storage.with_region(current_region);
        storage
            .deallocate_blocks(block_ids)
            .map_err(|e| anyhow!("Failed to deallocate blocks in region {}: {}", current_region, e))?;

        Ok(())
    }
}

impl BtreeIndex {
    #[async_recursion]
    async fn list_recursive(
        &self,
        region_id: u64,
        prefix: &str,
        results: &mut Vec<String>,
    ) -> Result<()> {
        let tree = self.get_tree(region_id)?;
        for result in tree.iter().map_err(|e| anyhow!("{}", e))? {
            let (name, entry) = result.map_err(|e| anyhow!("{}", e))?;
            let full_path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{}/{}", prefix, name)
            };

            match entry {
                IndexEntry::File { .. } => {
                    results.push(full_path);
                }
                IndexEntry::Directory { region_id } => {
                    self.list_recursive(region_id, &full_path, results).await?;
                }
            }
        }
        Ok(())
    }
}
