use crate::index::BPTreeStorage;
use crate::storage::BTREE_REGION_ID;
use crate::EfsIndex;
use anyhow::{anyhow, Result};
use async_recursion::async_recursion;
use async_trait::async_trait;
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

    fn allocate_region(&self, path_so_far: &str) -> u64 {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.storage.key());
        hasher.update(&BTREE_REGION_ID.to_le_bytes());
        hasher.update(path_so_far.as_bytes());
        let hash = hasher.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash.as_bytes()[..8]);
        u64::from_le_bytes(bytes)
    }

    fn normalize_path(&self, path: &str) -> Result<Vec<String>> {
        let normalized = crate::path::normalize_path(path)?;
        Ok(normalized.split('/').map(|s| s.to_string()).collect())
    }
}

#[async_trait]
impl EfsIndex for BtreeIndex {
    async fn insert(&self, path: &str, block_ids: Vec<u64>, total_size: u64) -> Result<()> {
        let parts = self.normalize_path(path)?;

        let mut current_region = BTREE_REGION_ID;
        let mut path_so_far = String::new();

        for part in parts.iter().take(parts.len() - 1) {
            path_so_far.push('/');
            path_so_far.push_str(part);

            let mut tree = self.get_tree(current_region)?;
            match tree.get(part).map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { region_id }) => {
                    current_region = region_id;
                }
                Some(IndexEntry::File { .. }) => {
                    return Err(anyhow!("Path component '{}' is a file", part));
                }
                None => {
                    let new_region = self.allocate_region(&path_so_far);
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

    async fn get(&self, path: &str) -> Result<Option<(Vec<u64>, u64)>> {
        let parts = match self.normalize_path(path) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        let mut current_region = BTREE_REGION_ID;
        for i in 0..parts.len() {
            let tree = self.get_tree(current_region)?;
            match tree.get(&parts[i]).map_err(|e| anyhow!("{}", e))? {
                Some(IndexEntry::Directory { region_id }) => {
                    if i == parts.len() - 1 {
                        return Err(anyhow!("Path is a directory"));
                    }
                    current_region = region_id;
                }
                Some(IndexEntry::File {
                    block_ids,
                    total_size,
                }) => {
                    if i == parts.len() - 1 {
                        return Ok(Some((block_ids, total_size)));
                    } else {
                        return Err(anyhow!("Path component '{}' is a file", parts[i]));
                    }
                }
                None => return Ok(None),
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

    async fn delete(&self, path: &str) -> Result<()> {
        let parts = self.normalize_path(path)?;

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
