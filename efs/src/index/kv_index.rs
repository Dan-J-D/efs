use crate::index::bptree_storage::BPTreeStorage;
use crate::{EfsEntry, EfsIndex};
use anyhow::{Context, Result};
use async_trait::async_trait;
use bptree::BPTree;
use tokio::sync::RwLock;

pub struct KvIndex {
    tree: RwLock<BPTree<String, (Vec<u64>, u64), BPTreeStorage>>,
}

impl KvIndex {
    pub fn new(storage: BPTreeStorage) -> Result<Self> {
        let tree = BPTree::new(storage).context("Failed to initialize BPTree for KvIndex")?;
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
            .context("Failed to insert entry into KvIndex")?;
        Ok(())
    }

    async fn mkdir(&self, path: &str) -> Result<()> {
        // Flat index does not have explicit directory entries.
        // We just validate the path.
        crate::path::normalize_path(path)?;
        Ok(())
    }

    async fn get(&self, path: &str) -> Result<Option<(Vec<u64>, u64)>> {
        let tree = self.tree.read().await;
        tree.get(&path.to_string())
            .context("Failed to retrieve entry from KvIndex")
    }

    async fn get_entry(&self, path: &str) -> Result<Option<EfsEntry>> {
        let tree = self.tree.read().await;
        let path_str = path.to_string();

        // Check if it is a file
        if let Some((block_ids, total_size)) = tree
            .get(&path_str)
            .context("Failed to retrieve entry from KvIndex")?
        {
            return Ok(Some(EfsEntry::File {
                block_ids,
                total_size,
            }));
        }

        // Check if it is a directory (prefix)
        let mut prefix = path_str;
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }

        let iter = tree
            .iter()
            .context("Failed to create iterator for KvIndex")?;
        for result in iter {
            let (p, _) = result.context("Failed to read entry during KvIndex search")?;
            if p.starts_with(&prefix) {
                return Ok(Some(EfsEntry::Directory));
            }
        }

        Ok(None)
    }

    async fn list(&self) -> Result<Vec<String>> {
        let tree = self.tree.read().await;
        let mut paths = Vec::new();
        let iter = tree
            .iter()
            .context("Failed to create iterator for KvIndex")?;
        for result in iter {
            let (path, _) = result.context("Failed to read entry during KvIndex listing")?;
            paths.push(path);
        }
        Ok(paths)
    }

    async fn list_dir(&self, path: &str) -> Result<Vec<(String, EfsEntry)>> {
        let tree = self.tree.read().await;
        let mut prefix = path.to_string();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        // Normalize empty/root prefix for matching
        if prefix == "/" {
            prefix = String::new();
        }

        let mut entries = std::collections::HashMap::new();
        let iter = tree
            .iter()
            .context("Failed to create iterator for KvIndex")?;
        for result in iter {
            let (p, value) = result.context("Failed to read entry during KvIndex list_dir")?;
            if p.starts_with(&prefix) {
                let remaining = &p[prefix.len()..];
                if remaining.is_empty() {
                    continue;
                }

                if let Some(slash_idx) = remaining.find('/') {
                    let dir_name = &remaining[..slash_idx];
                    entries.insert(dir_name.to_string(), EfsEntry::Directory);
                } else {
                    entries.insert(
                        remaining.to_string(),
                        EfsEntry::File {
                            block_ids: value.0.clone(),
                            total_size: value.1,
                        },
                    );
                }
            }
        }
        Ok(entries.into_iter().collect())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let mut tree = self.tree.write().await;
        // In a flat index, a "directory" doesn't have an entry to delete.
        // We ignore the error if the exact path doesn't exist.
        let _ = tree.delete(&path.to_string());
        Ok(())
    }

    async fn delete_region(&self, _path: &str) -> Result<()> {
        // KvIndex is a flat index, no regions to delete
        Ok(())
    }
}
