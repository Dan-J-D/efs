use crate::index::bptree_storage::BPTreeStorage;
use crate::EfsIndex;
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

    async fn get(&self, path: &str) -> Result<Option<(Vec<u64>, u64)>> {
        let tree = self.tree.read().await;
        tree.get(&path.to_string())
            .context("Failed to retrieve entry from KvIndex")
    }

    async fn list(&self) -> Result<Vec<String>> {
        let tree = self.tree.read().await;
        let mut paths = Vec::new();
        let iter = tree.iter().context("Failed to create iterator for KvIndex")?;
        for result in iter {
            let (path, _) = result.context("Failed to read entry during KvIndex listing")?;
            paths.push(path);
        }
        Ok(paths)
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let mut tree = self.tree.write().await;
        tree.delete(&path.to_string())
            .context("Failed to delete entry from KvIndex")?;
        Ok(())
    }
}
