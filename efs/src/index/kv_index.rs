use crate::index::bptree_storage::BPTreeStorage;
use crate::EfsIndex;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use bptree::BPTree;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;
use tokio::sync::RwLock;

pub struct KvIndex<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    tree: RwLock<BPTree<K, V, BPTreeStorage>>,
    #[allow(dead_code)]
    storage: BPTreeStorage,
}

impl<K, V> KvIndex<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    pub async fn new(storage: BPTreeStorage) -> Result<Self> {
        let tree = BPTree::new(storage.clone())
            .await
            .context("Failed to initialize BPTree for KvIndex")?;
        Ok(Self {
            tree: RwLock::new(tree),
            storage,
        })
    }
}

#[async_trait]
impl<K, V> EfsIndex<K, V> for KvIndex<K, V>
where
    K: Serialize + DeserializeOwned + Ord + Clone + Send + Sync + Debug + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + Debug + 'static,
{
    async fn put(&self, key: &K, value: V) -> Result<()> {
        let mut tree = self.tree.write().await;
        tree.insert(key.clone(), value)
            .await
            .context("Failed to insert entry into KvIndex")?;
        Ok(())
    }

    async fn get(&self, key: &K) -> Result<Option<V>> {
        let tree = self.tree.read().await;
        tree.get(key)
            .await
            .context("Failed to retrieve entry from KvIndex")
    }

    async fn list(&self) -> Result<Vec<(K, V)>> {
        let tree = self.tree.read().await;
        let mut entries = Vec::new();
        for (k, v) in tree.range(..).await.map_err(|e| anyhow!("{}", e))? {
            entries.push((k, v));
        }
        Ok(entries)
    }

    async fn list_dir(&self, _key: &K) -> Result<Vec<(K, V)>> {
        // Generic KvIndex doesn't support hierarchical list_dir unless K is String.
        // We return empty for the generic case.
        Ok(Vec::new())
    }

    async fn delete(&self, key: &K) -> Result<()> {
        let mut tree = self.tree.write().await;
        let _ = tree.delete(key).await;
        Ok(())
    }

    async fn delete_region(&self, _key: &K) -> Result<()> {
        // KvIndex is a flat index, no regions to delete
        Ok(())
    }
}
