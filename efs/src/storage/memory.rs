use super::StorageBackend;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use object_store::{memory::InMemory, ObjectStore};
use std::sync::Arc;

pub struct MemoryBackend {
    store: Arc<dyn ObjectStore>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MemoryBackend {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let path = object_store::path::Path::from(name);
        self.store
            .put(&path, data.into())
            .await
            .map_err(|e| anyhow!("Memory put error: {}", e))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = object_store::path::Path::from(name);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| anyhow!("Memory get error: {}", e))?;
        let bytes = result
            .bytes()
            .await
            .map_err(|e| anyhow!("Memory collect error: {}", e))?;
        Ok(bytes.to_vec())
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let path = object_store::path::Path::from(name);
        self.store
            .delete(&path)
            .await
            .map_err(|e| anyhow!("Memory delete error: {}", e))?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<String>> {
        let mut stream = self.store.list(None);
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| anyhow!("Memory list error: {}", e))?;
            keys.push(meta.location.to_string());
        }
        Ok(keys)
    }
}
