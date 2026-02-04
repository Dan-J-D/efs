use super::StorageBackend;
use anyhow::{Context, Result};
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
            .context("Memory put error")?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = object_store::path::Path::from(name);
        let result = self
            .store
            .get(&path)
            .await
            .context("Memory get error")?;
        let bytes = result
            .bytes()
            .await
            .context("Memory collect error")?;
        Ok(bytes.to_vec())
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let path = object_store::path::Path::from(name);
        self.store
            .delete(&path)
            .await
            .context("Memory delete error")?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<String>> {
        let mut stream = self.store.list(None);
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.context("Memory list error")?;
            keys.push(meta.location.to_string());
        }
        Ok(keys)
    }
}
