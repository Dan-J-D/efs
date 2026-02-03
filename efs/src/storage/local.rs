use anyhow::{Result, anyhow};
use async_trait::async_trait;
use object_store::{local::LocalFileSystem, ObjectStore};
use std::sync::Arc;
use super::StorageBackend;
use futures::StreamExt;

pub struct LocalBackend {
    store: Arc<dyn ObjectStore>,
}

impl LocalBackend {
    pub fn new(root: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            store: Arc::new(LocalFileSystem::new_with_prefix(root)?),
        })
    }
}

#[async_trait]
impl StorageBackend for LocalBackend {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let path = object_store::path::Path::from(name);
        self.store.put(&path, data.into()).await
            .map_err(|e| anyhow!("Local put error: {}", e))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = object_store::path::Path::from(name);
        let result = self.store.get(&path).await
            .map_err(|e| anyhow!("Local get error: {}", e))?;
        let bytes = result.bytes().await
            .map_err(|e| anyhow!("Local collect error: {}", e))?;
        Ok(bytes.to_vec())
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let path = object_store::path::Path::from(name);
        self.store.delete(&path).await
            .map_err(|e| anyhow!("Local delete error: {}", e))?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<String>> {
        let mut stream = self.store.list(None);
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| anyhow!("Local list error: {}", e))?;
            keys.push(meta.location.to_string());
        }
        Ok(keys)
    }
}
