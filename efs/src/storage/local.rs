use super::StorageBackend;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use object_store::{local::LocalFileSystem, ObjectStore};
use std::sync::Arc;

pub struct LocalBackend {
    store: Arc<dyn ObjectStore>,
}

impl LocalBackend {
    pub fn new(root: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            store: Arc::new(
                LocalFileSystem::new_with_prefix(root)
                    .context("Failed to initialize local storage")?,
            ),
        })
    }
}

#[async_trait]
impl StorageBackend for LocalBackend {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let path = object_store::path::Path::from(name);
        self.store
            .put(&path, data.into())
            .await
            .context("Failed to put object in local storage")?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = object_store::path::Path::from(name);
        let result = self
            .store
            .get(&path)
            .await
            .context("Failed to get object from local storage")?;
        let bytes = result
            .bytes()
            .await
            .context("Failed to read bytes from local storage object")?;
        Ok(bytes.to_vec())
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let path = object_store::path::Path::from(name);
        self.store
            .delete(&path)
            .await
            .context("Failed to delete object from local storage")?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<String>> {
        let mut stream = self.store.list(None);
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.context("Failed to list objects in local storage")?;
            keys.push(meta.location.to_string());
        }
        Ok(keys)
    }
}
