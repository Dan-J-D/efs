use super::StorageBackend;
use anyhow::{Context, Result};
use async_trait::async_trait;
use object_store::{path::Path, ObjectStore};
use std::sync::Arc;

pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
}

impl S3Backend {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    #[tracing::instrument(skip(self, data))]
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        tracing::debug!("S3 put: {}", name);
        let path = Path::from(name);
        self.store
            .put(&path, data.into())
            .await
            .context("Failed to put object in S3")?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        tracing::debug!("S3 get: {}", name);
        let path = Path::from(name);
        let result = self
            .store
            .get(&path)
            .await
            .context("Failed to get object from S3")?;

        let bytes = result
            .bytes()
            .await
            .context("Failed to read bytes from S3 object")?;

        Ok(bytes.to_vec())
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, name: &str) -> Result<()> {
        tracing::debug!("S3 delete: {}", name);
        let path = Path::from(name);
        self.store
            .delete(&path)
            .await
            .context("Failed to delete object from S3")?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn list(&self) -> Result<Vec<String>> {
        tracing::debug!("S3 list");
        use futures::StreamExt;
        let mut stream = self.store.list(None);
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.context("Failed to list objects in S3")?;
            keys.push(meta.location.to_string());
        }
        Ok(keys)
    }
}
