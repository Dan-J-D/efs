use std::sync::Arc;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use object_store::{ObjectStore, path::Path};
use super::StorageBackend;

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
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let path = Path::from(name);
        self.store
            .put(&path, data.into())
            .await
            .map_err(|e| anyhow!("S3 put error: {}", e))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = Path::from(name);
        let result = self.store
            .get(&path)
            .await
            .map_err(|e| anyhow!("S3 get error: {}", e))?;
        
        let bytes = result
            .bytes()
            .await
            .map_err(|e| anyhow!("S3 body collect error: {}", e))?;
        
        Ok(bytes.to_vec())
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let path = Path::from(name);
        self.store
            .delete(&path)
            .await
            .map_err(|e| anyhow!("S3 delete error: {}", e))?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<String>> {
        use futures::StreamExt;
        let mut stream = self.store.list(None);
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.map_err(|e| anyhow!("S3 list error: {}", e))?;
            keys.push(meta.location.to_string());
        }
        Ok(keys)
    }
}
