use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()>;
    async fn get(&self, name: &str) -> Result<Vec<u8>>;
    async fn delete(&self, name: &str) -> Result<()>;
    async fn list(&self) -> Result<Vec<String>>;
}

pub mod s3;
pub mod local;
