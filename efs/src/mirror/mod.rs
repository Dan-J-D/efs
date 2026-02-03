use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::future::join_all;
use crate::storage::StorageBackend;

pub struct MirrorOrchestrator {
    backends: Vec<Box<dyn StorageBackend>>,
}

impl MirrorOrchestrator {
    pub fn new(backends: Vec<Box<dyn StorageBackend>>) -> Self {
        Self { backends }
    }

    fn majority(&self) -> usize {
        (self.backends.len() / 2) + 1
    }
}

#[async_trait]
impl StorageBackend for MirrorOrchestrator {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let futures: Vec<_> = self.backends.iter().map(|b| b.put(name, data.clone())).collect();
        let results: Vec<Result<()>> = join_all(futures).await;
        
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        if success_count >= self.majority() {
            Ok(())
        } else {
            Err(anyhow!("Failed to reach majority consensus on write ({} successes out of {})", success_count, self.backends.len()))
        }
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        for backend in &self.backends {
            match backend.get(name).await {
                Ok(data) => return Ok(data),
                Err(_) => continue, // Fallback to next mirror
            }
        }
        Err(anyhow!("Failed to read from all mirrors for {}", name))
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let futures: Vec<_> = self.backends.iter().map(|b| b.delete(name)).collect();
        let results: Vec<Result<()>> = join_all(futures).await;
        
        let success_count = results.iter().filter(|r| r.is_ok()).count();
        if success_count >= self.majority() {
            Ok(())
        } else {
            Err(anyhow!("Failed to reach majority consensus on delete ({} successes out of {})", success_count, self.backends.len()))
        }
    }

    async fn list(&self) -> Result<Vec<String>> {
        // For list, we return the union of all backends or just the primary?
        // Usually, the primary should have everything.
        if self.backends.is_empty() {
            return Ok(Vec::new());
        }
        self.backends[0].list().await
    }
}
