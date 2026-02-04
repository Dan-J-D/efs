use super::StorageBackend;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

/// A storage backend that caches data in a faster storage layer.
pub struct CacheBackend {
    cache: Arc<dyn StorageBackend>,
    backend: Arc<dyn StorageBackend>,
}

impl CacheBackend {
    pub fn new(cache: Arc<dyn StorageBackend>, backend: Arc<dyn StorageBackend>) -> Self {
        Self { cache, backend }
    }
}

#[async_trait]
impl StorageBackend for CacheBackend {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        // Write-through cache: write to backend first, then cache.
        self.backend.put(name, data.clone()).await?;
        // We don't want a cache failure to fail the whole operation, but we might want to know about it.
        // However, for simplicity and consistency with standard cache patterns, we'll just ignore cache errors here
        // or we could return it. Let's return it for now as it might indicate a serious issue with the cache layer.
        self.cache.put(name, data).await?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        // Try to get from cache first.
        if let Ok(data) = self.cache.get(name).await {
            return Ok(data);
        }

        // Cache miss: get from backend and populate cache.
        let data = self.backend.get(name).await?;
        // Populate cache in the background or just wait. Since we are in an async context,
        // and this is a caching layer, we'll wait to ensure the next request hits the cache.
        let _ = self.cache.put(name, data.clone()).await;
        Ok(data)
    }

    async fn delete(&self, name: &str) -> Result<()> {
        // Delete from both.
        self.backend.delete(name).await?;
        let _ = self.cache.delete(name).await;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<String>> {
        // Always list from the backend as it is the source of truth.
        self.backend.list().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryBackend;

    #[tokio::test]
    async fn test_cache_backend() -> Result<()> {
        let cache = Arc::new(MemoryBackend::new());
        let backend = Arc::new(MemoryBackend::new());
        let cached_backend = CacheBackend::new(cache.clone(), backend.clone());

        let name = "test_file";
        let data = b"hello world".to_vec();

        // Put into cached backend
        cached_backend.put(name, data.clone()).await?;

        // Verify it's in both
        assert_eq!(backend.get(name).await?, data);
        assert_eq!(cache.get(name).await?, data);

        // Get from cached backend (should hit cache)
        assert_eq!(cached_backend.get(name).await?, data);

        // Delete from cached backend
        cached_backend.delete(name).await?;

        // Verify it's gone from both
        assert!(backend.get(name).await.is_err());
        assert!(cache.get(name).await.is_err());

        // Test cache miss population
        backend.put(name, data.clone()).await?;
        assert!(cache.get(name).await.is_err()); // Not in cache yet

        assert_eq!(cached_backend.get(name).await?, data); // Triggers population
        assert_eq!(cache.get(name).await?, data); // Now in cache

        Ok(())
    }
}
