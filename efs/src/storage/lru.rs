use super::StorageBackend;
use anyhow::Result;
use async_trait::async_trait;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A storage backend wrapper that adds LRU eviction to any StorageBackend.
pub struct LruBackend {
    inner: Arc<dyn StorageBackend>,
    // We use a Mutex to protect the LruCache as it's not thread-safe and requires &mut for most ops.
    // Using tokio::sync::Mutex because it might be held across await points if we evict.
    cache: Mutex<LruCache<String, usize>>,
    max_size: Option<usize>,
    current_size: Mutex<usize>,
}

impl LruBackend {
    /// Create a new LruBackend wrapping another backend.
    /// 
    /// # Arguments
    /// * `inner` - The backend to wrap (e.g., MemoryBackend or LocalBackend)
    /// * `max_count` - Maximum number of items to keep in cache
    /// * `max_size` - Optional maximum total size in bytes to keep in cache
    pub fn new(inner: Arc<dyn StorageBackend>, max_count: usize, max_size: Option<usize>) -> Self {
        let capacity = NonZeroUsize::new(max_count).unwrap_or(NonZeroUsize::new(100).unwrap());
        Self {
            inner,
            cache: Mutex::new(LruCache::new(capacity)),
            max_size,
            current_size: Mutex::new(0),
        }
    }

    async fn evict_if_needed(&self) -> Result<()> {
        if self.max_size.is_none() {
            return Ok(());
        }
        let max_size = self.max_size.unwrap();

        loop {
            let evicted = {
                let mut cache = self.cache.lock().await;
                let current_size = self.current_size.lock().await;

                if *current_size <= max_size {
                    None
                } else {
                    cache.pop_lru()
                }
            };

            if let Some((key, size)) = evicted {
                self.inner.delete(&key).await?;
                let mut current_size = self.current_size.lock().await;
                if *current_size >= size {
                    *current_size -= size;
                } else {
                    *current_size = 0;
                }
            } else {
                break;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for LruBackend {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()> {
        let size = data.len();
        
        // Put into inner storage first
        self.inner.put(name, data).await?;

        let evicted = {
            let mut cache = self.cache.lock().await;
            let mut current_size = self.current_size.lock().await;

            // If it was already in cache, subtract old size
            if let Some(old_size) = cache.get(name) {
                *current_size -= *old_size;
            }

            let evicted = cache.push(name.to_string(), size);
            *current_size += size;
            evicted
        };

        if let Some((key, evicted_size)) = evicted {
            if key != name {
                self.inner.delete(&key).await?;
                let mut current_size = self.current_size.lock().await;
                if *current_size >= evicted_size {
                    *current_size -= evicted_size;
                }
            }
        }

        self.evict_if_needed().await?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let data = self.inner.get(name).await?;
        
        let evicted = {
            let mut cache = self.cache.lock().await;
            if cache.get(name).is_none() {
                let mut current_size = self.current_size.lock().await;
                let size = data.len();
                let evicted = cache.push(name.to_string(), size);
                *current_size += size;
                evicted
            } else {
                None
            }
        };

        if let Some((key, evicted_size)) = evicted {
            self.inner.delete(&key).await?;
            let mut current_size = self.current_size.lock().await;
            if *current_size >= evicted_size {
                *current_size -= evicted_size;
            }
        }

        self.evict_if_needed().await?;
        Ok(data)
    }

    async fn delete(&self, name: &str) -> Result<()> {
        self.inner.delete(name).await?;
        
        let mut cache = self.cache.lock().await;
        if let Some(size) = cache.pop(name) {
            let mut current_size = self.current_size.lock().await;
            if *current_size >= size {
                *current_size -= size;
            }
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<String>> {
        self.inner.list().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryBackend;

    #[tokio::test]
    async fn test_lru_count_eviction() -> Result<()> {
        let inner = Arc::new(MemoryBackend::new());
        let lru = LruBackend::new(inner.clone(), 2, None);

        lru.put("a", vec![1]).await?;
        lru.put("b", vec![2]).await?;
        lru.put("c", vec![3]).await?; // Should evict "a"

        assert!(inner.get("a").await.is_err());
        assert!(inner.get("b").await.is_ok());
        assert!(inner.get("c").await.is_ok());

        lru.get("b").await?; // Mark "b" as recently used
        lru.put("d", vec![4]).await?; // Should evict "c"

        assert!(inner.get("c").await.is_err());
        assert!(inner.get("b").await.is_ok());
        assert!(inner.get("d").await.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_lru_size_eviction() -> Result<()> {
        let inner = Arc::new(MemoryBackend::new());
        // Max 10 bytes, max 100 items
        let lru = LruBackend::new(inner.clone(), 100, Some(10));

        lru.put("a", vec![0; 6]).await?;
        lru.put("b", vec![0; 3]).await?;
        lru.put("c", vec![0; 2]).await?; // Total 11, should evict "a"

        assert!(inner.get("a").await.is_err());
        assert!(inner.get("b").await.is_ok());
        assert!(inner.get("c").await.is_ok());

        Ok(())
    }
}
