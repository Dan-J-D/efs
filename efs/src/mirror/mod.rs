use crate::storage::StorageBackend;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use futures::future::join_all;
use std::collections::HashMap;

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
        let futures: Vec<_> = self
            .backends
            .iter()
            .map(|b| b.put(name, data.clone()))
            .collect();
        let results: Vec<Result<()>> = join_all(futures).await;

        let success_count = results.iter().filter(|r| r.is_ok()).count();
        if success_count >= self.majority() {
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to reach majority consensus on write ({} successes out of {})",
                success_count,
                self.backends.len()
            ))
        }
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>> {
        let futures: Vec<_> = self.backends.iter().map(|b| b.get(name)).collect();
        let results: Vec<Result<Vec<u8>>> = join_all(futures).await;

        let mut responses: HashMap<Option<Vec<u8>>, usize> = HashMap::new();
        let mut last_err = None;
        let mut not_found_err = None;

        for res in results {
            match res {
                Ok(data) => {
                    *responses.entry(Some(data)).or_insert(0) += 1;
                }
                Err(e) => {
                    if crate::storage::is_not_found(&e) {
                        *responses.entry(None).or_insert(0) += 1;
                        if not_found_err.is_none() {
                            not_found_err = Some(e);
                        }
                    } else {
                        last_err = Some(e);
                    }
                }
            }
        }

        for (data, count) in responses {
            if count >= self.majority() {
                match data {
                    Some(d) => return Ok(d),
                    None => {
                        return Err(not_found_err.unwrap_or_else(|| anyhow!("Not found (consensus)")));
                    }
                }
            }
        }

        if let Some(e) = last_err {
            Err(e).context(format!("Failed to reach majority consensus on read for {}", name))
        } else {
            Err(anyhow!(
                "Failed to reach majority consensus on read for {} (conflicting data or too many failures)",
                name
            ))
        }
    }

    async fn delete(&self, name: &str) -> Result<()> {
        let futures: Vec<_> = self.backends.iter().map(|b| b.delete(name)).collect();
        let results: Vec<Result<()>> = join_all(futures).await;

        let success_count = results.iter().filter(|r| r.is_ok()).count();
        if success_count >= self.majority() {
            Ok(())
        } else {
            Err(anyhow!(
                "Failed to reach majority consensus on delete ({} successes out of {})",
                success_count,
                self.backends.len()
            ))
        }
    }

    async fn list(&self) -> Result<Vec<String>> {
        let futures: Vec<_> = self.backends.iter().map(|b| b.list()).collect();
        let results: Vec<Result<Vec<String>>> = join_all(futures).await;

        let mut counts = HashMap::new();
        let mut success_count = 0;

        for items in results.into_iter().flatten() {
            success_count += 1;
            for item in items {
                *counts.entry(item).or_insert(0) += 1;
            }
        }

        if success_count < self.majority() {
            return Err(anyhow!("Failed to reach majority consensus on list"));
        }

        let mut consensus_items = Vec::new();
        for (item, count) in counts {
            if count >= self.majority() {
                consensus_items.push(item);
            }
        }

        Ok(consensus_items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::memory::MemoryBackend;

    #[tokio::test]
    async fn test_mirror_consensus_get() -> Result<()> {
        let b1 = MemoryBackend::new();
        let b2 = MemoryBackend::new();
        let b3 = MemoryBackend::new();

        b1.put("key", b"stale".to_vec()).await?;
        b2.put("key", b"fresh".to_vec()).await?;
        b3.put("key", b"fresh".to_vec()).await?;

        let orchestrator = MirrorOrchestrator::new(vec![
            Box::new(b1),
            Box::new(b2),
            Box::new(b3),
        ]);

        let data = orchestrator.get("key").await?;
        assert_eq!(data, b"fresh".to_vec());

        Ok(())
    }

    #[tokio::test]
    async fn test_mirror_consensus_not_found() -> Result<()> {
        let b1 = MemoryBackend::new();
        let b2 = MemoryBackend::new();
        let b3 = MemoryBackend::new();

        b1.put("key", b"ghost".to_vec()).await?;

        let orchestrator = MirrorOrchestrator::new(vec![
            Box::new(b1),
            Box::new(b2),
            Box::new(b3),
        ]);

        let res = orchestrator.get("key").await;
        assert!(res.is_err());
        assert!(crate::storage::is_not_found(&res.unwrap_err()));

        Ok(())
    }

    #[tokio::test]
    async fn test_mirror_consensus_list() -> Result<()> {
        let b1 = MemoryBackend::new();
        let b2 = MemoryBackend::new();
        let b3 = MemoryBackend::new();

        b1.put("a", b"v1".to_vec()).await?;
        b2.put("a", b"v1".to_vec()).await?;
        b3.put("a", b"v1".to_vec()).await?;

        b1.put("b", b"v1".to_vec()).await?;
        b2.put("b", b"v1".to_vec()).await?;

        b3.put("c", b"v1".to_vec()).await?;

        let orchestrator = MirrorOrchestrator::new(vec![
            Box::new(b1),
            Box::new(b2),
            Box::new(b3),
        ]);

        let list = orchestrator.list().await?;
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"a".to_string()));
        assert!(list.contains(&"b".to_string()));
        assert!(!list.contains(&"c".to_string()));

        Ok(())
    }
}
