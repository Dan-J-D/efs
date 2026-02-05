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

        let mut counts: HashMap<Option<Vec<u8>>, usize> = HashMap::new();

        for res in &results {
            match res {
                Ok(data) => {
                    *counts.entry(Some(data.clone())).or_insert(0) += 1;
                }
                Err(e) => {
                    if crate::storage::is_not_found(e) {
                        *counts.entry(None).or_insert(0) += 1;
                    }
                }
            }
        }

        let mut consensus = None;
        for (data, count) in counts {
            if count >= self.majority() {
                consensus = Some(data);
                break;
            }
        }

        if let Some(consensus_data) = consensus {
            match consensus_data {
                Some(data) => {
                    // Repair backends that have stale or missing data
                    for (i, res) in results.iter().enumerate() {
                        let needs_repair = match res {
                            Ok(existing_data) => existing_data != &data,
                            Err(_) => true,
                        };
                        if needs_repair {
                            let _ = self.backends[i].put(name, data.clone()).await;
                        }
                    }
                    return Ok(data);
                }
                None => {
                    // Consensus is NotFound. Repair backends that still have the data.
                    for (i, res) in results.iter().enumerate() {
                        if res.is_ok() {
                            let _ = self.backends[i].delete(name).await;
                        }
                    }
                    // Return the first NotFound error to preserve its type
                    for res in results {
                        if let Err(e) = res {
                            if crate::storage::is_not_found(&e) {
                                return Err(e);
                            }
                        }
                    }
                    return Err(anyhow!("Not found (consensus)"));
                }
            }
        }

        // No consensus. Return the first non-NotFound error or a generic error.
        for res in results {
            if let Err(e) = res {
                if !crate::storage::is_not_found(&e) {
                    return Err(e).context(format!(
                        "Failed to reach majority consensus on read for {}",
                        name
                    ));
                }
            }
        }

        Err(anyhow!(
            "Failed to reach majority consensus on read for {} (conflicting data or too many failures)",
            name
        ))
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

        let orchestrator = MirrorOrchestrator::new(vec![Box::new(b1), Box::new(b2), Box::new(b3)]);

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

        let orchestrator = MirrorOrchestrator::new(vec![Box::new(b1), Box::new(b2), Box::new(b3)]);

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

        let orchestrator = MirrorOrchestrator::new(vec![Box::new(b1), Box::new(b2), Box::new(b3)]);

        let list = orchestrator.list().await?;
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"a".to_string()));
        assert!(list.contains(&"b".to_string()));
        assert!(!list.contains(&"c".to_string()));

        Ok(())
    }

    #[tokio::test]
    async fn test_mirror_repair_on_get() -> Result<()> {
        let b1 = MemoryBackend::new();
        let b2 = MemoryBackend::new();
        let b3 = MemoryBackend::new();

        let key = "repair_key";
        let fresh_data = b"fresh_data".to_vec();
        let stale_data = b"stale_data".to_vec();

        // b1 is stale, b2 and b3 are fresh
        b1.put(key, stale_data.clone()).await?;
        b2.put(key, fresh_data.clone()).await?;
        b3.put(key, fresh_data.clone()).await?;

        let orchestrator = MirrorOrchestrator::new(vec![
            Box::new(b1.clone()),
            Box::new(b2.clone()),
            Box::new(b3.clone()),
        ]);

        // This should trigger repair of b1
        let data = orchestrator.get(key).await?;
        assert_eq!(data, fresh_data);

        // Verify b1 is now repaired
        let b1_data = b1.get(key).await?;
        assert_eq!(b1_data, fresh_data);

        // Test repair of missing data
        let key2 = "repair_key_2";
        b2.put(key2, fresh_data.clone()).await?;
        b3.put(key2, fresh_data.clone()).await?;
        // b1 doesn't have key2

        let data2 = orchestrator.get(key2).await?;
        assert_eq!(data2, fresh_data);

        // Verify b1 now has key2
        let b1_data2 = b1.get(key2).await?;
        assert_eq!(b1_data2, fresh_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_mirror_repair_not_found() -> Result<()> {
        let b1 = MemoryBackend::new();
        let b2 = MemoryBackend::new();
        let b3 = MemoryBackend::new();

        let key = "ghost_key";
        let ghost_data = b"ghost_data".to_vec();

        // b1 has it, b2 and b3 don't
        b1.put(key, ghost_data.clone()).await?;

        let orchestrator = MirrorOrchestrator::new(vec![
            Box::new(b1.clone()),
            Box::new(b2.clone()),
            Box::new(b3.clone()),
        ]);

        // Majority is NotFound
        let res = orchestrator.get(key).await;
        assert!(res.is_err());
        assert!(crate::storage::is_not_found(&res.unwrap_err()));

        // Verify b1 is repaired (it should be deleted)
        let b1_res = b1.get(key).await;
        assert!(b1_res.is_err());
        assert!(crate::storage::is_not_found(&b1_res.unwrap_err()));

        Ok(())
    }
}
