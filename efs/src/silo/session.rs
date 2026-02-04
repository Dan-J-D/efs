use crate::storage::StorageBackend;
use anyhow::{anyhow, Result};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

pub struct SessionLock {
    backend: Arc<dyn StorageBackend>,
    lock_key: String,
}

impl SessionLock {
    pub fn new(backend: Arc<dyn StorageBackend>, silo_id: &str) -> Self {
        Self {
            backend,
            lock_key: format!("session_{}.lock", silo_id),
        }
    }

    pub async fn acquire(&self) -> Result<()> {
        if let Ok(data) = self.backend.get(&self.lock_key).await {
            let timestamp = String::from_utf8(data)?.parse::<u64>()?;
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            if now - timestamp < 60 {
                return Err(anyhow!("Silo is locked by another session"));
            }
        }
        self.refresh().await
    }

    pub async fn refresh(&self) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        self.backend
            .put(&self.lock_key, now.to_string().into_bytes())
            .await
    }

    pub async fn start_heartbeat(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(30)).await;
                if let Err(e) = self.refresh().await {
                    eprintln!("Failed to refresh session lock: {}", e);
                    // In a real app, we might want to shut down or notify the user
                }
            }
        });
    }
}
