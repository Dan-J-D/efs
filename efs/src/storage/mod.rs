use anyhow::Result;
use async_trait::async_trait;
use bptree::storage::BlockId;

#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, name: &str, data: Vec<u8>) -> Result<()>;
    async fn get(&self, name: &str) -> Result<Vec<u8>>;
    async fn delete(&self, name: &str) -> Result<()>;
    async fn list(&self) -> Result<Vec<String>>;
}

pub type RegionId = u64;

pub const ROOT_BLOCK_ID: BlockId = 0;
pub const ALLOCATOR_STATE_BLOCK_ID: BlockId = 0;
pub const FREE_LIST_BLOCK_ID: BlockId = 1;

pub const METADATA_REGION_ID: RegionId = 0;
pub const BTREE_REGION_ID: RegionId = 2;
pub const FILE_DATA_REGION_ID: RegionId = 1;

pub mod block;
pub mod cache;
pub mod local;
pub mod lru;
pub mod memory;
pub mod s3;
