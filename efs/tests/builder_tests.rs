use efs::crypto::standard::StandardCipher;
use efs::index::{BPTreeStorage, BtreeIndex};
use efs::storage::local::LocalBackend;
use efs::{Efs, EfsBlockStorage, BTREE_INDEX_REGION_ID, DEFAULT_CHUNK_SIZE};
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn test_efs_builder_custom_index() {
    let temp_dir = TempDir::new().unwrap();
    let backend: Arc<dyn efs::storage::StorageBackend> =
        Arc::new(LocalBackend::new(temp_dir.path()).unwrap());
    let cipher = Arc::new(StandardCipher);
    let key = vec![0u8; 32];
    let chunk_size = DEFAULT_CHUNK_SIZE;

    // We need to load next_id to properly construct BPTreeStorage for BtreeIndex
    let storage_adapter =
        EfsBlockStorage::new(backend.clone(), cipher.clone(), key.clone(), chunk_size);
    storage_adapter.load_next_id().await.unwrap();

    let btree_storage = BPTreeStorage::new(
        storage_adapter,
        BTREE_INDEX_REGION_ID,
    );
    let btree_index: Arc<dyn efs::EfsIndex<String, efs::EfsEntry>> = Arc::new(BtreeIndex::new(btree_storage).unwrap());

    let efs = Efs::builder()
        .with_storage(backend)
        .with_cipher(cipher)
        .with_key(key)
        .with_chunk_size(chunk_size)
        .with_index(btree_index)
        .build()
        .await
        .unwrap();

    // Verify it works
    efs.put("test.txt", b"hello world").await.unwrap();
    let data = efs.get("test.txt").await.unwrap();
    assert_eq!(data, b"hello world");
}
