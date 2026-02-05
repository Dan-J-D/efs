use efs::crypto::Aes256GcmCipher;
use efs::index::{BPTreeStorage, BtreeIndex};
use efs::storage::local::LocalBackend;
use efs::storage::lru::LruBackend;
use efs::storage::memory::MemoryBackend;
use efs::storage::s3::S3Backend;
use efs::{Efs, BTREE_INDEX_REGION_ID, DEFAULT_CHUNK_SIZE};
use object_store::memory::InMemory;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn test_memory_kv_config() {
    let storage = Arc::new(MemoryBackend::new());
    let cipher = Arc::new(Aes256GcmCipher);
    let key = vec![0u8; 32];

    let efs = Efs::new(storage, cipher, key, DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();

    let data = b"memory + kv index test data";
    efs.put("test_file", data).await.unwrap();

    let retrieved = efs.get("test_file").await.unwrap();
    assert_eq!(data.to_vec(), retrieved);

    let list = efs.list().await.unwrap();
    assert_eq!(list, vec!["test_file"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_local_btree_config() {
    let tmp = tempdir().unwrap();
    let storage = Arc::new(LocalBackend::new(tmp.path()).unwrap());
    let cipher = Arc::new(Aes256GcmCipher);
    let key = vec![0u8; 32];

    let efs_temp = Efs::new(
        storage.clone(),
        cipher.clone(),
        key.clone(),
        DEFAULT_CHUNK_SIZE,
    )
    .await
    .unwrap();
    
    let index_storage = BPTreeStorage::new(
        efs_temp.storage_adapter.clone(),
        BTREE_INDEX_REGION_ID,
    );
    let index: Arc<dyn efs::EfsIndex<String, efs::EfsEntry>> = Arc::new(BtreeIndex::new(index_storage).unwrap());

    let efs = Efs::builder()
        .with_storage(storage)
        .with_cipher(cipher)
        .with_key(key)
        .with_index(index)
        .build()
        .await
        .unwrap();

    let data = b"local + btree index test data";
    efs.mkdir("/dir1").await.unwrap();
    efs.mkdir("/dir1/dir2").await.unwrap();
    efs.put("/dir1/dir2/test_file", data).await.unwrap();

    let retrieved = efs.get("/dir1/dir2/test_file").await.unwrap();
    assert_eq!(data.to_vec(), retrieved);

    let list = efs.list().await.unwrap();
    assert!(list.contains(&"dir1/dir2/test_file".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lru_memory_kv_config() {
    let inner_storage = Arc::new(MemoryBackend::new());
    let storage = Arc::new(LruBackend::new(inner_storage, 10, None)); // 10 blocks capacity
    let cipher = Arc::new(Aes256GcmCipher);
    let key = vec![0u8; 32];

    let efs = Efs::new(storage, cipher, key, DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();

    let data = b"lru cache test data";
    efs.put("cached_file", data).await.unwrap();

    let retrieved = efs.get("cached_file").await.unwrap();
    assert_eq!(data.to_vec(), retrieved);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_local_kv_persistence() {
    let tmp = tempdir().unwrap();
    let storage = Arc::new(LocalBackend::new(tmp.path()).unwrap());
    let cipher = Arc::new(Aes256GcmCipher);
    let key = vec![0u8; 32];

    let data = b"persistence test data";
    {
        let efs = Efs::new(
            storage.clone(),
            cipher.clone(),
            key.clone(),
            DEFAULT_CHUNK_SIZE,
        )
        .await
        .unwrap();
        efs.put("persistent_file", data).await.unwrap();
    }

    // Re-open
    {
        let efs = Efs::new(storage, cipher, key, DEFAULT_CHUNK_SIZE)
            .await
            .unwrap();
        let retrieved = efs.get("persistent_file").await.unwrap();
        assert_eq!(data.to_vec(), retrieved);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_small_chunks() {
    let storage = Arc::new(MemoryBackend::new());
    let cipher = Arc::new(Aes256GcmCipher);
    let key = vec![0u8; 32];
    let chunk_size = 1024; // 1KB chunks

    let efs = Efs::new(storage, cipher, key, chunk_size).await.unwrap();

    let data = vec![0u8; 5000]; // Should span multiple chunks
    efs.put("large_file", &data).await.unwrap();

    let retrieved = efs.get("large_file").await.unwrap();
    assert_eq!(data, retrieved);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_s3_mock_config() {
    let store = Arc::new(InMemory::new());
    let storage = Arc::new(S3Backend::new(store));
    let cipher = Arc::new(Aes256GcmCipher);
    let key = vec![0u8; 32];

    let efs = Efs::new(storage, cipher, key, DEFAULT_CHUNK_SIZE)
        .await
        .unwrap();

    let data = b"s3 mock test data";
    efs.put("s3_file", data).await.unwrap();

    let retrieved = efs.get("s3_file").await.unwrap();
    assert_eq!(data.to_vec(), retrieved);
}
