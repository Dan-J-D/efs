use efs::crypto::standard::StandardCipher;
use efs::index::{BPTreeStorage, BtreeIndex};
use efs::storage::local::LocalBackend;
use efs::{EfsIndex, BTREE_REGION_ID};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn test_btree_index_hierarchical() {
    let temp_dir = TempDir::new().unwrap();
    let backend: Arc<dyn efs::storage::StorageBackend> =
        Arc::new(LocalBackend::new(temp_dir.path()).unwrap());
    let cipher = Arc::new(StandardCipher);
    let key = vec![0u8; 32];
    let chunk_size = 1024 * 1024;
    let next_id = Arc::new(AtomicU64::new(10)); // Start from 10 to avoid collisions with reserved blocks

    let storage = BPTreeStorage::new(
        backend.clone(),
        cipher.clone(),
        key.clone(),
        next_id.clone(),
        chunk_size,
        BTREE_REGION_ID,
    );

    let index = BtreeIndex::new(storage).unwrap();

    // Test insert
    index.insert("/a/b/c.txt", vec![1, 2], 100).await.unwrap();
    index.insert("/a/d.txt", vec![3], 50).await.unwrap();
    index.insert("/e.txt", vec![4], 10).await.unwrap();

    // Test get
    let (blocks, size) = index.get("/a/b/c.txt").await.unwrap().unwrap();
    assert_eq!(blocks, vec![1, 2]);
    assert_eq!(size, 100);

    let (blocks, size) = index.get("/a/d.txt").await.unwrap().unwrap();
    assert_eq!(blocks, vec![3]);
    assert_eq!(size, 50);

    let (blocks, size) = index.get("/e.txt").await.unwrap().unwrap();
    assert_eq!(blocks, vec![4]);
    assert_eq!(size, 10);

    // Test list
    let mut list = index.list().await.unwrap();
    list.sort();
    assert_eq!(list, vec!["a/b/c.txt", "a/d.txt", "e.txt"]);

    // Test delete
    index.delete("/a/b/c.txt").await.unwrap();
    assert!(index.get("/a/b/c.txt").await.unwrap().is_none());

    let list = index.list().await.unwrap();
    assert_eq!(list.len(), 2);

    // Test path normalization and security
    index.insert("/x/./y/../z.txt", vec![5], 20).await.unwrap();
    assert!(index.get("/x/z.txt").await.unwrap().is_some());

    // Test security: .. above root should stay at root
    index
        .insert("/../../root_file.txt", vec![6], 30)
        .await
        .unwrap();
    assert!(index.get("/root_file.txt").await.unwrap().is_some());

    // Test .gitignore vs /.gitignore in root
    index.insert(".gitignore", vec![7], 70).await.unwrap();
    assert!(index.get("/.gitignore").await.unwrap().is_some());
    let (blocks, _) = index.get("/.gitignore").await.unwrap().unwrap();
    assert_eq!(blocks, vec![7]);

    index.insert("/.gitignore", vec![8], 80).await.unwrap();
    let (blocks, size) = index.get(".gitignore").await.unwrap().unwrap();
    assert_eq!(blocks, vec![8]);
    assert_eq!(size, 80);
}
