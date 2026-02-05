use efs::crypto::standard::StandardCipher;
use efs::storage::memory::MemoryBackend;
use efs::{Efs, StorageBackend};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn test_directory_deletion_leak() {
    let backend = Arc::new(MemoryBackend::new());
    let cipher = Arc::new(StandardCipher);
    let key = vec![0u8; 32];
    let chunk_size = 4096; // Larger chunk size to avoid free list overflow

    let mut efs = Efs::new(backend.clone(), cipher.clone(), key.clone(), chunk_size)
        .await
        .unwrap();

    // 0. Initial state - put something and delete it to ensure metadata is initialized
    efs.put("/init.txt", b"init").await.unwrap();
    efs.delete("/init.txt").await.unwrap();
    let initial_block_count = backend.list().await.unwrap().len();

    // 1. Create a large directory structure
    // We want enough entries to cause B-Tree splits in the directory's index region
    efs.mkdir("/work").await.unwrap();
    for i in 0..100 {
        let path = format!("/work/file_{}.txt", i);
        let data = format!("content {}", i);
        efs.put(&path, data.as_bytes()).await.unwrap();
    }

    let middle_block_count = backend.list().await.unwrap().len();
    assert!(middle_block_count > initial_block_count);

    // 2. Delete the directory recursively
    efs.delete_recursive("/work").await.unwrap();

    let final_block_count = backend.list().await.unwrap().len();

    // The number of blocks should be significantly reduced
    assert!(final_block_count < middle_block_count);

    // All blocks in the /work region should be gone.
    // Root index still has its root block.
    assert_eq!(
        final_block_count, initial_block_count,
        "Should return to initial block count after full cleanup"
    );
}
