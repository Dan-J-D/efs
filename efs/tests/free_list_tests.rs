use efs::crypto::standard::StandardCipher;
use efs::storage::memory::MemoryBackend;
use efs::{Efs, DEFAULT_CHUNK_SIZE};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn test_block_reuse() {
    let backend = Arc::new(MemoryBackend::new());
    let cipher = Arc::new(StandardCipher);
    let key = vec![0u8; 32];
    let chunk_size = 1024;

    let mut efs = Efs::new(backend.clone(), cipher, key, chunk_size).await.unwrap();

    // 1. Put a file
    let data = vec![0u8; 5000]; // 5 chunks
    efs.put("test_file", &data).await.unwrap();
    
    let next_id_after_put = efs.storage_adapter.next_id().load(std::sync::atomic::Ordering::SeqCst);
    
    // 2. Delete the file
    efs.delete("test_file").await.unwrap();
    
    let free_list = efs.storage_adapter.free_list().lock().unwrap().clone();
    assert!(!free_list.is_empty(), "Free list should not be empty after deletion");
    assert_eq!(free_list.len(), 6, "Should have 6 blocks in free list");

    // 3. Put another file of same size
    efs.put("test_file2", &data).await.unwrap();
    
    let next_id_after_reuse = efs.storage_adapter.next_id().load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(next_id_after_put, next_id_after_reuse, "next_id should not have increased because of reuse");
    
    let free_list_after = efs.storage_adapter.free_list().lock().unwrap().clone();
    assert!(free_list_after.is_empty(), "Free list should be empty after reuse");
}
