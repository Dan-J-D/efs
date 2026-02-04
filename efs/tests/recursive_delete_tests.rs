use efs::crypto::standard::StandardCipher;
use efs::storage::memory::MemoryBackend;
use efs::{Efs, StorageBackend};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread")]
async fn test_recursive_delete() {
    let backend = Arc::new(MemoryBackend::new());
    let cipher = Arc::new(StandardCipher);
    let key = vec![0u8; 32];
    let chunk_size = 1024; // Small chunk size for testing

    let mut efs = Efs::new(backend.clone(), cipher, key, chunk_size)
        .await
        .unwrap();

    // Create some files and directories
    efs.put("/a/b/c.txt", b"content c").await.unwrap();
    efs.put("/a/d.txt", b"content d").await.unwrap();
    efs.put("/e.txt", b"content e").await.unwrap();
    efs.put("/a/f/g.txt", b"content g").await.unwrap();

    // Verify they exist
    assert_eq!(efs.get("/a/b/c.txt").await.unwrap(), b"content c");
    assert_eq!(efs.get("/a/d.txt").await.unwrap(), b"content d");
    assert_eq!(efs.get("/e.txt").await.unwrap(), b"content e");
    assert_eq!(efs.get("/a/f/g.txt").await.unwrap(), b"content g");

    // List files
    let mut files = efs.index.list().await.unwrap();
    files.sort();
    assert_eq!(files, vec!["a/b/c.txt", "a/d.txt", "a/f/g.txt", "e.txt"]);

    // Delete folder /a/f/
    efs.delete_recursive("/a/f").await.unwrap();
    assert!(efs.get("/a/f/g.txt").await.is_err());

    let mut files = efs.index.list().await.unwrap();
    files.sort();
    assert_eq!(files, vec!["a/b/c.txt", "a/d.txt", "e.txt"]);

    // Delete folder /a
    efs.delete_recursive("/a").await.unwrap();
    assert!(efs.get("/a/b/c.txt").await.is_err());
    assert!(efs.get("/a/d.txt").await.is_err());

    let mut files = efs.index.list().await.unwrap();
    files.sort();
    assert_eq!(files, vec!["e.txt"]);

    // Finally delete e.txt
    efs.delete("/e.txt").await.unwrap();
    let files = efs.index.list().await.unwrap();
    assert_eq!(files.len(), 0);

    // Explicitly test region deletion for BtreeIndex
    let count_before = backend.list().await.unwrap().len();
    efs.put("/sub/1.txt", b"1").await.unwrap();
    efs.put("/sub/2.txt", b"2").await.unwrap();
    let count_middle = backend.list().await.unwrap().len();
    assert!(count_middle > count_before);

    efs.delete_recursive("/sub").await.unwrap();
    let count_after = backend.list().await.unwrap().len();
    assert!(count_after < count_middle);
}
