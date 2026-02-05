use efs::crypto::standard::StandardCipher;
use efs::index::{BPTreeStorage, BtreeIndex};
use efs::storage::local::LocalBackend;
use efs::{EfsEntry, EfsIndex, BTREE_INDEX_REGION_ID};
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

    let storage_adapter = efs::storage::block::EfsBlockStorage::new_with_shared_state(
        backend.clone(),
        cipher.clone(),
        key.clone(),
        chunk_size,
        next_id.clone(),
        next_id.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );

    let storage = BPTreeStorage::new(
        storage_adapter,
        BTREE_INDEX_REGION_ID,
    );

    let index = BtreeIndex::new(storage).unwrap();

    // Test put
    index
        .put(&"/a".to_string(), EfsEntry::Directory)
        .await
        .unwrap();
    index
        .put(&"/a/b".to_string(), EfsEntry::Directory)
        .await
        .unwrap();
    index
        .put(
            &"/a/b/c.txt".to_string(),
            EfsEntry::File {
                file_id: 1,
                total_size: 100,
            },
        )
        .await
        .unwrap();
    index
        .put(
            &"/a/d.txt".to_string(),
            EfsEntry::File {
                file_id: 3,
                total_size: 50,
            },
        )
        .await
        .unwrap();
    index
        .put(
            &"/e.txt".to_string(),
            EfsEntry::File {
                file_id: 4,
                total_size: 10,
            },
        )
        .await
        .unwrap();

    // Test get
    let entry = index.get(&"/a/b/c.txt".to_string()).await.unwrap().unwrap();
    if let EfsEntry::File {
        file_id,
        total_size,
    } = entry
    {
        assert_eq!(file_id, 1);
        assert_eq!(total_size, 100);
    } else {
        panic!("Expected file");
    }

    let entry = index.get(&"/a/d.txt".to_string()).await.unwrap().unwrap();
    if let EfsEntry::File {
        file_id,
        total_size,
    } = entry
    {
        assert_eq!(file_id, 3);
        assert_eq!(total_size, 50);
    } else {
        panic!("Expected file");
    }

    let entry = index.get(&"/e.txt".to_string()).await.unwrap().unwrap();
    if let EfsEntry::File {
        file_id,
        total_size,
    } = entry
    {
        assert_eq!(file_id, 4);
        assert_eq!(total_size, 10);
    } else {
        panic!("Expected file");
    }

    // Test list
    let list = index.list().await.unwrap();
    let mut paths: Vec<String> = list
        .into_iter()
        .filter_map(|(p, e)| match e {
            EfsEntry::File { .. } => Some(p),
            EfsEntry::Directory => None,
        })
        .collect();
    paths.sort();
    assert_eq!(paths, vec!["a/b/c.txt", "a/d.txt", "e.txt"]);

    // Test delete
    index.delete(&"/a/b/c.txt".to_string()).await.unwrap();
    assert!(index
        .get(&"/a/b/c.txt".to_string())
        .await
        .unwrap()
        .is_none());

    let list = index.list().await.unwrap();
    let file_count = list
        .into_iter()
        .filter(|(_, e)| matches!(e, EfsEntry::File { .. }))
        .count();
    assert_eq!(file_count, 2);

    // Test path normalization and security
    index
        .put(&"/x".to_string(), EfsEntry::Directory)
        .await
        .unwrap();
    index
        .put(
            &"/x/./y/../z.txt".to_string(),
            EfsEntry::File {
                file_id: 5,
                total_size: 20,
            },
        )
        .await
        .unwrap();
    assert!(index.get(&"/x/z.txt".to_string()).await.unwrap().is_some());

    // Test security: .. above root should stay at root
    index
        .put(
            &"/../../root_file.txt".to_string(),
            EfsEntry::File {
                file_id: 6,
                total_size: 30,
            },
        )
        .await
        .unwrap();
    assert!(index
        .get(&"/root_file.txt".to_string())
        .await
        .unwrap()
        .is_some());

    // Test .gitignore vs /.gitignore in root
    index
        .put(
            &".gitignore".to_string(),
            EfsEntry::File {
                file_id: 7,
                total_size: 70,
            },
        )
        .await
        .unwrap();
    assert!(index
        .get(&"/.gitignore".to_string())
        .await
        .unwrap()
        .is_some());
    let entry = index
        .get(&"/.gitignore".to_string())
        .await
        .unwrap()
        .unwrap();
    if let EfsEntry::File { file_id, .. } = entry {
        assert_eq!(file_id, 7);
    } else {
        panic!("Expected file");
    }

    index
        .put(
            &"/.gitignore".to_string(),
            EfsEntry::File {
                file_id: 8,
                total_size: 80,
            },
        )
        .await
        .unwrap();
    let entry = index.get(&".gitignore".to_string()).await.unwrap().unwrap();
    if let EfsEntry::File {
        file_id,
        total_size,
    } = entry
    {
        assert_eq!(file_id, 8);
        assert_eq!(total_size, 80);
    } else {
        panic!("Expected file");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_btree_index_no_implicit_creation() {
    let temp_dir = TempDir::new().unwrap();
    let backend: Arc<dyn efs::storage::StorageBackend> =
        Arc::new(LocalBackend::new(temp_dir.path()).unwrap());
    let cipher = Arc::new(StandardCipher);
    let key = vec![0u8; 32];
    let chunk_size = 1024 * 1024;
    let next_id = Arc::new(AtomicU64::new(10));

    let storage_adapter = efs::storage::block::EfsBlockStorage::new_with_shared_state(
        backend.clone(),
        cipher.clone(),
        key.clone(),
        chunk_size,
        next_id.clone(),
        next_id.clone(),
        Arc::new(tokio::sync::Mutex::new(())),
    );

    let storage = BPTreeStorage::new(
        storage_adapter,
        BTREE_INDEX_REGION_ID,
    );

    let index = BtreeIndex::new(storage).unwrap();

    // mkdir("a/b/c") should fail if "a" and "b" don't exist
    let result = index.put(&"a/b/c".to_string(), EfsEntry::Directory).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));

    // insert("x/y/z.txt") should fail if "x" and "y" don't exist
    let result = index
        .put(
            &"x/y/z.txt".to_string(),
            EfsEntry::File {
                file_id: 1,
                total_size: 10,
            },
        )
        .await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not exist"));

    // mkdir should be idempotent
    index
        .put(&"a".to_string(), EfsEntry::Directory)
        .await
        .unwrap();
    index.put(&"a".to_string(), EfsEntry::Directory).await.unwrap();
}
