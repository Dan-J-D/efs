use anyhow::Result;
use efs::EfsBuilder;
use std::fs;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_recursive_preserves_empty_directories() -> Result<()> {
    let local_dir = tempdir()?;
    let empty_dir_path = local_dir.path().join("empty_dir");
    fs::create_dir(&empty_dir_path)?;

    let sub_dir_path = local_dir.path().join("sub_dir");
    fs::create_dir(&sub_dir_path)?;
    fs::write(sub_dir_path.join("file.txt"), "hello")?;

    let mut efs = EfsBuilder::new().build().await?;

    efs.put_recursive(local_dir.path().to_str().unwrap(), "root")
        .await?;

    let entries = efs.list().await?;
    println!("Entries: {:?}", entries);

    let root_entries = efs.list_dir("root").await?;
    let root_entry_names: Vec<String> = root_entries.iter().map(|(name, _)| name.clone()).collect();
    assert!(root_entry_names.contains(&"empty_dir".to_string()));
    assert!(root_entry_names.contains(&"sub_dir".to_string()));

    let sub_entries = efs.list_dir("root/sub_dir").await?;
    let sub_entry_names: Vec<String> = sub_entries.iter().map(|(name, _)| name.clone()).collect();
    assert!(sub_entry_names.contains(&"file.txt".to_string()));

    let empty_entries = efs.list_dir("root/empty_dir").await?;
    assert!(empty_entries.is_empty());

    Ok(())
}
