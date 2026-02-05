use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_cli_full_flow() -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = tempdir()?;
    let config_path = tmp_dir.path().join("config.json");
    let backend_path = tmp_dir.path().join("backend");
    fs::create_dir(&backend_path)?;

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("add-local")
        .arg("--name")
        .arg("local1")
        .arg("--path")
        .arg(&backend_path)
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("init")
        .arg("--silo-id")
        .arg("test-silo")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    let file_to_upload = tmp_dir.path().join("hello.txt");
    fs::write(&file_to_upload, "hello world")?;

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("mkdir")
        .arg("--silo-id")
        .arg("test-silo")
        .arg("remote")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("put")
        .arg("--silo-id")
        .arg("test-silo")
        .arg(&file_to_upload)
        .arg("remote/hello.txt")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("ls")
        .arg("--silo-id")
        .arg("test-silo")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success()
        .stdout(predicates::str::contains("remote/hello.txt"));

    let downloaded_file = tmp_dir.path().join("downloaded.txt");
    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("get")
        .arg("--silo-id")
        .arg("test-silo")
        .arg("remote/hello.txt")
        .arg(&downloaded_file)
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    let content = fs::read_to_string(downloaded_file)?;
    assert_eq!(content, "hello world");

    // Test Delete
    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("delete")
        .arg("--silo-id")
        .arg("test-silo")
        .arg("remote/hello.txt")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("ls")
        .arg("--silo-id")
        .arg("test-silo")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success()
        .stdout(predicates::str::contains("remote/hello.txt").not());

    Ok(())
}

#[test]
fn test_cli_custom_chunk_size() -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = tempdir()?;
    let config_path = tmp_dir.path().join("config.json");
    let backend_path = tmp_dir.path().join("backend");
    fs::create_dir(&backend_path)?;

    // Use 64KB chunks
    let chunk_size = 64 * 1024;

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("add-local")
        .arg("--name")
        .arg("local1")
        .arg("--path")
        .arg(&backend_path)
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("init")
        .arg("--silo-id")
        .arg("test-silo")
        .arg("--chunk-size")
        .arg(chunk_size.to_string())
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    let file_to_upload = tmp_dir.path().join("large.bin");
    let data = vec![0u8; 200 * 1024]; // 200KB, should be ~4 chunks
    fs::write(&file_to_upload, &data)?;

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("put")
        .arg("--silo-id")
        .arg("test-silo")
        .arg(&file_to_upload)
        .arg("large.bin")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    // Verify number of blocks.
    // Each block is exactly chunk_size.
    let entries: Vec<_> = fs::read_dir(&backend_path)?.collect();
    // 200KB / (64KB - overhead) = 200 / 63.9... -> 4 chunks for data
    // Plus 1 for root silo config
    // Plus B+ Tree nodes. B+ Tree starts with root (1 node).
    // Total should be at least 6.
    assert!(entries.len() >= 6);

    let downloaded_file = tmp_dir.path().join("downloaded.bin");
    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("get")
        .arg("--silo-id")
        .arg("test-silo")
        .arg("large.bin")
        .arg(&downloaded_file)
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    let downloaded_data = fs::read(downloaded_file)?;
    assert_eq!(data, downloaded_data);

    Ok(())
}

#[test]
fn test_cli_folder_upload() -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = tempdir()?;
    let config_path = tmp_dir.path().join("config.json");
    let backend_path = tmp_dir.path().join("backend");
    fs::create_dir(&backend_path)?;

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("add-local")
        .arg("--name")
        .arg("local1")
        .arg("--path")
        .arg(&backend_path)
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("init")
        .arg("--silo-id")
        .arg("test-silo")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    let folder_to_upload = tmp_dir.path().join("my_folder");
    fs::create_dir(&folder_to_upload)?;
    fs::write(folder_to_upload.join("file1.txt"), "content1")?;
    fs::create_dir(folder_to_upload.join("subfolder"))?;
    fs::write(folder_to_upload.join("subfolder/file2.txt"), "content2")?;

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("put")
        .arg("--silo-id")
        .arg("test-silo")
        .arg(&folder_to_upload)
        .arg("remote_folder")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("ls")
        .arg("--silo-id")
        .arg("test-silo")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success()
        .stdout(predicates::str::contains("remote_folder/file1.txt"))
        .stdout(predicates::str::contains(
            "remote_folder/subfolder/file2.txt",
        ));

    Ok(())
}
