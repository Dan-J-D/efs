use assert_cmd::cargo::cargo_bin_cmd;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_cli_short_silo_id() -> Result<(), Box<dyn std::error::Error>> {
    let tmp_dir = tempdir()?;
    let config_path = tmp_dir.path().join("config.json");
    let backend_path = tmp_dir.path().join("backend");
    fs::create_dir(&backend_path)?;

    // 1. Add local backend
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

    // 2. Initialize silo with a very short ID (4 chars)
    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("init")
        .arg("--silo-id")
        .arg("test")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    // 3. List silo (this was reported failing)
    cargo_bin_cmd!("efs-cli")
        .arg("--config")
        .arg(&config_path)
        .arg("ls")
        .arg("--silo-id")
        .arg("test")
        .env("EFS_PASSWORD", "password123")
        .assert()
        .success();

    Ok(())
}
