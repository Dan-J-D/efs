use anyhow::Result;
use efs::crypto::standard::{StandardCipher, StandardKdf};
use efs::crypto::{Cipher, Kdf};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub backends: Vec<BackendConfig>,
    pub chunk_size: usize,
    pub data_key: Option<Vec<u8>>,
    #[serde(default)]
    pub known_silos: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backends: Vec::new(),
            chunk_size: 1024 * 1024, // 1MB default
            data_key: None,
            known_silos: Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct EncryptedConfig {
    ciphertext: Vec<u8>,
    nonce: [u8; 12],
    tag: [u8; 16],
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BackendConfig {
    pub name: String,
    pub backend_type: BackendType,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum BackendType {
    S3 {
        bucket: String,
        region: String,
        access_key: Option<String>,
        secret_key: Option<String>,
    },
    Local {
        path: String,
    },
}

pub fn load_config<P: AsRef<Path>>(path: P, password: &[u8]) -> Result<Config> {
    if !path.as_ref().exists() {
        return Ok(Config::default());
    }
    let content = fs::read(path)?;

    if let Ok(encrypted) = bincode::deserialize::<EncryptedConfig>(&content) {
        let mut key = [0u8; 32];
        StandardKdf.derive(password, b"efs_config_salt", &mut key)?;

        let plaintext = StandardCipher.decrypt(
            &key,
            b"config",
            &encrypted.nonce,
            &encrypted.tag,
            &encrypted.ciphertext,
        )?;

        Ok(bincode::deserialize::<Config>(&plaintext)?)
    } else if let Ok(config) = bincode::deserialize::<Config>(&content) {
        Ok(config)
    } else {
        // Fallback to JSON if it's still in that format
        let content_str = String::from_utf8_lossy(&content);
        if let Ok(config) = serde_json::from_str::<Config>(&content_str) {
            return Ok(config);
        }
        if let Ok(encrypted) = serde_json::from_str::<EncryptedConfig>(&content_str) {
            let mut key = [0u8; 32];
            StandardKdf.derive(password, b"efs_config_salt", &mut key)?;
            let plaintext = StandardCipher.decrypt(
                &key,
                b"config",
                &encrypted.nonce,
                &encrypted.tag,
                &encrypted.ciphertext,
            )?;
            return Ok(serde_json::from_slice::<Config>(&plaintext)?);
        }

        Err(anyhow::anyhow!("Failed to load config: unknown format"))
    }
}

pub fn save_config<P: AsRef<Path>>(path: P, config: &Config, password: &[u8]) -> Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let plaintext = bincode::serialize(config)?;
    let mut key = [0u8; 32];
    StandardKdf.derive(password, b"efs_config_salt", &mut key)?;

    let (ciphertext, nonce, tag) = StandardCipher.encrypt(&key, b"config", &plaintext)?;

    let encrypted = EncryptedConfig {
        ciphertext,
        nonce,
        tag,
    };

    let content = bincode::serialize(&encrypted)?;
    fs::write(path, content)?;
    Ok(())
}
