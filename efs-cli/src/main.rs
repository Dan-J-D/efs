use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use efs::crypto::{Aes256GcmCipher, Argon2Kdf, Blake3Hasher};
use efs::mirror::MirrorOrchestrator;
use efs::silo::SiloManager;
use efs::storage::local::LocalBackend;
use efs::storage::s3::S3Backend;
use efs::{Efs, StorageBackend};
use secrecy::{ExposeSecret, SecretBox, SecretString};
use std::sync::Arc;

mod config;
use config::{BackendConfig, BackendType};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to the configuration file
    #[arg(short, long)]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new silo
    Init {
        #[arg(short, long)]
        silo_id: String,
        #[arg(short, long, default_value_t = 1024 * 1024)]
        chunk_size: usize,
    },
    /// Add a backend to the config (S3)
    AddS3 {
        #[arg(short, long)]
        name: String,
        #[arg(short, long)]
        bucket: String,
        #[arg(short, long)]
        region: String,
        #[arg(long)]
        access_key: Option<String>,
        #[arg(long)]
        secret_key: Option<String>,
    },
    /// Add a backend to the config (Local)
    AddLocal {
        #[arg(short, long)]
        name: String,
        #[arg(short, long)]
        path: String,
    },
    /// Add a file to a silo
    Put {
        #[arg(short, long)]
        silo_id: String,
        local_path: String,
        remote_path: String,
    },
    /// Retrieve a file from a silo
    Get {
        #[arg(short, long)]
        silo_id: String,
        remote_path: String,
        local_path: String,
    },
    /// List files in a silo
    Ls {
        #[arg(short, long)]
        silo_id: String,
    },
    /// Delete a file or folder from a silo
    Delete {
        #[arg(short, long)]
        silo_id: String,
        remote_path: String,
        /// Recursive deletion for folders
        #[arg(short, long)]
        recursive: bool,
    },
    /// Create a new directory in a silo
    Mkdir {
        #[arg(short, long)]
        silo_id: String,
        remote_path: String,
    },
    /// Sync mirrors
    Sync {
        #[arg(short, long)]
        silo_id: String,
    },
    /// Show current configuration
    ShowConfig {
        /// Show sensitive information like keys
        #[arg(long)]
        show_keys: bool,
    },
}

fn get_password() -> Result<String> {
    if let Ok(pass) = std::env::var("EFS_PASSWORD") {
        return Ok(pass);
    }
    rpassword::prompt_password("Enter silo password: ").map_err(|e| anyhow!("{}", e))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let cli = Cli::parse();
    let password = SecretString::from(get_password()?);
    let mut cfg = config::load_config(&cli.config, password.expose_secret().as_bytes())?;

    match &cli.command {
        Commands::Init {
            silo_id,
            chunk_size,
        } => {
            println!("Initializing silo: {}", silo_id);

            let mut data_key_bytes = [0u8; 32];
            use rand::RngCore;
            rand::thread_rng().fill_bytes(&mut data_key_bytes);
            let data_key = SecretBox::new(Box::new(efs::Key32(data_key_bytes)));

            cfg.chunk_size = *chunk_size;
            config::save_config(&cli.config, &cfg, password.expose_secret().as_bytes())?;


            let storage = get_storage(&cfg).await?;
            let manager = SiloManager::new(
                Box::new(Argon2Kdf::default()),
                Box::new(Aes256GcmCipher::default()),
                Box::new(Blake3Hasher::default()),
            );

            manager
                .initialize_silo(
                    storage.as_ref(),
                    &password,
                    silo_id,
                    cfg.chunk_size,
                    data_key,
                )
                .await?;
            println!("Silo initialized successfully and config updated.");
        }
        Commands::AddS3 {
            name,
            bucket,
            region,
            access_key,
            secret_key,
        } => {
            cfg.backends.push(BackendConfig {
                name: name.clone(),
                backend_type: BackendType::S3 {
                    bucket: bucket.clone(),
                    region: region.clone(),
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                },
            });
            config::save_config(&cli.config, &cfg, password.expose_secret().as_bytes())?;
            println!("S3 backend '{}' added.", name);
        }
        Commands::AddLocal { name, path } => {
            cfg.backends.push(BackendConfig {
                name: name.clone(),
                backend_type: BackendType::Local { path: path.clone() },
            });
            config::save_config(&cli.config, &cfg, password.expose_secret().as_bytes())?;
            println!("Local backend '{}' added at {}.", name, path);
        }
        Commands::Put {
            silo_id,
            local_path,
            remote_path,
        } => {
            let storage = get_storage(&cfg).await?;
            let manager = SiloManager::new(
                Box::new(Argon2Kdf::default()),
                Box::new(Aes256GcmCipher::default()),
                Box::new(Blake3Hasher::default()),
            );

            let silo_cfg = manager
                .load_silo(storage.as_ref(), &password, silo_id)
                .await?;

            let efs = Efs::new(
                storage,
                Arc::new(Aes256GcmCipher::default()),
                Arc::new(Blake3Hasher::default()),
                silo_cfg.data_key.clone(),
                silo_cfg.chunk_size,
            )
            .await?;

            efs.put_recursive(local_path, remote_path).await?;
            println!("Upload complete.");
        }
        Commands::Get {
            silo_id,
            remote_path,
            local_path,
        } => {
            let storage = get_storage(&cfg).await?;
            let manager = SiloManager::new(
                Box::new(Argon2Kdf::default()),
                Box::new(Aes256GcmCipher::default()),
                Box::new(Blake3Hasher::default()),
            );

            let silo_cfg = manager
                .load_silo(storage.as_ref(), &password, silo_id)
                .await?;

            let efs = Efs::new(
                storage,
                Arc::new(Aes256GcmCipher::default()),
                Arc::new(Blake3Hasher::default()),
                silo_cfg.data_key.clone(),
                silo_cfg.chunk_size,
            )
            .await?;
            let data = efs.get(remote_path).await?;
            std::fs::write(local_path, data)?;
            println!("File downloaded successfully.");
        }
        Commands::Ls { silo_id } => {
            let storage = get_storage(&cfg).await?;
            let manager = SiloManager::new(
                Box::new(Argon2Kdf::default()),
                Box::new(Aes256GcmCipher::default()),
                Box::new(Blake3Hasher::default()),
            );

            let silo_cfg = manager
                .load_silo(storage.as_ref(), &password, silo_id)
                .await?;

            let efs = Efs::new(
                storage,
                Arc::new(Aes256GcmCipher::default()),
                Arc::new(Blake3Hasher::default()),
                silo_cfg.data_key.clone(),
                silo_cfg.chunk_size,
            )
            .await?;
            for path in efs.list().await? {
                println!("{}", path);
            }
        }
        Commands::Delete {
            silo_id,
            remote_path,
            recursive,
        } => {
            let storage = get_storage(&cfg).await?;
            let manager = SiloManager::new(
                Box::new(Argon2Kdf::default()),
                Box::new(Aes256GcmCipher::default()),
                Box::new(Blake3Hasher::default()),
            );

            let silo_cfg = manager
                .load_silo(storage.as_ref(), &password, silo_id)
                .await?;

            let efs = Efs::new(
                storage,
                Arc::new(Aes256GcmCipher::default()),
                Arc::new(Blake3Hasher::default()),
                silo_cfg.data_key.clone(),
                silo_cfg.chunk_size,
            )
            .await?;

            if *recursive {
                efs.delete_recursive(remote_path).await?;
                println!("Folder deleted successfully.");
            } else {
                efs.delete(remote_path).await?;
                println!("File deleted successfully.");
            }
        }
        Commands::Mkdir {
            silo_id,
            remote_path,
        } => {
            let storage = get_storage(&cfg).await?;
            let manager = SiloManager::new(
                Box::new(Argon2Kdf::default()),
                Box::new(Aes256GcmCipher::default()),
                Box::new(Blake3Hasher::default()),
            );

            let silo_cfg = manager
                .load_silo(storage.as_ref(), &password, silo_id)
                .await?;

            let efs = Efs::new(
                storage,
                Arc::new(Aes256GcmCipher::default()),
                Arc::new(Blake3Hasher::default()),
                silo_cfg.data_key.clone(),
                silo_cfg.chunk_size,
            )
            .await?;

            efs.mkdir(remote_path).await?;
            println!("Directory created successfully.");
        }
        Commands::Sync { silo_id: _ } => {
            println!("Syncing mirrors...");
            let storage = get_storage(&cfg).await?;
            let keys = storage.list().await?;
            println!(
                "Found {} blocks. Ensuring all mirrors have them...",
                keys.len()
            );
            for key in keys {
                match storage.get(&key).await {
                    Ok(data) => {
                        storage.put(&key, data).await?;
                    }
                    Err(e) => eprintln!("Failed to sync block {}: {}", key, e),
                }
            }
            println!("Sync complete.");
        }
        Commands::ShowConfig { show_keys } => {
            println!("Current Configuration:");
            if !show_keys {
                let mut hidden_cfg = serde_json::to_value(&cfg)?;
                if let Some(obj) = hidden_cfg.as_object_mut() {
                    if obj.contains_key("data_key") {
                        obj.insert("data_key".to_string(), serde_json::json!("***"));
                    }
                    if let Some(backends) = obj.get_mut("backends").and_then(|b| b.as_array_mut()) {
                        for backend in backends {
                            if let Some(b_obj) = backend.as_object_mut() {
                                if let Some(b_type) = b_obj
                                    .get_mut("backend_type")
                                    .and_then(|t| t.as_object_mut())
                                {
                                    if let Some(s3) =
                                        b_type.get_mut("S3").and_then(|s| s.as_object_mut())
                                    {
                                        if s3.get("access_key").and_then(|v| v.as_str()).is_some() {
                                            s3.insert(
                                                "access_key".to_string(),
                                                serde_json::json!("***"),
                                            );
                                        }
                                        if s3.get("secret_key").and_then(|v| v.as_str()).is_some() {
                                            s3.insert(
                                                "secret_key".to_string(),
                                                serde_json::json!("***"),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                println!("{}", serde_json::to_string_pretty(&hidden_cfg)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&cfg)?);
            }
        }
    }

    Ok(())
}

async fn get_storage(cfg: &config::Config) -> Result<Arc<dyn StorageBackend>> {
    if cfg.backends.is_empty() {
        return Err(anyhow!("No backends configured. Use 'add-backend' first."));
    }

    let mut backends = Vec::new();

    for b_cfg in &cfg.backends {
        match &b_cfg.backend_type {
            BackendType::S3 {
                bucket,
                region,
                access_key,
                secret_key,
            } => {
                use object_store::aws::AmazonS3Builder;
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region);

                if let Some(key) = access_key {
                    builder = builder.with_access_key_id(key);
                }
                if let Some(secret) = secret_key {
                    builder = builder.with_secret_access_key(secret);
                }

                let store = builder.build()?;
                let backend = S3Backend::new(Arc::new(store));
                backends.push(Box::new(backend) as Box<dyn StorageBackend>);
            }
            BackendType::Local { path } => {
                let backend = LocalBackend::new(path)?;
                backends.push(Box::new(backend) as Box<dyn StorageBackend>);
            }
        }
    }

    Ok(Arc::new(MirrorOrchestrator::new(backends)))
}
