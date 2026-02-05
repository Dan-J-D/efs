use crate::chunk::{Chunker, UniformEnvelope};
use crate::crypto::{Cipher, Hasher, Kdf, Key32};
use crate::storage::StorageBackend;
use anyhow::{Context, Result};
use secrecy::{ExposeSecret, SecretBox};
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

#[derive(Serialize, Deserialize, Debug, Zeroize)]
#[zeroize(drop)]
pub struct SiloConfig {
    pub chunk_size: usize,
    pub root_node_id: u64,
    pub data_key: SecretBox<Key32>,
}

pub struct SiloManager {
    kdf: Box<dyn Kdf>,
    cipher: Box<dyn Cipher>,
    hasher: Box<dyn Hasher>,
}

impl SiloManager {
    pub fn new(kdf: Box<dyn Kdf>, cipher: Box<dyn Cipher>, hasher: Box<dyn Hasher>) -> Self {
        Self {
            kdf,
            cipher,
            hasher,
        }
    }

    pub fn derive_root_key(&self, password: &secrecy::SecretString, silo_id: &str) -> Result<SecretBox<Key32>> {
        let mut root_key_bytes = [0u8; 32];
        let password_bytes = password.expose_secret().as_bytes();

        let mut context_data = password_bytes.to_vec();
        context_data.extend_from_slice(self.cipher.name().as_bytes());
        context_data.extend_from_slice(self.hasher.name().as_bytes());
        context_data.extend_from_slice(self.kdf.name().as_bytes());
        let context_hash = self.hasher.hash(&context_data);

        let mut extended_password = password_bytes.to_vec();
        extended_password.extend_from_slice(&context_hash);

        let salt = self.hasher.hash(silo_id.as_bytes());
        self.kdf
            .derive(&extended_password, &salt, &mut root_key_bytes)
            .context("Failed to derive root key")?;
        Ok(SecretBox::new(Box::new(Key32(root_key_bytes))))
    }

    pub fn derive_root_chunk_name(&self, root_key: &SecretBox<Key32>) -> String {
        let hash = self.hasher.hash(root_key.expose_secret().as_ref());
        hex::encode(hash)
    }

    pub async fn initialize_silo(
        &self,
        storage: &dyn StorageBackend,
        password: &secrecy::SecretString,
        silo_id: &str,
        chunk_size: usize,
        data_key: SecretBox<Key32>,
    ) -> Result<()> {
        let root_key = self
            .derive_root_key(password, silo_id)
            .context("Failed to derive root key for initialization")?;
        let root_name = self.derive_root_chunk_name(&root_key);

        let config = SiloConfig {
            chunk_size,
            root_node_id: 1,
            data_key,
        };

        let config_bytes =
            bincode::serialize(&config).context("Failed to serialize silo config")?;
        let payload_size = UniformEnvelope::payload_size(
            chunk_size,
            self.cipher.nonce_size(),
            self.cipher.tag_size(),
        );
        let padded_config = Chunker::pad(config_bytes, payload_size);

        let (ciphertext, nonce, tag) = self
            .cipher
            .encrypt(root_key.expose_secret().as_ref(), b"root", &padded_config)
            .context("Failed to encrypt silo config")?;
        let envelope = UniformEnvelope::new(nonce, tag, ciphertext);
        let envelope_bytes = envelope
            .serialize(chunk_size)
            .context("Failed to serialize uniform envelope for silo config")?;

        storage
            .put(&root_name, envelope_bytes)
            .await
            .context("Failed to store silo config")?;

        Ok(())
    }

    pub async fn load_silo(
        &self,
        storage: &dyn StorageBackend,
        password: &secrecy::SecretString,
        silo_id: &str,
    ) -> Result<SiloConfig> {
        let root_key = self
            .derive_root_key(password, silo_id)
            .context("Failed to derive root key for loading")?;
        let root_name = self.derive_root_chunk_name(&root_key);

        let envelope_bytes = storage
            .get(&root_name)
            .await
            .context("Failed to retrieve silo config from storage")?;
        let envelope = UniformEnvelope::deserialize(&envelope_bytes)
            .context("Failed to deserialize uniform envelope for silo config")?;

        let plaintext = self
            .cipher
            .decrypt(
                root_key.expose_secret().as_ref(),
                b"root",
                &envelope.nonce,
                &envelope.tag,
                &envelope.ciphertext,
            )
            .context("Failed to decrypt silo config")?;

        let config: SiloConfig =
            bincode::deserialize(&plaintext).context("Failed to deserialize silo config")?;
        Ok(config)
    }
}
