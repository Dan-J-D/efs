use anyhow::Result;
use efs::crypto::Aes256GcmCipher;
use efs::crypto::{Hasher, Kdf};
use efs::silo::SiloManager;
use efs::storage::memory::MemoryBackend;
use std::sync::Arc;

struct MockKdf;
impl Kdf for MockKdf {
    fn derive(&self, _password: &[u8], _salt: &[u8], output: &mut [u8]) -> Result<()> {
        // Just fill with a dummy value for testing
        for i in 0..output.len() {
            output[i] = 0x42;
        }
        Ok(())
    }
}

struct MockHasher;
impl Hasher for MockHasher {
    fn hash(&self, data: &[u8]) -> Vec<u8> {
        // Return a simple XOR "hash" for testing
        let mut result = vec![0u8; 32];
        for (i, &b) in data.iter().enumerate() {
            result[i % 32] ^= b;
        }
        result
    }
}

#[tokio::test]
async fn test_custom_kdf_hasher() {
    let kdf = Box::new(MockKdf);
    let hasher = Box::new(MockHasher);
    let cipher = Box::new(Aes256GcmCipher::default());

    let silo_manager = SiloManager::new(kdf, cipher, hasher);
    let storage = Arc::new(MemoryBackend::new());

    let password = secrecy::SecretString::from("secret");
    let silo_id = "test-silo";
    let data_key = secrecy::SecretBox::new(Box::new(efs::Key32([0x13; 32])));

    // Initialize silo with custom crypto
    silo_manager
        .initialize_silo(
            storage.as_ref(),
            &password,
            silo_id,
            1024 * 1024,
            data_key.clone(),
        )
        .await
        .unwrap();

    // Load it back
    let config = silo_manager
        .load_silo(storage.as_ref(), &password, silo_id)
        .await
        .unwrap();

    use secrecy::ExposeSecret;
    assert_eq!(config.data_key.expose_secret().0, data_key.expose_secret().0);
}
