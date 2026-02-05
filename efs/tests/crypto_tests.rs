use efs::crypto::{Aes256GcmCipher, Blake3Hasher, Argon2Kdf};
use efs::crypto::{Cipher, Hasher, Kdf};

#[test]
fn test_standard_crypto_flow() {
    let cipher = Aes256GcmCipher::default();
    let key = [0u8; 32];
    let ad = b"associated data";
    let plaintext = b"secret message";

    let (ciphertext, nonce, tag) = cipher.encrypt(&key, ad, plaintext).unwrap();
    assert_ne!(ciphertext, plaintext);

    let decrypted = cipher.decrypt(&key, ad, &nonce, &tag, &ciphertext).unwrap();
    assert_eq!(decrypted, plaintext);
}

#[test]
fn test_kdf() {
    let kdf = Argon2Kdf::default();
    let mut output1 = [0u8; 32];
    let mut output2 = [0u8; 32];
    let salt = b"salt-must-be-long";

    kdf.derive(b"password", salt, &mut output1).unwrap();
    kdf.derive(b"password", salt, &mut output2).unwrap();

    assert_eq!(output1, output2);

    kdf.derive(b"wrong", salt, &mut output2).unwrap();

    assert_ne!(output1, output2);
}

#[test]
fn test_hasher() {
    let hasher = Blake3Hasher::default();
    let h1 = hasher.hash(b"data");
    let h2 = hasher.hash(b"data");
    assert_eq!(h1, h2);

    let h3 = hasher.hash(b"other");
    assert_ne!(h1, h3);
}
