use efs::crypto::{
    Blake2b512Hasher, ChaCha20Poly1305Cipher, Pbkdf2Sha256Kdf, Sha256Hasher, Sha3_256Hasher,
    Sha512Hasher, XChaCha20Poly1305Cipher,
};
use efs::crypto::{Cipher, Hasher, Kdf};

#[test]
fn test_chacha20_flow() {
    let ciphers: Vec<Box<dyn Cipher>> = vec![
        Box::new(ChaCha20Poly1305Cipher),
        Box::new(XChaCha20Poly1305Cipher),
    ];

    for cipher in ciphers {
        let key = vec![0u8; 32];
        let ad = b"associated data";
        let plaintext = b"secret message";

        let (ciphertext, nonce, tag) = cipher.encrypt(&key, ad, plaintext).unwrap();
        assert_eq!(nonce.len(), cipher.nonce_size());
        assert_eq!(tag.len(), cipher.tag_size());
        assert_ne!(ciphertext, plaintext);

        let decrypted = cipher.decrypt(&key, ad, &nonce, &tag, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}

#[test]
fn test_pbkdf2() {
    let kdf = Pbkdf2Sha256Kdf;
    let mut output1 = [0u8; 32];
    let mut output2 = [0u8; 32];
    let salt = b"salt";

    kdf.derive(b"password", salt, &mut output1).unwrap();
    kdf.derive(b"password", salt, &mut output2).unwrap();

    assert_eq!(output1, output2);

    kdf.derive(b"wrong", salt, &mut output2).unwrap();
    assert_ne!(output1, output2);
}

#[test]
fn test_hashers() {
    let hashers: Vec<Box<dyn Hasher>> = vec![
        Box::new(Sha3_256Hasher),
        Box::new(Sha256Hasher),
        Box::new(Sha512Hasher),
        Box::new(Blake2b512Hasher),
    ];

    for hasher in hashers {
        let h1 = hasher.hash(b"data");
        let h2 = hasher.hash(b"data");
        assert_eq!(h1, h2);
        assert!(!h1.is_empty());

        let h3 = hasher.hash(b"other");
        assert_ne!(h1, h3);
    }
}
