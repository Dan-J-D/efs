use efs::crypto::{Argon2Kdf, Blake2bHasher, Hasher, Kdf, Pbkdf2Sha256Kdf};

#[test]
fn test_configurable_argon2() {
    let kdf_default = Argon2Kdf::default();
    let kdf_custom = Argon2Kdf {
        m_cost: 16384, // 16MB instead of 64MB
        t_cost: 1,
        p_cost: 1,
    };

    let password = b"password";
    let salt = b"salt-salt-salt-salt";
    let mut out_default = [0u8; 32];
    let mut out_custom = [0u8; 32];

    kdf_default
        .derive(password, salt, &mut out_default)
        .unwrap();
    kdf_custom.derive(password, salt, &mut out_custom).unwrap();

    assert_ne!(out_default, out_custom);
}

#[test]
fn test_configurable_pbkdf2() {
    let kdf_default = Pbkdf2Sha256Kdf::default();
    let kdf_custom = Pbkdf2Sha256Kdf { iterations: 1000 };

    let password = b"password";
    let salt = b"salt";
    let mut out_default = [0u8; 32];
    let mut out_custom = [0u8; 32];

    kdf_default
        .derive(password, salt, &mut out_default)
        .unwrap();
    kdf_custom.derive(password, salt, &mut out_custom).unwrap();

    assert_ne!(out_default, out_custom);
}

#[test]
fn test_configurable_blake2b() {
    let hasher_default = Blake2bHasher::default();
    let hasher_custom = Blake2bHasher {
        output_size: 32, // 256 bits instead of 512 bits
    };

    let data = b"some data";
    let h_default = hasher_default.hash(data);
    let h_custom = hasher_custom.hash(data);

    assert_eq!(h_default.len(), 64);
    assert_eq!(h_custom.len(), 32);
    assert_ne!(h_default[..32], h_custom);
}
