# EFS Agent Guidelines

This document provides essential instructions for agentic coding tools operating within the `efs` repository. Adhere to these standards to maintain consistency and safety.

## 1. Execution Commands

The project is a Rust workspace consisting of the `efs` library, `efs-cli`, and `efs-tui`.

### Build & Lint
- **Build Workspace:** `cargo build`
- **Build Library:** `cargo build -p efs`
- **Build CLI:** `cargo build -p efs-cli`
- **Build TUI:** `cargo build -p efs-tui`
- **Check (Fast):** `cargo check`
- **Lint:** `cargo clippy --workspace -- -D warnings`
- **Format:** `cargo fmt --all`

### Testing
- **Run all tests:** `cargo test`
- **Run library tests:** `cargo test -p efs`
- **Run a specific test file:** `cargo test -p efs --test crypto_tests`
- **Run a single test function:** `cargo test -p efs --test crypto_tests test_kdf`
- **Show test output:** `cargo test -- --nocapture`

---

## 2. Code Style & Conventions

### Imports
- Group imports in the following order:
  1. Standard library (`std::...`)
  2. External crates (`anyhow`, `serde`, etc.)
  3. Internal modules (`crate::...` or `super::...`)
- Prefer explicit imports over glob imports (avoid `use crate::crypto::*;`).

### Naming Conventions
- **Files/Modules:** `snake_case` (e.g., `storage/s3.rs`).
- **Structs/Enums/Traits:** `PascalCase` (e.g., `StorageBackend`).
- **Functions/Variables:** `snake_case` (e.g., `derive_root_chunk_name`).
- **Constants:** `SCREAMING_SNAKE_CASE` (e.g., `DEFAULT_CHUNK_SIZE`).

### Error Handling
- Use `anyhow::Result<T>` for functions that can fail.
- Use `anyhow::anyhow!("context: {}", error)` for creating ad-hoc errors.
- Library code should provide meaningful context to errors using `.context("...")`.
- Avoid `unwrap()` or `expect()` in library code unless in tests or if the invariant is locally guaranteed.

### Concurrency & Async
- The core logic is asynchronous. Use `tokio` for runtime operations.
- Use `#[async_trait]` for traits involving I/O (Storage, etc.).
- Shared state should be wrapped in `Arc<T>` or `Arc<dyn Trait + Send + Sync>`.
- Use `tokio::sync::Mutex` instead of `std::sync::Mutex` for locks held across `.await` points.

---

## 3. Architecture & Data Structures

### Trait-First Design
The library decouples logic via traits in `efs/src/lib.rs`. Always implement against traits (`Cipher`, `Hasher`, `Kdf`, `StorageBackend`, `EfsIndex`) to allow for mocking and future-proofing.

### The Uniform Envelope & AAD
Data uniformity is critical for deniability.
- Every chunk must be padded to exactly the configured chunk size (default 1MB).
- Use `UniformEnvelope` for serialization:
  ```rust
  #[derive(Serialize, Deserialize)]
  pub struct UniformEnvelope {
      pub nonce: [u8; 12],
      pub tag: [u8; 16],
      pub ciphertext: Vec<u8>,
  }
  ```
- **AAD (Associated Authenticated Data):** Always use deterministic metadata (e.g., `RegionId` and `BlockId`) as AAD when encrypting chunks. This prevents "block swapping" attacks.

### Deterministic Naming & Regions
To maintain deniability, chunk names in the storage backend must be deterministic but appear random:
- Use `blake3(key || context_salt || block_id)` for chunk names.
- `RegionId` separates different types of data (used as AAD):
  - `METADATA_REGION_ID` (0): Allocator state and root metadata.
  - `FILE_DATA_REGION_ID` (1): Actual encrypted file chunks.
  - `BTREE_INDEX_REGION_ID` (2): All B-Tree index nodes for all directories.
- **Context Salts:** Instead of separate `RegionId`s, isolation between directories is achieved using path-derived salts. The salt is derived as `blake3(silo_key || parent_salt || folder_name)`.
- `EfsBlockStorage` manages the mapping between logical blocks and physical storage names.

### Paths & Naming
- Always use `crate::path::normalize_path` for user-provided paths.
- Paths are normalized to strip leading/trailing slashes and resolve `.` and `..`.
- The root directory is represented by an empty string `""`.
- **Naming Whitelist:** Only ASCII alphanumeric characters, dots (`.`), hyphens (`-`), and underscores (`_`) are allowed in path components.

### EfsIndex & B-Trees
- The default index is `BtreeIndex` (implemented in `efs/src/index/bptree_index.rs`).
- It uses `BPTreeStorage` which adapts `EfsBlockStorage` for use with the `bptree` crate.
- Index nodes are stored in the `BTREE_INDEX_REGION_ID`.
- Recursive operations (delete/put) should be handled via the high-level `Efs` methods.

### Silo Management
`SiloManager` handles high-level operations:
- Key derivation from passwords using `Kdf`.
- **Multi-Silo Support:** The `silo_id` is used as a salt for the `Kdf`, allowing multiple logical volumes to coexist on the same storage backend.
- Initialization and loading of "Silos" (logical encrypted volumes).
- Root chunk name derivation: `hex(hasher.hash(root_key))`.

### Storage Backends
- Backends must implement `StorageBackend`.
- `MirrorOrchestrator` handles the majority logic ($\lfloor N/2 \rfloor + 1$); do not implement retry/mirroring logic inside specific backends like `S3Backend`.

---

## 4. Development Workflow

### Adding Features
1. Define the interface in the relevant module's `mod.rs` or the root `lib.rs`.
2. Implement the logic in a submodule.
3. Write unit tests in the same file or integration tests in `efs/tests/`.
4. Run `cargo fmt` and `cargo clippy` before finalizing.

### Modifying the CLI & TUI
- Both the CLI and TUI use `clap` for command parsing (or at least for basic arguments).
- Keep `main.rs` focused on routing or UI state management; move complex logic into specialized modules like `config.rs` or `session.rs`.
- Ensure `Config` changes are backward compatible or handled via versioning.
- **Directory Support:** The `put` command handles recursive directory uploads via `Efs::put_recursive`. The `delete` command supports a `--recursive` flag.

---

## 5. Security Mandates
- **Secrets:** Never log or store raw passwords or keys. Use KDFs to derive working keys.
- **Padding:** Always use CSPRNG (`rand::thread_rng()`) for padding noise. Never use zero-padding.
- **Erasure:** Use crates like `zeroize` for sensitive memory if required by the plan (e.g., for salts or keys).
