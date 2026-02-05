# EFS: Encrypted File System

EFS is a high-performance, asynchronous, and secure encrypted file system implemented in Rust. It is designed with a focus on data uniformity and deniability, ensuring that stored data appears as random noise and providing robust protection against "block swapping" attacks.

## Key Features

- **Deniability & Uniformity:** Every chunk is padded to a fixed size (default 1MB) and encrypted, making the storage backend appear as a collection of random-sized blocks.
- **Deterministic Naming:** Chunk names are derived deterministically using BLAKE3 hashes, preventing metadata leakage.
- **Multi-Silo Support:** Multiple logical volumes (silos) can coexist on the same storage backend, each with its own keys derived from a password.
- **Mirroring & High Availability:** Built-in support for mirroring data across multiple storage backends with majority-logic for consistency.
- **Flexible Storage Backends:** Supports S3-compatible storage, local filesystem, and in-memory backends.
- **B-Tree Indexing:** Efficient directory and file indexing using a B-Tree structure optimized for encrypted block storage.
- **Modern CLI & TUI:** Includes both a powerful command-line interface and an intuitive terminal user interface.

## Project Structure

This project is organized as a Rust workspace:

- **`efs`**: The core library containing the encrypted file system logic, storage adapters, and cryptographic traits.
- **`efs-cli`**: A command-line tool for managing silos, backends, and performing file operations.
- **`efs-tui`**: A terminal user interface for interactive file management and silo exploration.

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (latest stable version)
- `cargo`

### Installation

Clone the repository and build the workspace:

```bash
git clone https://github.com/dan-j-d/efs.git
cd efs
cargo build --release
```

The binaries will be available in `target/release/efs-cli` and `target/release/efs-tui`.

## Usage

### CLI

The CLI requires a configuration file and a password (provided via the `EFS_PASSWORD` environment variable or prompted at runtime).

#### 1. Initialize a Silo
```bash
efs-cli --config my_config.bin init --silo-id my-vault
```

#### 2. Add a Storage Backend
```bash
# Add a local directory as a backend
efs-cli --config my_config.bin add-local --name local-store --path ./storage

# Add an S3 bucket
efs-cli --config my_config.bin add-s3 --name s3-store --bucket my-efs-bucket --region us-east-1
```

#### 3. Manage Files
```bash
# Upload a file or directory
efs-cli --config my_config.bin put --silo-id my-vault local_file.txt remote_path.txt

# List files
efs-cli --config my_config.bin ls --silo-id my-vault

# Download a file
efs-cli --config my_config.bin get --silo-id my-vault remote_path.txt local_file.txt
```

### TUI

Launch the interactive interface:

```bash
cargo run -p efs-tui
```

The TUI allows you to:
- Log in with your password.
- Manage storage backends and silos.
- Browse directories, upload, download, move, and delete files.

## Development

### Build & Lint
- **Build Workspace:** `cargo build`
- **Lint:** `cargo clippy --workspace -- -D warnings`
- **Format:** `cargo fmt --all`

### Testing
- **Run all tests:** `cargo test`
- **Run library tests:** `cargo test -p efs`

## Security Design

- **AAD (Associated Authenticated Data):** Every chunk uses its logical `RegionId` and `BlockId` as AAD during encryption to prevent block-swapping attacks.
- **KDF:** Working keys are derived from user passwords using robust Key Derivation Functions.
- **Zeroization:** Sensitive data like raw keys and passwords are handled with care to minimize memory exposure.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
