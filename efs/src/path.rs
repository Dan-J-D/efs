use anyhow::{anyhow, Result};

pub fn normalize_path(path: &str) -> Result<String> {
    let mut parts = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => continue,
            ".." => {
                parts.pop();
            }
            _ => {
                if !is_valid_name(part) {
                    return Err(anyhow!("Invalid character in path component: {}", part));
                }
                parts.push(part);
            }
        }
    }
    if parts.is_empty() {
        return Err(anyhow!("Invalid path: root cannot be operated on directly"));
    }
    Ok(parts.join("/"))
}

fn is_valid_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path(".gitignore").unwrap(), ".gitignore");
        assert_eq!(normalize_path("/.gitignore").unwrap(), ".gitignore");
        assert_eq!(normalize_path("./.gitignore").unwrap(), ".gitignore");
        assert_eq!(normalize_path("/a/b/c").unwrap(), "a/b/c");
        assert_eq!(normalize_path("a/b/../c").unwrap(), "a/c");
        assert_eq!(normalize_path("/../../a").unwrap(), "a");
        assert!(normalize_path("/").is_err());
        assert!(normalize_path("").is_err());
        assert!(normalize_path(".").is_err());
        assert!(normalize_path("./..").is_err());
        assert!(normalize_path("a/b$/c").is_err());
        assert!(normalize_path("file name.txt").is_err());
        assert!(normalize_path("name?").is_err());
    }
}
