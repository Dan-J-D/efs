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
        return Ok("".to_string());
    }
    Ok(parts.join("/"))
}

pub fn is_valid_name(name: &str) -> bool {
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_')
}

pub fn get_parent(path: &str) -> Option<&str> {
    if path.is_empty() {
        return None;
    }
    match path.rfind('/') {
        Some(idx) => Some(&path[..idx]),
        None => Some(""),
    }
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
        assert_eq!(normalize_path("/").unwrap(), "");
        assert_eq!(normalize_path("").unwrap(), "");
        assert_eq!(normalize_path(".").unwrap(), "");
        assert_eq!(normalize_path("./..").unwrap(), "");
        assert!(normalize_path("a/b$/c").is_err());
        assert!(normalize_path("file name.txt").is_err());
        assert!(normalize_path("name?").is_err());
    }

    #[test]
    fn test_get_parent() {
        assert_eq!(get_parent("a/b/c"), Some("a/b"));
        assert_eq!(get_parent("a/b"), Some("a"));
        assert_eq!(get_parent("a"), Some(""));
        assert_eq!(get_parent(""), None);
    }
}
