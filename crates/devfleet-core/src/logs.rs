//! Bounded on-disk resource logs; event rows retain only references and metadata.

use std::{fs, io::Write as _, path::PathBuf};
use thiserror::Error;

const MAX_LINE_BYTES: usize = 16 * 1024;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("log resource identifier is invalid")]
    InvalidResource,
    #[error("log I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

pub struct LogStore {
    root: PathBuf,
    max_bytes: u64,
    secrets: Vec<String>,
}

#[allow(clippy::missing_errors_doc)]
impl LogStore {
    pub fn new(root: PathBuf, max_bytes: u64, secrets: Vec<String>) -> Result<Self, LogError> {
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            max_bytes: max_bytes.max(1),
            secrets,
        })
    }
    pub fn append(&self, resource: &str, line: &str) -> Result<(), LogError> {
        let path = self.path(resource)?;
        let mut line = line
            .as_bytes()
            .get(..line.len().min(MAX_LINE_BYTES))
            .unwrap_or_default()
            .to_vec();
        for secret in &self.secrets {
            if !secret.is_empty() {
                line = String::from_utf8_lossy(&line)
                    .replace(secret, "[REDACTED]")
                    .into_bytes();
            }
        }
        line.push(b'\n');
        if fs::metadata(&path).is_ok_and(|metadata| {
            metadata.len() + u64::try_from(line.len()).unwrap_or(u64::MAX) > self.max_bytes
        }) {
            let rotated = path.with_extension("log.1");
            let _ = fs::remove_file(&rotated);
            if path.exists() {
                fs::rename(&path, rotated)?;
            }
        }
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?
            .write_all(&line)?;
        Ok(())
    }
    pub fn tail(&self, resource: &str, limit: usize) -> Result<Vec<String>, LogError> {
        let path = self.path(resource)?;
        let content = fs::read_to_string(path).unwrap_or_default();
        let lines = content.lines().collect::<Vec<_>>();
        let start = lines.len().saturating_sub(limit.min(256));
        Ok(lines.into_iter().skip(start).map(str::to_owned).collect())
    }
    fn path(&self, resource: &str) -> Result<PathBuf, LogError> {
        if resource.is_empty()
            || !resource
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'_' | b'-'))
        {
            return Err(LogError::InvalidResource);
        }
        Ok(self.root.join(format!("{resource}.log")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounds_rotation_and_redacts_explicit_known_secrets() {
        let directory = tempfile::tempdir().expect("temp directory");
        let logs = LogStore::new(directory.path().to_owned(), 20, vec!["token-123".into()])
            .expect("store");
        logs.append("api", "one token-123").expect("first");
        logs.append("api", "two").expect("second");
        logs.append("api", "three").expect("rotates");
        assert!(
            logs.tail("api", 256)
                .expect("tail")
                .iter()
                .all(|line| !line.contains("token-123"))
        );
        assert!(directory.path().join("api.log.1").exists());
    }
}
