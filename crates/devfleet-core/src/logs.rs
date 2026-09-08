//! Bounded on-disk resource logs; event rows retain only references and metadata.

use std::{
    fs,
    io::{Read as _, Seek as _, Write as _},
    os::unix::fs::PermissionsExt,
    path::PathBuf,
};
use thiserror::Error;

const MAX_LINE_BYTES: usize = 16 * 1024;
const MAX_TAIL_BYTES: usize = MAX_LINE_BYTES * 256;

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

/// Bounded log segment used to resume a CLI tail without retaining payloads in state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogTail {
    pub lines: Vec<String>,
    pub cursor: u64,
    pub reset: bool,
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
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        file.write_all(&line)?;
        Ok(())
    }
    pub fn tail(&self, resource: &str, limit: usize) -> Result<Vec<String>, LogError> {
        Ok(self.tail_from(resource, None, limit)?.lines)
    }
    pub fn tail_from(
        &self,
        resource: &str,
        after: Option<u64>,
        limit: usize,
    ) -> Result<LogTail, LogError> {
        let path = self.path(resource)?;
        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(LogTail {
                    lines: Vec::new(),
                    cursor: 0,
                    reset: false,
                });
            }
            Err(error) => return Err(error.into()),
        };
        let length = file.metadata()?.len();
        let reset = after.is_some_and(|cursor| cursor > length);
        let start = match after {
            Some(cursor) if cursor <= length => cursor,
            Some(_) => 0,
            None => length.saturating_sub(u64::try_from(MAX_TAIL_BYTES).unwrap_or(u64::MAX)),
        };
        let discards_initial_fragment = if start == 0 {
            false
        } else {
            file.seek(std::io::SeekFrom::Start(start - 1))?;
            let mut preceding = [0_u8; 1];
            file.read_exact(&mut preceding)?;
            preceding[0] != b'\n'
        };
        file.seek(std::io::SeekFrom::Start(start))?;
        let mut bytes = Vec::with_capacity(MAX_TAIL_BYTES);
        file.take(u64::try_from(MAX_TAIL_BYTES).unwrap_or(u64::MAX))
            .read_to_end(&mut bytes)?;
        let cursor = start + u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let mut lines = String::from_utf8_lossy(&bytes)
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if discards_initial_fragment && !lines.is_empty() {
            lines.remove(0);
        }
        let start = lines.len().saturating_sub(limit.min(256));
        Ok(LogTail {
            lines: lines.into_iter().skip(start).collect(),
            cursor,
            reset,
        })
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

    #[test]
    fn resumes_from_a_bounded_disk_cursor_and_resets_after_rotation() {
        let directory = tempfile::tempdir().expect("temp directory");
        let logs = LogStore::new(directory.path().to_owned(), 1_024, Vec::new()).expect("store");
        logs.append("api", "first").expect("first write");
        let initial = logs.tail_from("api", None, 256).expect("initial tail");
        logs.append("api", "second").expect("second write");
        let resumed = logs
            .tail_from("api", Some(initial.cursor), 256)
            .expect("resumed tail");
        assert_eq!(resumed.lines, vec!["second"]);
        let reset = logs
            .tail_from("api", Some(resumed.cursor + 1), 256)
            .expect("reset tail");
        assert!(reset.reset);
    }
}
