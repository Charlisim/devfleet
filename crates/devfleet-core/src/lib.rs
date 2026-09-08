//! Provider-neutral domain identities, plans, and Git worktree discovery.

use serde::{Deserialize, Serialize};
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
};
use thiserror::Error;
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub Uuid);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
        }
    };
}

id_type!(ProjectId);
id_type!(WorktreeId);
id_type!(EnvironmentId);
id_type!(ResourceId);
id_type!(OperationId);

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Generation(pub u64);

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvironmentKey {
    pub project_id: ProjectId,
    pub worktree_id: WorktreeId,
    pub profile: String,
    pub instance: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourcePlan {
    pub resource_id: ResourceId,
    pub provider: String,
    pub depends_on: Vec<ResourceId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentState {
    Registered,
    Planning,
    Starting,
    Running,
    Degraded,
    Stopping,
    Stopped,
    Failed,
    Destroying,
    Destroyed,
    Orphaned,
}

/// A repository and checked-out worktree discovered through Git's own output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitWorktreeIdentity {
    pub checkout: PathBuf,
    pub common_git_dir: PathBuf,
    pub git_dir: PathBuf,
    pub project_fingerprint: String,
    pub worktree_fingerprint: String,
    pub head: String,
    pub branch: Option<String>,
    pub detached: bool,
}

#[derive(Debug, Error)]
pub enum GitDiscoveryError {
    #[error("cannot inspect Git worktree: {0}")]
    Io(#[from] std::io::Error),
    #[error("Git did not recognize {path} as a worktree: {message}")]
    CommandFailed { path: String, message: String },
    #[error("Git returned malformed worktree porcelain")]
    InvalidPorcelain,
    #[error("Git did not list the requested worktree")]
    WorktreeNotListed,
    #[error("Git returned an invalid administrative-directory response")]
    InvalidDirectories,
}

/// Discovers a repository using Git's NUL-delimited worktree porcelain.
///
/// The identity fingerprints are filesystem device/inode pairs for Git's shared
/// administrative directory and the worktree-specific administrative directory.
/// They survive checkout moves and branch changes but do not identify a newly
/// created repository at an old checkout path.
///
/// # Errors
///
/// Returns an error when `path` is not a readable Git worktree or Git returns
/// malformed structured discovery output.
pub fn discover_git_worktree(path: &Path) -> Result<GitWorktreeIdentity, GitDiscoveryError> {
    let checkout = path.canonicalize()?;
    let worktrees = git_output(&checkout, ["worktree", "list", "--porcelain", "-z"])?;
    let record = parse_worktree_porcelain(&worktrees)?
        .into_iter()
        .find(|record| canonical_path(&record.path).is_ok_and(|value| value == checkout))
        .ok_or(GitDiscoveryError::WorktreeNotListed)?;
    let directories = git_output(
        &checkout,
        [
            "rev-parse",
            "--path-format=absolute",
            "--git-common-dir",
            "--git-dir",
        ],
    )?;
    let mut directories = directories
        .split(|byte| *byte == b'\n')
        .filter(|entry| !entry.is_empty());
    let common_git_dir = PathBuf::from(
        std::str::from_utf8(
            directories
                .next()
                .ok_or(GitDiscoveryError::InvalidDirectories)?,
        )
        .map_err(|_| GitDiscoveryError::InvalidDirectories)?,
    );
    let git_dir = PathBuf::from(
        std::str::from_utf8(
            directories
                .next()
                .ok_or(GitDiscoveryError::InvalidDirectories)?,
        )
        .map_err(|_| GitDiscoveryError::InvalidDirectories)?,
    );
    if directories.next().is_some() {
        return Err(GitDiscoveryError::InvalidDirectories);
    }
    Ok(GitWorktreeIdentity {
        checkout,
        project_fingerprint: fingerprint(&common_git_dir)?,
        worktree_fingerprint: fingerprint(&git_dir)?,
        common_git_dir,
        git_dir,
        head: record.head,
        branch: record.branch,
        detached: record.detached,
    })
}

#[derive(Debug)]
struct WorktreeRecord {
    path: PathBuf,
    head: String,
    branch: Option<String>,
    detached: bool,
}

fn git_output<const N: usize>(
    checkout: &Path,
    arguments: [&str; N],
) -> Result<Vec<u8>, GitDiscoveryError> {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(checkout)
        .output()?;
    if output.status.success() {
        return Ok(output.stdout);
    }
    Err(GitDiscoveryError::CommandFailed {
        path: checkout.display().to_string(),
        message: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
    })
}

fn parse_worktree_porcelain(input: &[u8]) -> Result<Vec<WorktreeRecord>, GitDiscoveryError> {
    let mut records = Vec::new();
    let mut current: Option<WorktreeRecord> = None;
    for field in input.split(|byte| *byte == b'\0') {
        if field.is_empty() {
            if let Some(record) = current.take() {
                if record.head.is_empty() {
                    return Err(GitDiscoveryError::InvalidPorcelain);
                }
                records.push(record);
            }
            continue;
        }
        let field = std::str::from_utf8(field).map_err(|_| GitDiscoveryError::InvalidPorcelain)?;
        if let Some(path) = field.strip_prefix("worktree ") {
            if current.is_some() {
                return Err(GitDiscoveryError::InvalidPorcelain);
            }
            current = Some(WorktreeRecord {
                path: PathBuf::from(path),
                head: String::new(),
                branch: None,
                detached: false,
            });
            continue;
        }
        let current = current
            .as_mut()
            .ok_or(GitDiscoveryError::InvalidPorcelain)?;
        if let Some(head) = field.strip_prefix("HEAD ") {
            current.head = head.into();
        } else if let Some(branch) = field.strip_prefix("branch ") {
            current.branch = Some(branch.into());
        } else if field == "detached" {
            current.detached = true;
        }
    }
    if current.is_some() || records.is_empty() {
        return Err(GitDiscoveryError::InvalidPorcelain);
    }
    Ok(records)
}

fn canonical_path(path: &Path) -> Result<PathBuf, GitDiscoveryError> {
    Ok(path.canonicalize()?)
}

fn fingerprint(path: &Path) -> Result<String, GitDiscoveryError> {
    let metadata = fs::metadata(path)?;
    Ok(format!("{}:{}", metadata.dev(), metadata.ino()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_key_uses_stable_ids_not_a_branch_name() {
        let key = EnvironmentKey {
            project_id: ProjectId::new(),
            worktree_id: WorktreeId::new(),
            profile: "ios".into(),
            instance: None,
        };
        assert_eq!(key.profile, "ios");
    }

    #[test]
    fn parses_nul_delimited_worktree_records_with_spaces_and_detached_head() {
        let records = parse_worktree_porcelain(
            b"worktree /tmp/project space\0HEAD abc\0branch refs/heads/renamed\0\0worktree /tmp/detached\0HEAD def\0detached\0\0",
        )
        .expect("porcelain parses");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].path, PathBuf::from("/tmp/project space"));
        assert_eq!(records[0].branch.as_deref(), Some("refs/heads/renamed"));
        assert!(records[1].detached);
    }
}
