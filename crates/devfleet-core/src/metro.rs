//! Rendered Metro configuration with worktree-scoped module and cache roots.

use crate::bindings::BindingIntegration;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetroAdapter {
    pub project_root: PathBuf,
    pub cache_directory: PathBuf,
    pub port: u16,
    pub max_workers: u16,
    pub environment_identity: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum MetroAdapterError {
    #[error("Metro bundle bindings require the bundle_config integration")]
    UnsupportedBindingIntegration,
    #[error("Metro requires nonzero port and worker limits")]
    InvalidBudget,
    #[error("Metro project root and cache directory must be absolute")]
    RelativePath,
    #[error("Metro identity must be nonempty")]
    EmptyIdentity,
}

/// Validates a real Metro bundle integration and renders its configuration.
///
/// # Errors
///
/// Returns an error for a non-bundle integration or invalid resolved resources.
pub fn metro_adapter(
    project_root: PathBuf,
    cache_directory: PathBuf,
    port: u16,
    max_workers: u16,
    environment_identity: String,
    integration: BindingIntegration,
) -> Result<MetroAdapter, MetroAdapterError> {
    if integration != BindingIntegration::BundleConfig {
        return Err(MetroAdapterError::UnsupportedBindingIntegration);
    }
    if port == 0 || max_workers == 0 {
        return Err(MetroAdapterError::InvalidBudget);
    }
    if !project_root.is_absolute() || !cache_directory.is_absolute() {
        return Err(MetroAdapterError::RelativePath);
    }
    if environment_identity.is_empty() {
        return Err(MetroAdapterError::EmptyIdentity);
    }
    Ok(MetroAdapter {
        project_root,
        cache_directory,
        port,
        max_workers,
        environment_identity,
    })
}

/// Emits a `metro.config.cjs` that configures the documented Metro options.
///
/// # Panics
///
/// Panics only if serializing a Rust-owned path or identity fails.
#[must_use]
pub fn render_metro_config(adapter: &MetroAdapter) -> String {
    let root = js_string(&adapter.project_root);
    let cache = js_string(&adapter.cache_directory);
    let identity =
        serde_json::to_string(&adapter.environment_identity).expect("identity serializes");
    format!(
        "const path = require('path');\nconst {{getDefaultConfig, mergeConfig}} = require({root} + '/node_modules/@react-native/metro-config');\nconst {{FileStore}} = require({root} + '/node_modules/metro-cache');\nconst projectRoot = {root};\nmodule.exports = mergeConfig(getDefaultConfig(projectRoot), {{\n  projectRoot,\n  watchFolders: [],\n  maxWorkers: {},\n  cacheStores: [new FileStore({{root: {cache}}})],\n  resolver: {{disableHierarchicalLookup: true, nodeModulesPaths: [path.join(projectRoot, 'node_modules')]}},\n  server: {{enhanceMiddleware: middleware => (req, res, next) => {{ if (req.url === '/devfleet-identity') {{ res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({{environment: {identity}, port: {}}})); return; }} return middleware(req, res, next); }}}},\n}});\n",
        adapter.max_workers, adapter.port
    )
}

fn js_string(path: &Path) -> String {
    serde_json::to_string(&path.display().to_string()).expect("path serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_two_worktrees_without_shared_watch_or_cache_paths() {
        let a = metro_adapter(
            PathBuf::from("/tmp/a"),
            PathBuf::from("/state/a"),
            18_081,
            1,
            "a".into(),
            BindingIntegration::BundleConfig,
        )
        .expect("A adapter");
        let b = metro_adapter(
            PathBuf::from("/tmp/b"),
            PathBuf::from("/state/b"),
            18_082,
            1,
            "b".into(),
            BindingIntegration::BundleConfig,
        )
        .expect("B adapter");
        let rendered_a = render_metro_config(&a);
        let rendered_b = render_metro_config(&b);
        assert!(rendered_a.contains("watchFolders: []"));
        assert!(rendered_a.contains("disableHierarchicalLookup: true"));
        assert!(rendered_a.contains("/tmp/a"));
        assert!(!rendered_a.contains("/tmp/b"));
        assert!(rendered_b.contains("/state/b"));
        assert!(matches!(
            metro_adapter(
                PathBuf::from("/tmp/a"),
                PathBuf::from("/state/a"),
                1,
                1,
                "a".into(),
                BindingIntegration::ProcessEnvironment
            ),
            Err(MetroAdapterError::UnsupportedBindingIntegration)
        ));
    }
}
