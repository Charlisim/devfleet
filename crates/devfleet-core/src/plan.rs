//! Declarative, side-effect-free environment-plan compilation.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
use thiserror::Error;

const SUPPORTED_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PlanOptions {
    /// Names of providers whose descriptors were already validated by the daemon.
    pub known_providers: BTreeSet<String>,
    /// References the caller can satisfy without sending a secret value to planning.
    pub available_secret_references: BTreeSet<String>,
    /// Values supplied for runtime output references in an inspect/re-plan operation.
    pub runtime_outputs: BTreeMap<String, JsonValue>,
    /// Reserved planning endpoints supplied by a provider-free allocator.
    pub planning_endpoints: BTreeMap<String, JsonValue>,
    /// Values assigned after project and local configuration, keyed by dotted path.
    pub operation_overrides: BTreeMap<String, JsonValue>,
    /// Non-secret environment facts made available to expression resolution.
    pub environment: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EffectivePlan {
    pub schema_version: u32,
    pub profile: String,
    pub resources: Vec<PlannedResource>,
    pub dependency_edges: Vec<DependencyEdge>,
    /// Canonical redacted configuration persisted by the daemon.
    pub effective_configuration: JsonValue,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlannedResource {
    pub id: String,
    pub provider: String,
    pub config: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub toolchain: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct DependencyEdge {
    pub resource: String,
    pub depends_on: String,
    pub kind: DependencyKind,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyKind {
    Explicit,
    RuntimeOutput,
    PlanningEndpoint,
    Readiness,
}

#[derive(Debug, Error)]
pub enum PlanError {
    #[error("cannot read manifest: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest TOML is invalid: {0}")]
    Toml(String),
    #[error("manifest schema_version must be {SUPPORTED_SCHEMA_VERSION}")]
    UnsupportedSchema,
    #[error("profile '{0}' is not declared")]
    UnknownProfile(String),
    #[error("resource '{0}' is not declared")]
    UnknownResource(String),
    #[error("resource '{resource}' has an invalid provider '{provider}'")]
    InvalidProvider { resource: String, provider: String },
    #[error("resource '{resource}' references unavailable toolchain '{toolchain}'")]
    UnknownToolchain { resource: String, toolchain: String },
    #[error("resource '{resource}' configuration is invalid: {message}")]
    InvalidResourceConfig { resource: String, message: String },
    #[error("expression '{0}' is unknown or malformed")]
    UnknownExpression(String),
    #[error("secret reference '{0}' is required but unavailable")]
    MissingSecretReference(String),
    #[error("resource dependency graph contains a cycle: {0}")]
    CircularDependency(String),
    #[error("operation override '{0}' is invalid")]
    InvalidOverride(String),
}

#[derive(Debug, Deserialize)]
struct Manifest {
    schema_version: u32,
    #[serde(default)]
    ports: BTreeMap<String, toml::Value>,
    #[serde(default)]
    toolchains: BTreeMap<String, toml::Value>,
    #[serde(default)]
    profiles: BTreeMap<String, Profile>,
    #[serde(default)]
    resources: BTreeMap<String, Resource>,
}

#[derive(Debug, Deserialize)]
struct Profile {
    resources: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct Resource {
    provider: String,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    readiness_depends_on: Vec<String>,
    #[serde(default)]
    toolchain: Option<String>,
    #[serde(default)]
    config: Option<toml::Value>,
    #[serde(default)]
    config_schema: Option<ConfigSchema>,
}

#[derive(Debug, Deserialize)]
struct ConfigSchema {
    #[serde(default)]
    required: Vec<String>,
    #[serde(default)]
    types: BTreeMap<String, String>,
}

/// Loads the project manifest and optional local override without contacting a provider.
///
/// # Errors
///
/// Returns an error for unreadable files, invalid configuration, unknown references,
/// missing secret references, or a dependency cycle before any resource action occurs.
pub fn compile_manifest(
    manifest_path: &Path,
    local_override_path: Option<&Path>,
    profile: &str,
    options: &PlanOptions,
) -> Result<EffectivePlan, PlanError> {
    let manifest = fs::read_to_string(manifest_path)?;
    let local = local_override_path.map(fs::read_to_string).transpose()?;
    compile_document(&manifest, local.as_deref(), profile, options)
}

/// Compiles TOML text for tests and callers that already hold a manifest document.
///
/// Project configuration is merged first, then the local override, then explicit
/// operation options. Secret values are never accepted or rendered by this API.
///
/// # Errors
///
/// Returns an error before resource side effects for configuration and graph failures.
pub fn compile_document(
    document: &str,
    local_override: Option<&str>,
    profile: &str,
    options: &PlanOptions,
) -> Result<EffectivePlan, PlanError> {
    let mut value = parse_document(document)?;
    require_schema_version(&value)?;
    if let Some(local_override) = local_override {
        let local = parse_document(local_override)?;
        require_schema_version(&local)?;
        merge_local_override(&mut value, local)?;
    }
    apply_operation_overrides(&mut value, &options.operation_overrides)?;
    let manifest: Manifest = value
        .clone()
        .try_into()
        .map_err(|error: toml::de::Error| PlanError::Toml(error.to_string()))?;
    if manifest.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(PlanError::UnsupportedSchema);
    }
    let selected = selected_resources(&manifest, profile)?;
    let mut edges = dependency_edges(&manifest, &selected)?;
    edges.sort_by(|left, right| {
        (&left.resource, &left.depends_on, left.kind).cmp(&(
            &right.resource,
            &right.depends_on,
            right.kind,
        ))
    });
    detect_cycles(&selected, &edges)?;

    for resource_id in &selected {
        let resource = manifest
            .resources
            .get(resource_id)
            .ok_or_else(|| PlanError::UnknownResource(resource_id.clone()))?;
        validate_resource_shape(resource_id, resource, &manifest.toolchains, options)?;
        validate_config_schema(
            resource_id,
            resource.config.as_ref(),
            resource.config_schema.as_ref(),
        )?;
    }

    let context = expression_context(&manifest, options)?;
    let order = dependency_order(&selected, &edges)?;
    let mut planned = BTreeMap::new();
    for resource_id in order {
        let resource = manifest
            .resources
            .get(&resource_id)
            .ok_or_else(|| PlanError::UnknownResource(resource_id.clone()))?;
        let config = resource
            .config
            .as_ref()
            .map(toml_to_json)
            .transpose()?
            .unwrap_or_else(|| JsonValue::Object(serde_json::Map::new()));
        let resolved = resolve_value(&config, &context, &planned, options)?;
        validate_resolved_schema(
            resource_id.as_str(),
            &resolved,
            resource.config_schema.as_ref(),
        )?;
        planned.insert(
            resource_id.clone(),
            PlannedResource {
                id: resource_id,
                provider: resource.provider.clone(),
                config: resolved,
                toolchain: resource.toolchain.clone(),
            },
        );
    }
    let resources = selected
        .iter()
        .map(|id| {
            planned
                .remove(id)
                .ok_or_else(|| PlanError::UnknownResource(id.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let effective_configuration = serde_json::json!({
        "schema_version": manifest.schema_version,
        "profile": profile,
        "ports": toml_map_to_json(&manifest.ports)?,
        "toolchains": toml_map_to_json(&manifest.toolchains)?,
        "resources": resources.clone(),
        "dependency_edges": edges.clone(),
    });
    Ok(EffectivePlan {
        schema_version: manifest.schema_version,
        profile: profile.into(),
        resources,
        dependency_edges: edges,
        effective_configuration,
    })
}

fn parse_document(document: &str) -> Result<toml::Value, PlanError> {
    toml::from_str(document).map_err(|error: toml::de::Error| PlanError::Toml(error.to_string()))
}

fn require_schema_version(value: &toml::Value) -> Result<(), PlanError> {
    if value
        .get("schema_version")
        .and_then(toml::Value::as_integer)
        != Some(i64::from(SUPPORTED_SCHEMA_VERSION))
    {
        return Err(PlanError::UnsupportedSchema);
    }
    Ok(())
}

fn merge_local_override(base: &mut toml::Value, mut local: toml::Value) -> Result<(), PlanError> {
    let local_table = local
        .as_table_mut()
        .ok_or_else(|| PlanError::Toml("local override root must be a table".into()))?;
    local_table.remove("schema_version");
    let overrides = local_table.remove("overrides");
    merge_toml(base, toml::Value::Table(std::mem::take(local_table)));
    if let Some(overrides) = overrides {
        merge_toml(base, overrides);
    }
    Ok(())
}

fn merge_toml(base: &mut toml::Value, override_value: toml::Value) {
    if let (Some(base_table), Some(override_table)) =
        (base.as_table_mut(), override_value.as_table())
    {
        for (key, value) in override_table {
            if let Some(existing) = base_table.get_mut(key) {
                merge_toml(existing, value.clone());
            } else {
                base_table.insert(key.clone(), value.clone());
            }
        }
    } else {
        *base = override_value;
    }
}

fn apply_operation_overrides(
    document: &mut toml::Value,
    overrides: &BTreeMap<String, JsonValue>,
) -> Result<(), PlanError> {
    for (path, value) in overrides {
        if path.is_empty() || path.split('.').any(str::is_empty) {
            return Err(PlanError::InvalidOverride(path.clone()));
        }
        let value = toml::Value::try_from(value.clone())
            .map_err(|_| PlanError::InvalidOverride(path.clone()))?;
        let parts = path.split('.').collect::<Vec<_>>();
        let mut current = document
            .as_table_mut()
            .ok_or_else(|| PlanError::InvalidOverride(path.clone()))?;
        for part in &parts[..parts.len() - 1] {
            current = current
                .entry((*part).to_owned())
                .or_insert_with(|| toml::Value::Table(toml::map::Map::new()))
                .as_table_mut()
                .ok_or_else(|| PlanError::InvalidOverride(path.clone()))?;
        }
        let last = parts
            .last()
            .ok_or_else(|| PlanError::InvalidOverride(path.clone()))?;
        current.insert((*last).to_string(), value);
    }
    Ok(())
}

fn selected_resources(manifest: &Manifest, profile: &str) -> Result<Vec<String>, PlanError> {
    let profile = manifest
        .profiles
        .get(profile)
        .ok_or_else(|| PlanError::UnknownProfile(profile.into()))?;
    let mut selected = BTreeSet::new();
    let mut pending = profile.resources.clone();
    while let Some(resource_id) = pending.pop() {
        if !selected.insert(resource_id.clone()) {
            continue;
        }
        let resource = manifest
            .resources
            .get(&resource_id)
            .ok_or_else(|| PlanError::UnknownResource(resource_id.clone()))?;
        pending.extend(resource.depends_on.iter().cloned());
        pending.extend(resource.readiness_depends_on.iter().cloned());
    }
    Ok(selected.into_iter().collect())
}

fn dependency_edges(
    manifest: &Manifest,
    selected: &[String],
) -> Result<Vec<DependencyEdge>, PlanError> {
    let selected = selected.iter().collect::<BTreeSet<_>>();
    let mut edges = BTreeSet::new();
    for resource_id in &selected {
        let resource = manifest
            .resources
            .get(*resource_id)
            .ok_or_else(|| PlanError::UnknownResource((*resource_id).clone()))?;
        for dependency in &resource.depends_on {
            ensure_selected_dependency(dependency, &selected)?;
            edges.insert(DependencyEdge {
                resource: (*resource_id).clone(),
                depends_on: dependency.clone(),
                kind: DependencyKind::Explicit,
            });
        }
        for dependency in &resource.readiness_depends_on {
            ensure_selected_dependency(dependency, &selected)?;
            edges.insert(DependencyEdge {
                resource: (*resource_id).clone(),
                depends_on: dependency.clone(),
                kind: DependencyKind::Readiness,
            });
        }
        if let Some(config) = &resource.config {
            let mut expressions = Vec::new();
            collect_expressions(&toml_to_json(config)?, &mut expressions)?;
            for expression in expressions {
                if let Some((dependency, kind)) = expression_dependency(&expression) {
                    ensure_selected_dependency(&dependency, &selected)?;
                    edges.insert(DependencyEdge {
                        resource: (*resource_id).clone(),
                        depends_on: dependency,
                        kind,
                    });
                }
            }
        }
    }
    Ok(edges.into_iter().collect())
}

fn ensure_selected_dependency(
    dependency: &str,
    selected: &BTreeSet<&String>,
) -> Result<(), PlanError> {
    if selected
        .iter()
        .any(|resource| resource.as_str() == dependency)
    {
        Ok(())
    } else {
        Err(PlanError::UnknownResource(dependency.into()))
    }
}

fn expression_dependency(expression: &str) -> Option<(String, DependencyKind)> {
    let parts = expression.split('.').collect::<Vec<_>>();
    match parts.as_slice() {
        ["resources", resource, "outputs", ..] => {
            Some(((*resource).into(), DependencyKind::RuntimeOutput))
        }
        ["resources", resource, "endpoints", ..] | ["endpoints", resource, ..] => {
            Some(((*resource).into(), DependencyKind::PlanningEndpoint))
        }
        _ => None,
    }
}

fn detect_cycles(selected: &[String], edges: &[DependencyEdge]) -> Result<(), PlanError> {
    let mut by_resource: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for edge in edges {
        by_resource
            .entry(&edge.resource)
            .or_default()
            .push(&edge.depends_on);
    }
    let mut visiting = BTreeSet::new();
    let mut completed = BTreeSet::new();
    for resource in selected {
        if let Some(cycle) = visit(resource, &by_resource, &mut visiting, &mut completed) {
            return Err(PlanError::CircularDependency(cycle));
        }
    }
    Ok(())
}

fn visit(
    resource: &str,
    edges: &BTreeMap<&str, Vec<&str>>,
    visiting: &mut BTreeSet<String>,
    completed: &mut BTreeSet<String>,
) -> Option<String> {
    if completed.contains(resource) {
        return None;
    }
    if !visiting.insert(resource.into()) {
        return Some(resource.into());
    }
    for dependency in edges.get(resource).into_iter().flatten() {
        if let Some(cycle) = visit(dependency, edges, visiting, completed) {
            return Some(cycle);
        }
    }
    visiting.remove(resource);
    completed.insert(resource.into());
    None
}

fn dependency_order(
    selected: &[String],
    edges: &[DependencyEdge],
) -> Result<Vec<String>, PlanError> {
    let mut dependencies = BTreeMap::<&str, BTreeSet<&str>>::new();
    for resource in selected {
        dependencies.entry(resource).or_default();
    }
    for edge in edges {
        dependencies
            .entry(&edge.resource)
            .or_default()
            .insert(&edge.depends_on);
    }
    let mut ordered = Vec::new();
    while !dependencies.is_empty() {
        let ready = dependencies
            .iter()
            .find(|(_, values)| values.is_empty())
            .map(|(resource, _)| *resource)
            .ok_or_else(|| PlanError::CircularDependency("unresolved dependency".into()))?;
        dependencies.remove(ready);
        for values in dependencies.values_mut() {
            values.remove(ready);
        }
        ordered.push(ready.into());
    }
    Ok(ordered)
}

fn validate_resource_shape(
    resource_id: &str,
    resource: &Resource,
    toolchains: &BTreeMap<String, toml::Value>,
    options: &PlanOptions,
) -> Result<(), PlanError> {
    if resource.provider.is_empty()
        || !resource
            .provider
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'/'))
        || (!options.known_providers.is_empty()
            && !options.known_providers.contains(&resource.provider))
    {
        return Err(PlanError::InvalidProvider {
            resource: resource_id.into(),
            provider: resource.provider.clone(),
        });
    }
    if let Some(toolchain) = &resource.toolchain
        && !toolchains.contains_key(toolchain)
    {
        return Err(PlanError::UnknownToolchain {
            resource: resource_id.into(),
            toolchain: toolchain.clone(),
        });
    }
    if resource
        .config
        .as_ref()
        .is_some_and(|config| !config.is_table())
    {
        return Err(PlanError::InvalidResourceConfig {
            resource: resource_id.into(),
            message: "config must be a TOML table".into(),
        });
    }
    Ok(())
}

fn validate_config_schema(
    resource_id: &str,
    config: Option<&toml::Value>,
    schema: Option<&ConfigSchema>,
) -> Result<(), PlanError> {
    let Some(schema) = schema else {
        return Ok(());
    };
    let config =
        config
            .and_then(toml::Value::as_table)
            .ok_or_else(|| PlanError::InvalidResourceConfig {
                resource: resource_id.into(),
                message: "config_schema requires a config table".into(),
            })?;
    for required in &schema.required {
        if !has_dotted_toml(config, required) {
            return Err(PlanError::InvalidResourceConfig {
                resource: resource_id.into(),
                message: format!("missing required '{required}'"),
            });
        }
    }
    for (path, expected) in &schema.types {
        if !matches!(
            expected.as_str(),
            "string" | "integer" | "float" | "boolean" | "array" | "table"
        ) {
            return Err(PlanError::InvalidResourceConfig {
                resource: resource_id.into(),
                message: format!("unknown schema type '{expected}' for '{path}'"),
            });
        }
        let value = dotted_toml(config, path).ok_or_else(|| PlanError::InvalidResourceConfig {
            resource: resource_id.into(),
            message: format!("missing typed value '{path}'"),
        })?;
        let value = toml_to_json(value)?;
        if !is_expression_string(&value) && json_type(&value) != expected {
            return Err(PlanError::InvalidResourceConfig {
                resource: resource_id.into(),
                message: format!("'{path}' must be {expected}"),
            });
        }
    }
    Ok(())
}

fn validate_resolved_schema(
    resource_id: &str,
    config: &JsonValue,
    schema: Option<&ConfigSchema>,
) -> Result<(), PlanError> {
    let Some(schema) = schema else {
        return Ok(());
    };
    for (path, expected) in &schema.types {
        let value = dotted_json(config, path).ok_or_else(|| PlanError::InvalidResourceConfig {
            resource: resource_id.into(),
            message: format!("missing typed value '{path}'"),
        })?;
        if is_expression_string(value) || json_type(value) == expected {
            continue;
        }
        return Err(PlanError::InvalidResourceConfig {
            resource: resource_id.into(),
            message: format!("'{path}' must be {expected}"),
        });
    }
    Ok(())
}

fn expression_context(
    manifest: &Manifest,
    options: &PlanOptions,
) -> Result<BTreeMap<String, JsonValue>, PlanError> {
    let mut context = BTreeMap::new();
    context.insert("ports".into(), toml_map_to_json(&manifest.ports)?);
    context.insert("toolchains".into(), toml_map_to_json(&manifest.toolchains)?);
    context.insert(
        "environment".into(),
        JsonValue::Object(options.environment.clone().into_iter().collect()),
    );
    Ok(context)
}

fn resolve_value(
    value: &JsonValue,
    context: &BTreeMap<String, JsonValue>,
    planned: &BTreeMap<String, PlannedResource>,
    options: &PlanOptions,
) -> Result<JsonValue, PlanError> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .map(|item| resolve_value(item, context, planned, options))
            .collect::<Result<Vec<_>, _>>()
            .map(JsonValue::Array),
        JsonValue::Object(values) => values
            .iter()
            .map(|(key, item)| Ok((key.clone(), resolve_value(item, context, planned, options)?)))
            .collect::<Result<serde_json::Map<_, _>, PlanError>>()
            .map(JsonValue::Object),
        JsonValue::String(value) => resolve_string(value, context, planned, options),
        value => Ok(value.clone()),
    }
}

fn resolve_string(
    text: &str,
    context: &BTreeMap<String, JsonValue>,
    planned: &BTreeMap<String, PlannedResource>,
    options: &PlanOptions,
) -> Result<JsonValue, PlanError> {
    let expressions = expressions_in(text)?;
    if expressions.is_empty() {
        return Ok(JsonValue::String(text.into()));
    }
    if expressions.len() == 1 && is_whole_expression(text) {
        return resolve_expression(&expressions[0], context, planned, options);
    }
    let mut rendered = text.to_owned();
    for expression in expressions {
        let value = resolve_expression(&expression, context, planned, options)?;
        let string = match value {
            JsonValue::String(value) => value,
            JsonValue::Number(value) => value.to_string(),
            JsonValue::Bool(value) => value.to_string(),
            _ => {
                return Err(PlanError::UnknownExpression(expression));
            }
        };
        rendered = replace_next_expression(&rendered, &string)?;
    }
    Ok(JsonValue::String(rendered))
}

fn resolve_expression(
    expression: &str,
    context: &BTreeMap<String, JsonValue>,
    planned: &BTreeMap<String, PlannedResource>,
    options: &PlanOptions,
) -> Result<JsonValue, PlanError> {
    let parts = expression.split('.').collect::<Vec<_>>();
    if parts.iter().any(|part| part.is_empty()) {
        return Err(PlanError::UnknownExpression(expression.into()));
    }
    if let ["secrets", secret] = parts.as_slice() {
        if !options.available_secret_references.contains(*secret) {
            return Err(PlanError::MissingSecretReference((*secret).into()));
        }
        return Ok(JsonValue::String(format!("${{{{ secrets.{secret} }}}}")));
    }
    if let ["resources", resource, "outputs", output @ ..] = parts.as_slice() {
        if output.is_empty() {
            return Err(PlanError::UnknownExpression(expression.into()));
        }
        let key = format!("{resource}.{}", output.join("."));
        return Ok(options
            .runtime_outputs
            .get(&key)
            .cloned()
            .unwrap_or_else(|| JsonValue::String(format!("${{{{ {expression} }}}}"))));
    }
    if let ["resources", resource, "endpoints", endpoint @ ..]
    | ["endpoints", resource, endpoint @ ..] = parts.as_slice()
    {
        if endpoint.is_empty() {
            return Err(PlanError::UnknownExpression(expression.into()));
        }
        let endpoint_key = format!("{resource}.{}", endpoint.join("."));
        if let Some(value) = options.planning_endpoints.get(&endpoint_key) {
            return Ok(value.clone());
        }
        let resource = planned
            .get(*resource)
            .ok_or_else(|| PlanError::UnknownExpression(expression.into()))?;
        let path = std::iter::once("endpoints")
            .chain(endpoint.iter().copied())
            .collect::<Vec<_>>()
            .join(".");
        return dotted_json(&resource.config, &path)
            .cloned()
            .ok_or_else(|| PlanError::UnknownExpression(expression.into()));
    }
    let root = context
        .get(parts[0])
        .ok_or_else(|| PlanError::UnknownExpression(expression.into()))?;
    dotted_json(root, &parts[1..].join("."))
        .cloned()
        .ok_or_else(|| PlanError::UnknownExpression(expression.into()))
}

fn expressions_in(text: &str) -> Result<Vec<String>, PlanError> {
    let mut remaining = text;
    let mut expressions = Vec::new();
    while let Some(start) = remaining.find("${{") {
        let suffix = &remaining[start + 3..];
        let end = suffix
            .find("}}")
            .ok_or_else(|| PlanError::UnknownExpression(text.into()))?;
        let expression = suffix[..end].trim();
        if expression.is_empty() {
            return Err(PlanError::UnknownExpression(text.into()));
        }
        expressions.push(expression.into());
        remaining = &suffix[end + 2..];
    }
    if remaining.contains("}}") {
        return Err(PlanError::UnknownExpression(text.into()));
    }
    Ok(expressions)
}

fn is_whole_expression(text: &str) -> bool {
    let trimmed = text.trim();
    trimmed.starts_with("${{")
        && trimmed.ends_with("}}")
        && expressions_in(trimmed).is_ok_and(|items| items.len() == 1)
}

fn replace_next_expression(text: &str, replacement: &str) -> Result<String, PlanError> {
    let start = text
        .find("${{")
        .ok_or_else(|| PlanError::UnknownExpression(text.into()))?;
    let suffix = &text[start + 3..];
    let end = suffix
        .find("}}")
        .ok_or_else(|| PlanError::UnknownExpression(text.into()))?;
    Ok(format!(
        "{}{}{}",
        &text[..start],
        replacement,
        &suffix[end + 2..]
    ))
}

fn collect_expressions(value: &JsonValue, expressions: &mut Vec<String>) -> Result<(), PlanError> {
    match value {
        JsonValue::Array(values) => values
            .iter()
            .try_for_each(|value| collect_expressions(value, expressions)),
        JsonValue::Object(values) => values
            .values()
            .try_for_each(|value| collect_expressions(value, expressions)),
        JsonValue::String(text) => {
            expressions.extend(expressions_in(text)?);
            Ok(())
        }
        _ => Ok(()),
    }
}

fn toml_to_json(value: &toml::Value) -> Result<JsonValue, PlanError> {
    serde_json::to_value(value).map_err(|error| PlanError::Toml(error.to_string()))
}

fn toml_map_to_json(values: &BTreeMap<String, toml::Value>) -> Result<JsonValue, PlanError> {
    serde_json::to_value(values).map_err(|error| PlanError::Toml(error.to_string()))
}

fn has_dotted_toml(values: &toml::map::Map<String, toml::Value>, path: &str) -> bool {
    dotted_toml(values, path).is_some()
}

fn dotted_toml<'a>(
    values: &'a toml::map::Map<String, toml::Value>,
    path: &str,
) -> Option<&'a toml::Value> {
    let mut current = values.get(path.split('.').next().unwrap_or_default());
    for part in path.split('.').skip(1) {
        current = current
            .and_then(toml::Value::as_table)
            .and_then(|table| table.get(part));
    }
    current
}

fn dotted_json<'a>(value: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path.is_empty() {
        return Some(value);
    }
    path.split('.')
        .try_fold(value, |value, part| value.get(part))
}

fn is_expression_string(value: &JsonValue) -> bool {
    value
        .as_str()
        .is_some_and(|value| expressions_in(value).is_ok_and(|expressions| !expressions.is_empty()))
}

fn json_type(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::String(_) => "string",
        JsonValue::Number(number) if number.is_i64() || number.is_u64() => "integer",
        JsonValue::Number(_) => "float",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "table",
        JsonValue::Null => "null",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = r#"
schema_version = 1
[ports]
api = 4010
[toolchains.node]
path = "/opt/node"
[profiles.dev]
resources = ["api", "web"]
[resources.api]
provider = "official.process"
toolchain = "node"
[resources.api.config]
port = "${{ ports.api }}"
token = "${{ secrets.API_TOKEN }}"
[resources.api.config_schema]
required = ["port", "token"]
[resources.api.config_schema.types]
port = "integer"
[resources.web]
provider = "official.process"
depends_on = ["api"]
readiness_depends_on = ["api"]
[resources.web.config]
api_port = "${{ resources.api.outputs.port }}"
url = "http://127.0.0.1:${{ resources.api.outputs.port }}"
"#;

    fn options() -> PlanOptions {
        PlanOptions {
            known_providers: BTreeSet::from(["official.process".into()]),
            available_secret_references: BTreeSet::from(["API_TOKEN".into()]),
            runtime_outputs: BTreeMap::from([("api.port".into(), serde_json::json!(4010))]),
            ..PlanOptions::default()
        }
    }

    #[test]
    fn resolves_whole_expressions_with_types_and_interpolations_as_strings() {
        let plan = compile_document(MANIFEST, None, "dev", &options()).expect("plan compiles");
        let web = plan
            .resources
            .iter()
            .find(|resource| resource.id == "web")
            .unwrap();
        assert_eq!(web.config["api_port"], serde_json::json!(4010));
        assert_eq!(
            web.config["url"],
            serde_json::json!("http://127.0.0.1:4010")
        );
        assert!(
            plan.dependency_edges
                .iter()
                .any(|edge| edge.kind == DependencyKind::RuntimeOutput)
        );
        assert!(
            plan.dependency_edges
                .iter()
                .any(|edge| edge.kind == DependencyKind::Readiness)
        );
        assert_eq!(
            plan.resources[0].config["token"],
            "${{ secrets.API_TOKEN }}"
        );
    }

    #[test]
    fn rejects_missing_secret_and_circular_runtime_references_before_effects() {
        let mut missing_secret = options();
        missing_secret.available_secret_references.clear();
        let error = compile_document(MANIFEST, None, "dev", &missing_secret).unwrap_err();
        assert!(matches!(error, PlanError::MissingSecretReference(_)));
        let cycle = MANIFEST.replace("depends_on = [\"api\"]", "depends_on = [\"api\"]\n");
        let cycle = cycle.replace(
            "provider = \"official.process\"\ntoolchain",
            "provider = \"official.process\"\ndepends_on = [\"web\"]\ntoolchain",
        );
        let error = compile_document(&cycle, None, "dev", &options()).unwrap_err();
        assert!(matches!(error, PlanError::CircularDependency(_)));
    }

    #[test]
    fn applies_local_overrides_before_operation_options() {
        let local = r"
schema_version = 1
[overrides.resources.api.config]
port = 5000
";
        let mut options = options();
        options
            .operation_overrides
            .insert("resources.api.config.port".into(), serde_json::json!(6000));
        let plan = compile_document(MANIFEST, Some(local), "dev", &options).expect("plan compiles");
        assert_eq!(plan.resources[0].config["port"], serde_json::json!(6000));
    }

    #[test]
    fn resolves_reserved_endpoints_without_turning_them_into_runtime_outputs() {
        let manifest = MANIFEST.replace(
            "${{ resources.api.outputs.port }}",
            "${{ endpoints.api.reserved.port }}",
        );
        let mut options = options();
        options
            .planning_endpoints
            .insert("api.reserved.port".into(), serde_json::json!(4011));
        let plan = compile_document(&manifest, None, "dev", &options).expect("plan compiles");
        let web = plan
            .resources
            .iter()
            .find(|resource| resource.id == "web")
            .unwrap();
        assert_eq!(web.config["api_port"], serde_json::json!(4011));
        assert!(
            plan.dependency_edges
                .iter()
                .any(|edge| edge.kind == DependencyKind::PlanningEndpoint)
        );
    }

    #[test]
    fn rejects_static_schema_mismatches_before_expression_resolution() {
        let invalid = MANIFEST.replace("port = \"${{ ports.api }}\"", "port = \"not-a-port\"");
        let error = compile_document(&invalid, None, "dev", &options()).unwrap_err();
        assert!(matches!(error, PlanError::InvalidResourceConfig { .. }));
    }
}
