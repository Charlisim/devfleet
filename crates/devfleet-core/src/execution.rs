//! Dependency-aware resource execution with durable-intent hooks.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};
use thiserror::Error;
use uuid::Uuid;

/// The immutable context carried by every provider effect and observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExecutionContext {
    pub environment_id: Uuid,
    pub resource_id: String,
    pub generation: u64,
    pub operation_id: Uuid,
    pub idempotency_key: String,
    pub deadline_unix_ms: u128,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionNode {
    pub id: String,
    pub dependencies: Vec<String>,
    pub readiness_dependencies: Vec<String>,
    pub ephemeral: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionPlan {
    pub environment_id: Uuid,
    pub generation: u64,
    pub operation_id: Uuid,
    pub deadline_unix_ms: u128,
    pub nodes: Vec<ExecutionNode>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeOwnership {
    NewlyCreatedEphemeral,
    Preexisting,
    Uncertain,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeResult {
    pub ownership: NodeOwnership,
    pub readiness: BTreeSet<String>,
    pub observed: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionResult {
    pub completed: BTreeMap<String, NodeResult>,
    pub quarantined: BTreeSet<String>,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ExecutionError {
    #[error("resource '{0}' is not declared")]
    UnknownResource(String),
    #[error("resource graph contains a cycle")]
    Cycle,
    #[error("generation {received} is stale; current generation is {current}")]
    StaleGeneration { received: u64, current: u64 },
    #[error("provider failed for resource '{resource}': {message}")]
    Provider { resource: String, message: String },
    #[error("journal failed for resource '{resource}': {message}")]
    Journal { resource: String, message: String },
}

/// Provider seam used by production adapters and deterministic simulated tests.
#[allow(clippy::missing_errors_doc)]
pub trait GraphProvider: Sync {
    /// Reconciles one eligible resource. `LostResponse` requires an inspect before retry.
    fn reconcile(&self, context: &ExecutionContext) -> Result<NodeResult, ProviderError>;

    /// Inspects a resource after a lost response. `None` permits one idempotent retry.
    fn inspect(&self, context: &ExecutionContext) -> Result<Option<NodeResult>, ProviderError>;

    /// Stops only an owned ephemeral resource during failed-start compensation.
    fn stop(&self, context: &ExecutionContext) -> Result<(), ProviderError>;
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ProviderError {
    #[error("response was lost after the provider may have performed the effect")]
    LostResponse,
    #[error("{0}")]
    Failed(String),
}

/// Persistence seam. Intents are written before the provider receives a call.
#[allow(clippy::missing_errors_doc)]
pub trait ExecutionJournal: Sync {
    fn current_generation(&self, environment_id: Uuid) -> Result<u64, String>;
    fn record_intent(&self, context: &ExecutionContext) -> Result<(), String>;
    fn record_result(&self, context: &ExecutionContext, result: &NodeResult) -> Result<(), String>;
    fn record_quarantine(&self, context: &ExecutionContext, reason: &str) -> Result<(), String>;
}

impl ExecutionPlan {
    /// Runs a validated graph in dependency order. Each resource intent is durable
    /// before reconciliation; a lost response is inspected before one retry.
    ///
    /// # Errors
    ///
    /// Returns failures without attempting nodes whose dependencies were not ready.
    pub fn execute<P: GraphProvider, J: ExecutionJournal>(
        &self,
        provider: &P,
        journal: &J,
    ) -> Result<ExecutionResult, ExecutionError> {
        let current = journal
            .current_generation(self.environment_id)
            .map_err(|message| ExecutionError::Journal {
                resource: "environment".into(),
                message,
            })?;
        if current != self.generation {
            return Err(ExecutionError::StaleGeneration {
                received: self.generation,
                current,
            });
        }
        let nodes = node_map(&self.nodes)?;
        let stages = stages(&nodes)?;
        let mut completed = BTreeMap::new();
        for stage in stages {
            let mut contexts = Vec::new();
            for resource_id in &stage {
                let node = nodes
                    .get(resource_id)
                    .ok_or_else(|| ExecutionError::UnknownResource(resource_id.clone()))?;
                if !dependencies_ready(node, &completed) {
                    return Err(ExecutionError::Provider {
                        resource: resource_id.clone(),
                        message: "a dependency did not reach readiness".into(),
                    });
                }
                let context = self.context_for(node);
                journal
                    .record_intent(&context)
                    .map_err(|message| ExecutionError::Journal {
                        resource: node.id.clone(),
                        message,
                    })?;
                contexts.push(context);
            }
            let results = std::thread::scope(|scope| {
                contexts
                    .iter()
                    .map(|context| {
                        let context = context.clone();
                        scope.spawn(move || {
                            let result = reconcile_with_inspect(provider, &context);
                            (context, result)
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|handle| {
                        handle.join().map_err(|_| ExecutionError::Provider {
                            resource: "execution-thread".into(),
                            message: "provider thread panicked".into(),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            for (context, result) in results {
                let result = result.map_err(|error| ExecutionError::Provider {
                    resource: context.resource_id.clone(),
                    message: error.to_string(),
                });
                match result {
                    Ok(result) => {
                        journal
                            .record_result(&context, &result)
                            .map_err(|message| ExecutionError::Journal {
                                resource: context.resource_id.clone(),
                                message,
                            })?;
                        completed.insert(context.resource_id.clone(), result);
                    }
                    Err(error) => {
                        let quarantined = compensate(provider, journal, self, &nodes, &completed);
                        return Err(with_compensation(error, &quarantined));
                    }
                }
            }
        }
        Ok(ExecutionResult {
            completed,
            quarantined: BTreeSet::new(),
        })
    }

    fn context_for(&self, node: &ExecutionNode) -> ExecutionContext {
        ExecutionContext {
            environment_id: self.environment_id,
            resource_id: node.id.clone(),
            generation: self.generation,
            operation_id: self.operation_id,
            idempotency_key: format!("{}:{}:{}", self.operation_id, self.generation, node.id),
            deadline_unix_ms: self.deadline_unix_ms,
        }
    }
}

fn reconcile_with_inspect<P: GraphProvider>(
    provider: &P,
    context: &ExecutionContext,
) -> Result<NodeResult, ProviderError> {
    match provider.reconcile(context) {
        Ok(result) => Ok(result),
        Err(ProviderError::LostResponse) => match provider.inspect(context)? {
            Some(result) => Ok(result),
            None => provider.reconcile(context),
        },
        Err(error) => Err(error),
    }
}

fn compensate<P: GraphProvider, J: ExecutionJournal>(
    provider: &P,
    journal: &J,
    plan: &ExecutionPlan,
    nodes: &BTreeMap<String, ExecutionNode>,
    completed: &BTreeMap<String, NodeResult>,
) -> BTreeSet<String> {
    let mut quarantined = BTreeSet::new();
    for resource_id in reverse_dependency_order(nodes, completed) {
        let Some(result) = completed.get(&resource_id) else {
            continue;
        };
        let Some(node) = nodes.get(&resource_id) else {
            continue;
        };
        let context = plan.context_for(node);
        match result.ownership {
            NodeOwnership::NewlyCreatedEphemeral => {
                if provider.stop(&context).is_err() {
                    let _ = journal.record_quarantine(&context, "failed compensation");
                    quarantined.insert(resource_id);
                }
            }
            NodeOwnership::Uncertain => {
                let _ =
                    journal.record_quarantine(&context, "ownership uncertain after failed start");
                quarantined.insert(resource_id);
            }
            NodeOwnership::Preexisting => {}
        }
    }
    quarantined
}

fn with_compensation(error: ExecutionError, quarantined: &BTreeSet<String>) -> ExecutionError {
    if quarantined.is_empty() {
        error
    } else {
        ExecutionError::Provider {
            resource: "compensation".into(),
            message: format!("{error}; quarantined {quarantined:?}"),
        }
    }
}

fn node_map(nodes: &[ExecutionNode]) -> Result<BTreeMap<String, ExecutionNode>, ExecutionError> {
    let mut mapped = BTreeMap::new();
    for node in nodes {
        if node.id.is_empty() || mapped.insert(node.id.clone(), node.clone()).is_some() {
            return Err(ExecutionError::UnknownResource(node.id.clone()));
        }
    }
    for node in mapped.values() {
        for dependency in node.dependencies.iter().chain(&node.readiness_dependencies) {
            if !mapped.contains_key(dependency) {
                return Err(ExecutionError::UnknownResource(dependency.clone()));
            }
        }
    }
    Ok(mapped)
}

fn stages(nodes: &BTreeMap<String, ExecutionNode>) -> Result<Vec<Vec<String>>, ExecutionError> {
    let mut remaining = nodes
        .iter()
        .map(|(id, node)| {
            (
                id.clone(),
                node.dependencies
                    .iter()
                    .chain(&node.readiness_dependencies)
                    .cloned()
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut output = Vec::new();
    while !remaining.is_empty() {
        let stage = remaining
            .iter()
            .filter(|(_, dependencies)| dependencies.is_empty())
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        if stage.is_empty() {
            return Err(ExecutionError::Cycle);
        }
        for id in &stage {
            remaining.remove(id);
        }
        for dependencies in remaining.values_mut() {
            for id in &stage {
                dependencies.remove(id);
            }
        }
        output.push(stage);
    }
    Ok(output)
}

fn dependencies_ready(node: &ExecutionNode, completed: &BTreeMap<String, NodeResult>) -> bool {
    node.dependencies
        .iter()
        .chain(&node.readiness_dependencies)
        .all(|dependency| completed.contains_key(dependency))
}

fn reverse_dependency_order(
    nodes: &BTreeMap<String, ExecutionNode>,
    completed: &BTreeMap<String, NodeResult>,
) -> Vec<String> {
    let mut ids = completed.keys().cloned().collect::<Vec<_>>();
    ids.sort_by_key(|id| {
        nodes.get(id).map_or(0, |node| {
            node.dependencies.len() + node.readiness_dependencies.len()
        })
    });
    ids.reverse();
    ids
}

/// Per-environment operation coalescing. The small lock is held only while the
/// registry is updated, never during provider calls.
#[derive(Default)]
pub struct OperationCoordinator {
    active: Mutex<BTreeMap<String, Uuid>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationDisposition {
    Leader(Uuid),
    Existing(Uuid),
}

impl OperationCoordinator {
    #[must_use]
    pub fn begin(&self, environment_key: &str, operation_id: Uuid) -> OperationDisposition {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(operation_id) = active.get(environment_key) {
            OperationDisposition::Existing(*operation_id)
        } else {
            active.insert(environment_key.into(), operation_id);
            OperationDisposition::Leader(operation_id)
        }
    }

    pub fn finish(&self, environment_key: &str, operation_id: Uuid) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active
            .get(environment_key)
            .is_some_and(|current| *current == operation_id)
        {
            active.remove(environment_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier, Mutex};

    #[derive(Default)]
    struct Journal {
        generation: u64,
        intents: Mutex<Vec<String>>,
        results: Mutex<Vec<String>>,
        quarantined: Mutex<Vec<String>>,
    }

    impl ExecutionJournal for Journal {
        fn current_generation(&self, _: Uuid) -> Result<u64, String> {
            Ok(self.generation)
        }
        fn record_intent(&self, context: &ExecutionContext) -> Result<(), String> {
            self.intents
                .lock()
                .unwrap()
                .push(context.resource_id.clone());
            Ok(())
        }
        fn record_result(&self, context: &ExecutionContext, _: &NodeResult) -> Result<(), String> {
            self.results
                .lock()
                .unwrap()
                .push(context.resource_id.clone());
            Ok(())
        }
        fn record_quarantine(&self, context: &ExecutionContext, _: &str) -> Result<(), String> {
            self.quarantined
                .lock()
                .unwrap()
                .push(context.resource_id.clone());
            Ok(())
        }
    }

    struct Provider {
        lost: Mutex<BTreeSet<String>>,
        stopped: Mutex<Vec<String>>,
    }
    impl GraphProvider for Provider {
        fn reconcile(&self, context: &ExecutionContext) -> Result<NodeResult, ProviderError> {
            if self.lost.lock().unwrap().remove(&context.resource_id) {
                return Err(ProviderError::LostResponse);
            }
            Ok(NodeResult {
                ownership: NodeOwnership::NewlyCreatedEphemeral,
                readiness: BTreeSet::from(["ready".into()]),
                observed: Value::Null,
            })
        }
        fn inspect(&self, context: &ExecutionContext) -> Result<Option<NodeResult>, ProviderError> {
            Ok(Some(NodeResult {
                ownership: NodeOwnership::NewlyCreatedEphemeral,
                readiness: BTreeSet::from(["ready".into()]),
                observed: serde_json::json!({"inspected": context.resource_id}),
            }))
        }
        fn stop(&self, context: &ExecutionContext) -> Result<(), ProviderError> {
            self.stopped
                .lock()
                .unwrap()
                .push(context.resource_id.clone());
            Ok(())
        }
    }

    fn plan() -> ExecutionPlan {
        ExecutionPlan {
            environment_id: Uuid::now_v7(),
            generation: 4,
            operation_id: Uuid::now_v7(),
            deadline_unix_ms: u128::MAX,
            nodes: vec![
                ExecutionNode {
                    id: "backend".into(),
                    dependencies: vec![],
                    readiness_dependencies: vec![],
                    ephemeral: true,
                },
                ExecutionNode {
                    id: "metro".into(),
                    dependencies: vec!["backend".into()],
                    readiness_dependencies: vec![],
                    ephemeral: true,
                },
                ExecutionNode {
                    id: "device".into(),
                    dependencies: vec![],
                    readiness_dependencies: vec![],
                    ephemeral: true,
                },
            ],
        }
    }

    #[test]
    fn runs_eligible_branches_and_inspects_before_retrying_a_lost_response() {
        let plan = plan();
        let journal = Journal {
            generation: 4,
            ..Journal::default()
        };
        let provider = Provider {
            lost: Mutex::new(BTreeSet::from(["backend".into()])),
            stopped: Mutex::new(Vec::new()),
        };
        let result = plan.execute(&provider, &journal).expect("graph runs");
        assert_eq!(result.completed.len(), 3);
        assert_eq!(journal.intents.lock().unwrap().len(), 3);
        assert!(journal.results.lock().unwrap().contains(&"backend".into()));
    }

    #[test]
    fn rejects_stale_generations_and_coalesces_concurrent_environment_operations() {
        let plan = plan();
        let journal = Journal {
            generation: 5,
            ..Journal::default()
        };
        let provider = Provider {
            lost: Mutex::new(BTreeSet::new()),
            stopped: Mutex::new(Vec::new()),
        };
        assert!(matches!(
            plan.execute(&provider, &journal),
            Err(ExecutionError::StaleGeneration { .. })
        ));
        let coordinator = Arc::new(OperationCoordinator::default());
        let barrier = Arc::new(Barrier::new(2));
        let operation = plan.operation_id;
        let first_coordinator = Arc::clone(&coordinator);
        let first_barrier = Arc::clone(&barrier);
        let first = std::thread::spawn(move || {
            first_barrier.wait();
            first_coordinator.begin("same", operation)
        });
        let second_coordinator = Arc::clone(&coordinator);
        let second_barrier = Arc::clone(&barrier);
        let second = std::thread::spawn(move || {
            second_barrier.wait();
            second_coordinator.begin("same", Uuid::now_v7())
        });
        let first = first.join().expect("first operation joins");
        let second = second.join().expect("second operation joins");
        let leaders = [first, second]
            .into_iter()
            .filter(|value| matches!(value, OperationDisposition::Leader(_)))
            .count();
        assert_eq!(leaders, 1);
        let operations = [first, second]
            .into_iter()
            .map(|value| match value {
                OperationDisposition::Leader(id) | OperationDisposition::Existing(id) => id,
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(operations.len(), 1);
    }
}
