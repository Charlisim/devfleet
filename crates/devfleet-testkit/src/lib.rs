//! Shared deterministic helpers for black-box binary and provider tests.

use devfleet_core::{
    ExecutionContext, ExecutionJournal, GraphProvider, NodeOwnership, NodeResult, PortBlock,
    PortRange,
};
use devfleet_protocol::{PROTOCOL_VERSION, SmokeResponse, SmokeStatus};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};
use uuid::Uuid;

/// Parses and validates a smoke response emitted by a `DevFleet` binary.
///
/// # Panics
///
/// Panics when the output is not a valid matching smoke response.
pub fn assert_smoke_output(output: &[u8], component: &str) {
    let response: SmokeResponse = serde_json::from_slice(output).expect("valid smoke JSON");
    assert_eq!(response.component, component);
    assert_eq!(response.protocol, PROTOCOL_VERSION);
    assert_eq!(response.status, SmokeStatus::Ready);
}

/// A monotonic clock that tests advance without waiting for wall-clock time.
#[derive(Clone, Debug, Default)]
pub struct ControlledClock(Arc<Mutex<u64>>);

impl ControlledClock {
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self(Arc::new(Mutex::new(now_ms)))
    }

    #[must_use]
    pub fn now_ms(&self) -> u64 {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn advance_ms(&self, elapsed_ms: u64) -> u64 {
        let mut now = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *now = now.saturating_add(elapsed_ms);
        *now
    }
}

/// A deterministic provider fault, consumed in the order assigned to a resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderFault {
    LostReply,
    MalformedReply,
    DuplicateReply,
    InspectMissing,
    StopFailure,
}

/// One observed interaction with a [`SimulatedProvider`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProviderCall {
    Reconcile(String),
    Inspect(String),
    Stop(String),
}

/// A provider seam with scripted faults and observable call history.
#[derive(Default)]
pub struct SimulatedProvider {
    faults: Mutex<BTreeMap<String, VecDeque<ProviderFault>>>,
    calls: Mutex<Vec<ProviderCall>>,
    results: Mutex<BTreeMap<String, NodeResult>>,
}

impl SimulatedProvider {
    pub fn inject(&self, resource_id: impl Into<String>, fault: ProviderFault) {
        self.faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(resource_id.into())
            .or_default()
            .push_back(fault);
    }

    pub fn set_result(&self, resource_id: impl Into<String>, result: NodeResult) {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(resource_id.into(), result);
    }

    #[must_use]
    pub fn calls(&self) -> Vec<ProviderCall> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn take_fault(&self, resource_id: &str, expected: &[ProviderFault]) -> Option<ProviderFault> {
        let mut faults = self
            .faults
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let queue = faults.get_mut(resource_id)?;
        if queue.front().is_some_and(|fault| expected.contains(fault)) {
            queue.pop_front()
        } else {
            None
        }
    }

    fn result(&self, resource_id: &str) -> NodeResult {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(resource_id)
            .cloned()
            .unwrap_or_else(|| NodeResult {
                ownership: NodeOwnership::NewlyCreatedEphemeral,
                readiness: ["ready".into()].into_iter().collect(),
                observed: serde_json::json!({"resource": resource_id}),
            })
    }
}

impl GraphProvider for SimulatedProvider {
    fn reconcile(
        &self,
        context: &ExecutionContext,
    ) -> Result<NodeResult, devfleet_core::execution::ProviderError> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProviderCall::Reconcile(context.resource_id.clone()));
        match self.take_fault(
            &context.resource_id,
            &[
                ProviderFault::LostReply,
                ProviderFault::MalformedReply,
                ProviderFault::DuplicateReply,
            ],
        ) {
            Some(ProviderFault::LostReply) => {
                Err(devfleet_core::execution::ProviderError::LostResponse)
            }
            Some(ProviderFault::MalformedReply) => Err(
                devfleet_core::execution::ProviderError::Failed("malformed provider reply".into()),
            ),
            Some(ProviderFault::DuplicateReply) => Err(
                devfleet_core::execution::ProviderError::Failed("duplicate provider reply".into()),
            ),
            Some(ProviderFault::InspectMissing | ProviderFault::StopFailure) | None => {
                Ok(self.result(&context.resource_id))
            }
        }
    }

    fn inspect(
        &self,
        context: &ExecutionContext,
    ) -> Result<Option<NodeResult>, devfleet_core::execution::ProviderError> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProviderCall::Inspect(context.resource_id.clone()));
        if self
            .take_fault(&context.resource_id, &[ProviderFault::InspectMissing])
            .is_some()
        {
            Ok(None)
        } else {
            Ok(Some(self.result(&context.resource_id)))
        }
    }

    fn stop(
        &self,
        context: &ExecutionContext,
    ) -> Result<(), devfleet_core::execution::ProviderError> {
        self.calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProviderCall::Stop(context.resource_id.clone()));
        if self
            .take_fault(&context.resource_id, &[ProviderFault::StopFailure])
            .is_some()
        {
            Err(devfleet_core::execution::ProviderError::Failed(
                "simulated stop failure".into(),
            ))
        } else {
            Ok(())
        }
    }
}

/// In-memory durable-intent seam for graph tests.
pub struct SimulatedJournal {
    generation: u64,
    intents: Mutex<Vec<String>>,
    results: Mutex<Vec<String>>,
    quarantined: Mutex<Vec<String>>,
}

impl SimulatedJournal {
    #[must_use]
    pub const fn new(generation: u64) -> Self {
        Self {
            generation,
            intents: Mutex::new(Vec::new()),
            results: Mutex::new(Vec::new()),
            quarantined: Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    pub fn intents(&self) -> Vec<String> {
        self.intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn results(&self) -> Vec<String> {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[must_use]
    pub fn quarantined(&self) -> Vec<String> {
        self.quarantined
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ExecutionJournal for SimulatedJournal {
    fn current_generation(&self, _: Uuid) -> Result<u64, String> {
        Ok(self.generation)
    }

    fn record_intent(&self, context: &ExecutionContext) -> Result<(), String> {
        self.intents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(context.resource_id.clone());
        Ok(())
    }

    fn record_result(&self, context: &ExecutionContext, _: &NodeResult) -> Result<(), String> {
        self.results
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(context.resource_id.clone());
        Ok(())
    }

    fn record_quarantine(&self, context: &ExecutionContext, _: &str) -> Result<(), String> {
        self.quarantined
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(context.resource_id.clone());
        Ok(())
    }
}

/// Lifecycle state held by the bounded simulation fixture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SimulatedLifecycle {
    Planned,
    Running,
    CancellationRequested,
    Stopped,
    Destroyed,
}

struct SimulatedEnvironment {
    project_id: Uuid,
    lifecycle: SimulatedLifecycle,
    logs: VecDeque<String>,
    reaped: bool,
}

/// Bounded multi-environment fixture for log, cancellation and lifecycle tests.
pub struct SimulatedFleet {
    log_capacity: usize,
    environments: BTreeMap<Uuid, SimulatedEnvironment>,
    reaped_processes: usize,
}

impl SimulatedFleet {
    #[must_use]
    pub fn new(log_capacity: usize) -> Self {
        Self {
            log_capacity,
            environments: BTreeMap::new(),
            reaped_processes: 0,
        }
    }

    pub fn plan(&mut self, environment_id: Uuid, project_id: Uuid) {
        self.environments.insert(
            environment_id,
            SimulatedEnvironment {
                project_id,
                lifecycle: SimulatedLifecycle::Planned,
                logs: VecDeque::new(),
                reaped: false,
            },
        );
    }

    pub fn start(&mut self, environment_id: Uuid) -> bool {
        let Some(environment) = self.environments.get_mut(&environment_id) else {
            return false;
        };
        if matches!(
            environment.lifecycle,
            SimulatedLifecycle::Planned | SimulatedLifecycle::Stopped
        ) {
            environment.lifecycle = SimulatedLifecycle::Running;
            environment.reaped = false;
            true
        } else {
            false
        }
    }

    pub fn append_log(&mut self, environment_id: Uuid, line: impl Into<String>) -> bool {
        let Some(environment) = self.environments.get_mut(&environment_id) else {
            return false;
        };
        environment.logs.push_back(line.into());
        while environment.logs.len() > self.log_capacity {
            environment.logs.pop_front();
        }
        true
    }

    pub fn cancel(&mut self, environment_id: Uuid) -> bool {
        let Some(environment) = self.environments.get_mut(&environment_id) else {
            return false;
        };
        if environment.lifecycle == SimulatedLifecycle::Running {
            environment.lifecycle = SimulatedLifecycle::CancellationRequested;
            true
        } else {
            false
        }
    }

    pub fn stop(&mut self, environment_id: Uuid) -> bool {
        let Some(environment) = self.environments.get_mut(&environment_id) else {
            return false;
        };
        if matches!(
            environment.lifecycle,
            SimulatedLifecycle::Running | SimulatedLifecycle::CancellationRequested
        ) {
            environment.lifecycle = SimulatedLifecycle::Stopped;
            if !environment.reaped {
                environment.reaped = true;
                self.reaped_processes += 1;
            }
            true
        } else {
            false
        }
    }

    pub fn destroy(&mut self, environment_id: Uuid) -> bool {
        let Some(environment) = self.environments.get_mut(&environment_id) else {
            return false;
        };
        if environment.lifecycle == SimulatedLifecycle::Stopped {
            environment.lifecycle = SimulatedLifecycle::Destroyed;
            true
        } else {
            false
        }
    }

    #[must_use]
    pub fn lifecycle(&self, environment_id: Uuid) -> Option<SimulatedLifecycle> {
        self.environments
            .get(&environment_id)
            .map(|environment| environment.lifecycle)
    }

    #[must_use]
    pub fn log_count(&self) -> usize {
        self.environments
            .values()
            .map(|environment| environment.logs.len())
            .sum()
    }

    #[must_use]
    pub fn reaped_processes(&self) -> usize {
        self.reaped_processes
    }

    #[must_use]
    pub fn project_count(&self) -> usize {
        self.environments
            .values()
            .map(|environment| environment.project_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
    }
}

/// Checks that every allocator block is valid, in range and disjoint.
#[must_use]
pub fn allocator_blocks_are_disjoint(range: PortRange, size: u16) -> bool {
    if size == 0 {
        return false;
    }
    let mut ports = std::collections::BTreeSet::new();
    for start in range.block_starts(size) {
        let offsets = if size == 1 {
            BTreeMap::from([("first".into(), 0)])
        } else {
            BTreeMap::from([("first".into(), 0), ("last".into(), size - 1)])
        };
        let block = PortBlock {
            start,
            size,
            offsets,
        };
        let Ok(resolved) = block.ports(range) else {
            return false;
        };
        if !resolved.values().all(|port| ports.insert(*port)) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use devfleet_core::{ExecutionNode, ExecutionPlan};

    #[test]
    fn clock_and_allocator_properties_are_deterministic() {
        let clock = ControlledClock::new(10);
        assert_eq!(clock.now_ms(), 10);
        assert_eq!(clock.advance_ms(5), 15);
        assert_eq!(clock.advance_ms(u64::MAX), u64::MAX);
        let range = PortRange {
            start: 20_000,
            end: 20_255,
        };
        for size in [1, 2, 3, 8, 16, 64, 128] {
            assert!(allocator_blocks_are_disjoint(range, size));
        }
    }

    #[test]
    fn simulated_provider_recovers_lost_replies_and_exposes_faults() {
        let provider = SimulatedProvider::default();
        provider.inject("api", ProviderFault::LostReply);
        provider.inject("api", ProviderFault::InspectMissing);
        let plan = ExecutionPlan {
            environment_id: Uuid::now_v7(),
            generation: 3,
            operation_id: Uuid::now_v7(),
            deadline_unix_ms: 10_000,
            nodes: vec![ExecutionNode {
                id: "api".into(),
                dependencies: Vec::new(),
                readiness_dependencies: Vec::new(),
                ephemeral: true,
            }],
        };
        let journal = SimulatedJournal::new(3);
        let result = plan
            .execute(&provider, &journal)
            .expect("recover lost reply");
        assert_eq!(result.completed.len(), 1);
        assert_eq!(journal.intents(), vec!["api"]);
        assert_eq!(journal.results(), vec!["api"]);
        assert_eq!(
            provider.calls(),
            vec![
                ProviderCall::Reconcile("api".into()),
                ProviderCall::Inspect("api".into()),
                ProviderCall::Reconcile("api".into()),
            ]
        );
    }

    #[test]
    fn simulated_provider_rejects_malformed_or_duplicate_replies() {
        let context = ExecutionContext {
            environment_id: Uuid::now_v7(),
            resource_id: "api".into(),
            generation: 1,
            operation_id: Uuid::now_v7(),
            idempotency_key: "fixture".into(),
            deadline_unix_ms: 10_000,
        };
        for fault in [ProviderFault::MalformedReply, ProviderFault::DuplicateReply] {
            let provider = SimulatedProvider::default();
            provider.inject("api", fault);
            assert!(matches!(
                provider.reconcile(&context),
                Err(devfleet_core::execution::ProviderError::Failed(_))
            ));
        }
        let provider = SimulatedProvider::default();
        provider.inject("api", ProviderFault::StopFailure);
        assert!(provider.stop(&context).is_err());
    }

    #[test]
    fn twenty_environment_cycles_are_bounded_and_reaped() {
        let mut fleet = SimulatedFleet::new(16);
        let projects = (0..4).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
        let environments = (0..20).map(|_| Uuid::now_v7()).collect::<Vec<_>>();
        for (index, environment_id) in environments.iter().enumerate() {
            fleet.plan(*environment_id, projects[index % projects.len()]);
            for cycle in 0..5 {
                assert!(fleet.start(*environment_id));
                for line in 0..32 {
                    assert!(fleet.append_log(*environment_id, format!("{index}:{cycle}:{line}")));
                }
                assert!(fleet.cancel(*environment_id));
                assert!(fleet.stop(*environment_id));
            }
            assert!(fleet.destroy(*environment_id));
            assert_eq!(
                fleet.lifecycle(*environment_id),
                Some(SimulatedLifecycle::Destroyed)
            );
        }
        assert_eq!(fleet.project_count(), 4);
        assert_eq!(fleet.log_count(), 20 * 16);
        assert_eq!(fleet.reaped_processes(), 20 * 5);
    }
}
