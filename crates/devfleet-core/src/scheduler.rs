//! Deterministic host-budget scheduling for expensive local operations.

use devfleet_protocol::{
    ResourcePressureSnapshot, ScheduledJobSnapshot, ScheduledJobState, ScheduledResource,
    SchedulerConfig, SchedulerSnapshot, SchedulingPriority, SchedulingRequest,
};
use std::collections::BTreeMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SchedulerError {
    #[error("scheduler configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error(
        "RESOURCE_BUDGET_EXCEEDED: measured={measured:?} estimated_active={estimated_active} requested={requested} limit={limit} reserve={reserve}"
    )]
    ResourceBudgetExceeded {
        measured: Option<u64>,
        estimated_active: u64,
        requested: u64,
        limit: u64,
        reserve: u64,
    },
    #[error("scheduled job was not found")]
    NotFound,
    #[error("scheduled job is not running")]
    NotRunning,
    #[error("resource budget is currently unavailable: {0}")]
    ResourceUnavailable(String),
}

#[derive(Clone, Debug)]
struct ScheduledJob {
    snapshot: ScheduledJobSnapshot,
    sequence: u64,
    queued_at_ms: u64,
}

/// Serially owned scheduler state. Time is supplied by callers so tests can
/// prove fairness without wall-clock sleeps.
pub struct HostScheduler {
    config: SchedulerConfig,
    measured_memory_bytes: Option<u64>,
    focused_environment: Option<Uuid>,
    sequence: u64,
    jobs: BTreeMap<Uuid, ScheduledJob>,
}

impl Default for HostScheduler {
    fn default() -> Self {
        Self::new(SchedulerConfig::default()).expect("default scheduler configuration is valid")
    }
}

impl HostScheduler {
    /// Creates an empty scheduler with explicit host limits.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured resource limit cannot admit work.
    pub fn new(config: SchedulerConfig) -> Result<Self, SchedulerError> {
        validate_config(&config)?;
        Ok(Self {
            config,
            measured_memory_bytes: None,
            focused_environment: None,
            sequence: 0,
            jobs: BTreeMap::new(),
        })
    }

    /// Replaces host limits without cancelling or preempting work already running.
    ///
    /// # Errors
    ///
    /// Returns an error when new limits are invalid or below active resources.
    pub fn configure(&mut self, config: SchedulerConfig) -> Result<(), SchedulerError> {
        validate_config(&config)?;
        let usage = self.usage();
        if usage.native_builds > config.native_build_limit
            || usage.device_boots > config.device_boot_limit
            || usage.preparations > config.preparation_limit
            || usage.metro_workers > config.metro_worker_limit
        {
            return Err(SchedulerError::InvalidConfig(
                "new limits cannot preempt active work".into(),
            ));
        }
        self.config = config;
        Ok(())
    }

    /// Updates measured host memory. Estimates remain separate and are added
    /// conservatively during admission.
    pub fn set_measured_memory(&mut self, bytes: Option<u64>) {
        self.measured_memory_bytes = bytes;
    }

    /// Marks one environment as focused. Focus only biases future admission;
    /// it never cancels or demotes running work.
    pub fn focus(&mut self, environment_id: Uuid) {
        self.focused_environment = Some(environment_id);
    }

    /// Queues work without starting it. Call `admit_next` to make one queued
    /// job running after limits are checked.
    ///
    /// # Errors
    ///
    /// Returns an error when the request can never fit the configured budget.
    pub fn enqueue(
        &mut self,
        request: SchedulingRequest,
        now_ms: u64,
    ) -> Result<Uuid, SchedulerError> {
        validate_request(&self.config, &request)?;
        let job_id = Uuid::now_v7();
        self.sequence = self.sequence.checked_add(1).unwrap_or(0);
        self.jobs.insert(
            job_id,
            ScheduledJob {
                snapshot: ScheduledJobSnapshot {
                    job_id,
                    environment_id: request.environment_id,
                    project_id: request.project_id,
                    resource: request.resource,
                    priority: request.priority,
                    focused: request.focused,
                    state: ScheduledJobState::Queued,
                    queued_reason: Some("awaiting host budget".into()),
                    estimated_memory_bytes: request.estimated_memory_bytes,
                },
                sequence: self.sequence,
                queued_at_ms: now_ms,
            },
        );
        Ok(job_id)
    }

    /// Starts the highest effective-priority admissible job. Aging is based on
    /// the supplied controlled clock, so older maintenance work eventually wins.
    ///
    /// # Errors
    ///
    /// Returns a measured-plus-estimated diagnostic when memory is the only
    /// admission blocker.
    pub fn admit_next(&mut self, now_ms: u64) -> Result<Option<Uuid>, SchedulerError> {
        let mut pending = self
            .jobs
            .values()
            .filter(|job| job.snapshot.state == ScheduledJobState::Queued)
            .map(|job| {
                (
                    priority_score(
                        job,
                        self.focused_environment,
                        self.config.aging_interval_ms,
                        now_ms,
                    ),
                    job.sequence,
                    job.snapshot.job_id,
                )
            })
            .collect::<Vec<_>>();
        pending.sort_unstable();
        let mut memory_error = None;
        for (_, _, job_id) in pending {
            match self.admission(job_id) {
                Ok(()) => {
                    let job = self.jobs.get_mut(&job_id).ok_or(SchedulerError::NotFound)?;
                    job.snapshot.state = ScheduledJobState::Running;
                    job.snapshot.queued_reason = None;
                    return Ok(Some(job_id));
                }
                Err(error @ SchedulerError::ResourceBudgetExceeded { .. }) => {
                    let job = self.jobs.get_mut(&job_id).ok_or(SchedulerError::NotFound)?;
                    job.snapshot.queued_reason = Some(error.to_string());
                    memory_error.get_or_insert(error);
                }
                Err(SchedulerError::ResourceUnavailable(reason)) => {
                    let job = self.jobs.get_mut(&job_id).ok_or(SchedulerError::NotFound)?;
                    job.snapshot.queued_reason = Some(reason);
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(error) = memory_error {
            return Err(error);
        }
        self.refresh_queued_reasons();
        Ok(None)
    }

    /// Releases completed work. It does not admit another job implicitly.
    ///
    /// # Errors
    ///
    /// Returns an error when the job does not currently own a host budget.
    pub fn complete(&mut self, job_id: Uuid) -> Result<(), SchedulerError> {
        let job = self.jobs.get_mut(&job_id).ok_or(SchedulerError::NotFound)?;
        if job.snapshot.state != ScheduledJobState::Running {
            return Err(SchedulerError::NotRunning);
        }
        job.snapshot.state = ScheduledJobState::Completed;
        Ok(())
    }

    /// Completes all running work owned by one environment without preempting
    /// another environment. Used after a manual stop or verified destroy.
    pub fn complete_environment(&mut self, environment_id: Uuid) -> Vec<Uuid> {
        let mut completed = Vec::new();
        for job in self.jobs.values_mut().filter(|job| {
            job.snapshot.environment_id == environment_id
                && job.snapshot.state == ScheduledJobState::Running
        }) {
            job.snapshot.state = ScheduledJobState::Completed;
            completed.push(job.snapshot.job_id);
        }
        completed
    }

    /// Prevents a queued job from starting. Running work is deliberately left alone.
    ///
    /// # Errors
    ///
    /// Returns an error when the job is unknown.
    pub fn cancel(&mut self, job_id: Uuid) -> Result<bool, SchedulerError> {
        self.transition_waiting(job_id, ScheduledJobState::Cancelled)
    }

    /// Prevents new work after a caller loses control. Running work remains intact.
    ///
    /// # Errors
    ///
    /// Returns an error when the job is unknown.
    pub fn control_lost(&mut self, job_id: Uuid) -> Result<bool, SchedulerError> {
        self.transition_waiting(job_id, ScheduledJobState::ControlLost)
    }

    /// Returns queue state and measured versus estimated pressure.
    #[must_use]
    pub fn snapshot(&self) -> SchedulerSnapshot {
        let mut jobs = self
            .jobs
            .values()
            .map(|job| job.snapshot.clone())
            .collect::<Vec<_>>();
        jobs.sort_by_key(|job| job.job_id);
        let usage = self.usage();
        SchedulerSnapshot {
            config: self.config.clone(),
            pressure: ResourcePressureSnapshot {
                measured_memory_bytes: self.measured_memory_bytes,
                estimated_active_memory_bytes: usage.estimated_memory,
                estimated_queued_memory_bytes: self
                    .jobs
                    .values()
                    .filter(|job| job.snapshot.state == ScheduledJobState::Queued)
                    .map(|job| job.snapshot.estimated_memory_bytes)
                    .sum(),
                memory_limit_bytes: self.config.memory_limit_bytes,
                memory_reserve_bytes: self.config.memory_reserve_bytes,
                native_builds_active: usage.native_builds,
                device_boots_active: usage.device_boots,
                preparations_active: usage.preparations,
                metro_workers_active: usage.metro_workers,
            },
            jobs,
        }
    }

    fn transition_waiting(
        &mut self,
        job_id: Uuid,
        target: ScheduledJobState,
    ) -> Result<bool, SchedulerError> {
        let job = self.jobs.get_mut(&job_id).ok_or(SchedulerError::NotFound)?;
        if job.snapshot.state != ScheduledJobState::Queued {
            return Ok(false);
        }
        job.snapshot.state = target;
        job.snapshot.queued_reason =
            Some("admission prevented by cancellation or control loss".into());
        Ok(true)
    }

    fn admission(&self, job_id: Uuid) -> Result<(), SchedulerError> {
        let job = self.jobs.get(&job_id).ok_or(SchedulerError::NotFound)?;
        let usage = self.usage();
        if !resource_fits(&self.config, &usage, &job.snapshot.resource) {
            return Err(SchedulerError::ResourceUnavailable(resource_reason(
                &job.snapshot.resource,
            )));
        }
        let measured = self.measured_memory_bytes;
        let total = measured
            .unwrap_or(0)
            .saturating_add(usage.estimated_memory)
            .saturating_add(job.snapshot.estimated_memory_bytes)
            .saturating_add(self.config.memory_reserve_bytes);
        if total > self.config.memory_limit_bytes {
            return Err(SchedulerError::ResourceBudgetExceeded {
                measured,
                estimated_active: usage.estimated_memory,
                requested: job.snapshot.estimated_memory_bytes,
                limit: self.config.memory_limit_bytes,
                reserve: self.config.memory_reserve_bytes,
            });
        }
        Ok(())
    }

    fn refresh_queued_reasons(&mut self) {
        let usage = self.usage();
        for job in self
            .jobs
            .values_mut()
            .filter(|job| job.snapshot.state == ScheduledJobState::Queued)
        {
            if !resource_fits(&self.config, &usage, &job.snapshot.resource) {
                job.snapshot.queued_reason = Some(resource_reason(&job.snapshot.resource));
            }
        }
    }

    fn usage(&self) -> Usage {
        self.jobs
            .values()
            .filter(|job| job.snapshot.state == ScheduledJobState::Running)
            .fold(Usage::default(), |mut usage, job| {
                usage.estimated_memory = usage
                    .estimated_memory
                    .saturating_add(job.snapshot.estimated_memory_bytes);
                match job.snapshot.resource {
                    ScheduledResource::NativeBuild => usage.native_builds += 1,
                    ScheduledResource::DeviceBoot => usage.device_boots += 1,
                    ScheduledResource::Preparation => usage.preparations += 1,
                    ScheduledResource::MetroWorkers { workers } => {
                        usage.metro_workers = usage.metro_workers.saturating_add(workers);
                    }
                }
                usage
            })
    }
}

#[derive(Default)]
struct Usage {
    estimated_memory: u64,
    native_builds: u16,
    device_boots: u16,
    preparations: u16,
    metro_workers: u16,
}

fn validate_config(config: &SchedulerConfig) -> Result<(), SchedulerError> {
    if config.native_build_limit == 0
        || config.device_boot_limit == 0
        || config.preparation_limit == 0
        || config.metro_worker_limit == 0
        || config.memory_limit_bytes == 0
        || config.memory_reserve_bytes >= config.memory_limit_bytes
        || config.aging_interval_ms == 0
    {
        return Err(SchedulerError::InvalidConfig(
            "limits must be positive and reserve smaller than memory limit".into(),
        ));
    }
    Ok(())
}

fn validate_request(
    config: &SchedulerConfig,
    request: &SchedulingRequest,
) -> Result<(), SchedulerError> {
    if request.estimated_memory_bytes == 0 {
        return Err(SchedulerError::InvalidConfig(
            "heavy work requires a nonzero memory estimate".into(),
        ));
    }
    if let ScheduledResource::MetroWorkers { workers } = request.resource
        && (workers == 0 || workers > config.metro_worker_limit)
    {
        return Err(SchedulerError::InvalidConfig(
            "Metro worker request exceeds the configured limit".into(),
        ));
    }
    Ok(())
}

fn resource_fits(config: &SchedulerConfig, usage: &Usage, resource: &ScheduledResource) -> bool {
    match resource {
        ScheduledResource::NativeBuild => usage.native_builds < config.native_build_limit,
        ScheduledResource::DeviceBoot => usage.device_boots < config.device_boot_limit,
        ScheduledResource::Preparation => usage.preparations < config.preparation_limit,
        ScheduledResource::MetroWorkers { workers } => {
            usage.metro_workers.saturating_add(*workers) <= config.metro_worker_limit
        }
    }
}

fn resource_reason(resource: &ScheduledResource) -> String {
    match resource {
        ScheduledResource::NativeBuild => "native build slots exhausted".into(),
        ScheduledResource::DeviceBoot => "device boot slots exhausted".into(),
        ScheduledResource::Preparation => "preparation slots exhausted".into(),
        ScheduledResource::MetroWorkers { .. } => "Metro worker budget exhausted".into(),
    }
}

fn priority_score(
    job: &ScheduledJob,
    focused_environment: Option<Uuid>,
    aging_interval_ms: u64,
    now_ms: u64,
) -> (i128, u64) {
    let base = match job.snapshot.priority {
        SchedulingPriority::Interactive => 0_i128,
        SchedulingPriority::Background => i128::from(aging_interval_ms),
        SchedulingPriority::Maintenance => i128::from(aging_interval_ms) * 2,
    };
    let focused = job.snapshot.focused || focused_environment == Some(job.snapshot.environment_id);
    let focus_bonus = if focused {
        i128::from(aging_interval_ms) / 2
    } else {
        0
    };
    let age = i128::from(now_ms.saturating_sub(job.queued_at_ms));
    (base - focus_bonus - age, job.sequence)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        environment_id: Uuid,
        project_id: Uuid,
        resource: ScheduledResource,
        priority: SchedulingPriority,
    ) -> SchedulingRequest {
        SchedulingRequest {
            environment_id,
            project_id,
            resource,
            priority,
            focused: false,
            estimated_memory_bytes: 64,
        }
    }

    fn scheduler() -> HostScheduler {
        HostScheduler::new(SchedulerConfig {
            native_build_limit: 2,
            device_boot_limit: 2,
            preparation_limit: 2,
            metro_worker_limit: 4,
            memory_limit_bytes: 1_024,
            memory_reserve_bytes: 64,
            aging_interval_ms: 10,
        })
        .expect("scheduler config")
    }

    #[test]
    fn controlled_clock_ages_twenty_environments_without_starvation() {
        let mut scheduler = scheduler();
        let project_ids = [Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7()];
        let mut jobs = Vec::new();
        for index in 0..20 {
            jobs.push(
                scheduler
                    .enqueue(
                        request(
                            Uuid::now_v7(),
                            project_ids[index % project_ids.len()],
                            ScheduledResource::Preparation,
                            SchedulingPriority::Maintenance,
                        ),
                        0,
                    )
                    .expect("queue work"),
            );
        }
        let interactive = scheduler
            .enqueue(
                request(
                    Uuid::now_v7(),
                    project_ids[0],
                    ScheduledResource::Preparation,
                    SchedulingPriority::Interactive,
                ),
                1,
            )
            .expect("queue interactive work");
        let mut completed = Vec::new();
        for now in 1..22 {
            let job = scheduler
                .admit_next(u64::try_from(now).expect("clock fits"))
                .expect("admission")
                .expect("queued job admits");
            scheduler.complete(job).expect("job completes");
            completed.push(job);
        }
        assert_eq!(completed.len(), 21);
        assert_eq!(completed[0], interactive);
        assert!(jobs.iter().all(|job| completed.contains(job)));
    }

    #[test]
    fn cancellation_or_control_loss_never_starts_new_work() {
        let mut scheduler = scheduler();
        let cancelled = scheduler
            .enqueue(
                request(
                    Uuid::now_v7(),
                    Uuid::now_v7(),
                    ScheduledResource::NativeBuild,
                    SchedulingPriority::Interactive,
                ),
                0,
            )
            .expect("queue cancelled job");
        let lost = scheduler
            .enqueue(
                request(
                    Uuid::now_v7(),
                    Uuid::now_v7(),
                    ScheduledResource::DeviceBoot,
                    SchedulingPriority::Interactive,
                ),
                0,
            )
            .expect("queue lost job");
        assert!(scheduler.cancel(cancelled).expect("cancel job"));
        assert!(scheduler.control_lost(lost).expect("lose control"));
        assert_eq!(scheduler.admit_next(1).expect("admission"), None);
        let states = scheduler
            .snapshot()
            .jobs
            .into_iter()
            .map(|job| (job.job_id, job.state))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(states[&cancelled], ScheduledJobState::Cancelled);
        assert_eq!(states[&lost], ScheduledJobState::ControlLost);
    }

    #[test]
    fn enforces_independent_slots_and_reports_measured_memory_pressure() {
        let mut scheduler = scheduler();
        let project = Uuid::now_v7();
        let first = scheduler
            .enqueue(
                request(
                    Uuid::now_v7(),
                    project,
                    ScheduledResource::NativeBuild,
                    SchedulingPriority::Interactive,
                ),
                0,
            )
            .expect("first native build");
        let second = scheduler
            .enqueue(
                request(
                    Uuid::now_v7(),
                    project,
                    ScheduledResource::NativeBuild,
                    SchedulingPriority::Interactive,
                ),
                0,
            )
            .expect("second native build");
        let boot = scheduler
            .enqueue(
                request(
                    Uuid::now_v7(),
                    project,
                    ScheduledResource::DeviceBoot,
                    SchedulingPriority::Background,
                ),
                0,
            )
            .expect("boot queues independently");
        assert_eq!(scheduler.admit_next(1).expect("first admits"), Some(first));
        assert_eq!(
            scheduler.admit_next(1).expect("second admits"),
            Some(second)
        );
        assert_eq!(scheduler.admit_next(1).expect("boot admits"), Some(boot));
        scheduler.set_measured_memory(Some(900));
        let blocked = scheduler
            .enqueue(
                request(
                    Uuid::now_v7(),
                    project,
                    ScheduledResource::Preparation,
                    SchedulingPriority::Interactive,
                ),
                1,
            )
            .expect("preparation queues");
        assert!(matches!(
            scheduler.admit_next(2),
            Err(SchedulerError::ResourceBudgetExceeded {
                measured: Some(900),
                ..
            })
        ));
        assert!(
            scheduler
                .snapshot()
                .jobs
                .into_iter()
                .any(|job| job.job_id == blocked && job.queued_reason.is_some())
        );
    }

    #[test]
    fn focus_biases_future_work_without_preempting_a_running_environment() {
        let mut scheduler = scheduler();
        let active_environment = Uuid::now_v7();
        let focused_environment = Uuid::now_v7();
        let project = Uuid::now_v7();
        let active = scheduler
            .enqueue(
                request(
                    active_environment,
                    project,
                    ScheduledResource::Preparation,
                    SchedulingPriority::Background,
                ),
                0,
            )
            .expect("active work queues");
        assert_eq!(
            scheduler.admit_next(1).expect("active admits"),
            Some(active)
        );
        scheduler.focus(focused_environment);
        let focused = scheduler
            .enqueue(
                request(
                    focused_environment,
                    project,
                    ScheduledResource::Preparation,
                    SchedulingPriority::Background,
                ),
                2,
            )
            .expect("focused queues");
        assert_eq!(
            scheduler
                .snapshot()
                .jobs
                .iter()
                .find(|job| job.job_id == active)
                .expect("active snapshot")
                .state,
            ScheduledJobState::Running
        );
        scheduler.complete(active).expect("manual completion");
        assert_eq!(
            scheduler.admit_next(3).expect("focused admits"),
            Some(focused)
        );
    }
}
