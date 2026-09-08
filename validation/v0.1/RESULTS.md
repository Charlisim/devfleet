# v0.1 release evidence

## Portable evidence

| Cases | Evidence | Coverage |
|---|---|---|
| AC-01, AC-05, AC-06, AC-16 | `validation/ios-isolation/evidence` and its Rust validation | two real detached worktrees, Metro/backend/UDID separation, JS edit, compiled configuration, crosswire detection |
| AC-02, AC-15 | `validation/compose-isolation` | two 15-service Compose projects, loopback APIs, separate volumes and controlled stop |
| AC-03, AC-04 | `crates/devfleet-core` ports/execution tests and `crates/devfleet-state` lease tests | graph ordering, idempotency, guarded endpoint handoff and retained port leases |
| AC-07, AC-08, AC-09 | `crates/devfleet-daemon/tests/process_lifecycle.rs` | stop/destroy, restart/reconciliation, log and operation recovery |
| AC-10, AC-11 | `crates/devfleet-daemon/tests/worktree_identity.rs` and `process_lifecycle.rs` | Git worktree identity, recreation and concurrent operation handling |
| AC-12, AC-13 | daemon capture tests and plugin SDK tests | owned capture artifact checks, protocol/plugin compatibility |
| AC-14 | `crates/devfleet-core/src/scheduler.rs` | bounded scheduling and controlled aging |
| protocol faults and 20-environment pressure | `crates/devfleet-testkit` | controllable clock; malformed, duplicate and lost replies; bounded logs; cancellation/lifecycle cycles; 100 simulated reaps across four projects |

The retained iOS result passed nine application checks on 2026-09-07. The
retained Compose result passed two-worktree fifteen-service isolation on
2026-09-08. Both are historical host evidence; `verify-v01-release-gates.sh`
checks their structure and regression behavior but does not replay hardware.

## Measured local calibration

[`HARDWARE-RESULTS.json`](HARDWARE-RESULTS.json) records one local release-binary
calibration on 2026-09-08: 100 `environment.status` samples had p95 **40.861
ms** against 200 ms; the idle daemon RSS was **5,904 KiB** against 100 MiB. The
sample starts one process-backed environment and excludes that process and other
external tools from RSS. It is a measured local result, not a cross-project or
simulator-capacity claim.

[`HARDWARE-RESULTS-4.json`](HARDWARE-RESULTS-4.json) and
[`HARDWARE-RESULTS-10.json`](HARDWARE-RESULTS-10.json) record process-backed
four- and ten-environment runs on the same host. They measured 36.678 ms / 5,504
KiB / 0.9% CPU and 38.396 ms / 5,824 KiB / 0.7% CPU respectively; both reports
also retain host CPU, memory, disk, service count and zero native-build
concurrency. The ten-environment run sampled each environment round-robin and
stopped every owned process before cleanup.

Those are process-provider measurements only. They neither exercise simulators
or native builds nor establish simulator capacity. A target iOS/Compose hardware
replay remains required before release if that workload or matrix changes.
