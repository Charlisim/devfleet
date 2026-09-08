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

The report marks four- and ten-environment runs incomplete. Before release, run
the hardware procedure in `README.md` and retain CPU, memory, disk, service
count and build concurrency for those real runs. Missing or failed measurements
block the v0.1 release; this file deliberately makes no capacity or performance
guarantee.
