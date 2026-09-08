# v0.1 release gates

This directory makes the release evidence reproducible. It separates the
portable suite from host-dependent gates; a passing portable suite is never a
claim about unmeasured Apple hardware.

## Portable gate

From the repository root:

```sh
./scripts/verify-v01-release-gates.sh
```

It checks formatting, lint, locked builds, the full workspace suite, dependency
boundaries, the reusable simulation testkit, and the checked-in iOS evidence
shape. It does not boot a simulator, use Docker, or claim a performance result.

## Hardware gates

Run on the target macOS Apple Silicon matrix in a disposable state directory:

```sh
cargo build --locked --manifest-path validation/ios-isolation/Cargo.toml
validation/ios-isolation/target/debug/ios-isolation prepare "$PWD" /tmp/devfleet-ios-proof
validation/ios-isolation/target/debug/ios-isolation run /tmp/devfleet-ios-proof
./scripts/measure-v01-daemon.sh validation/v0.1/HARDWARE-RESULTS.json "$PWD" 1
./scripts/measure-v01-daemon.sh validation/v0.1/HARDWARE-RESULTS-4.json "$PWD" 4
./scripts/measure-v01-daemon.sh validation/v0.1/HARDWARE-RESULTS-10.json "$PWD" 10
```

The iOS fixture creates exactly two detached worktrees and two owned UDIDs. It
tests independent Metro/backend routes, a JavaScript-only edit, a compiled
endpoint change, both deliberate crosswires, and restoration. It retains the
fixture resources for inspection; follow its README before any cleanup.

Run the fifteen-service Compose fixture from two Git worktrees with separate
managed `DEVFLEET_API_PORT` values. Verify both health endpoints, write a marker
only to the first `files` volume, stop through DevFleet, then record the two
project names, endpoints, volume outcome, CPU, memory, disk, service count and
build concurrency in `HARDWARE-RESULTS.json` or a dated sibling. The fixture
does not remove containers, volumes, simulators, worktrees or evidence.

`measure-v01-daemon.sh` builds release binaries, starts one private temporary
daemon, creates the requested number of trusted process environments, takes 100
round-robin `environment.status` samples, reads that daemon's RSS/CPU, stops
every owned environment, and writes one non-overwriting JSON result. Its 200 ms
p95 and 100 MiB RSS fields are measured comparisons, not guarantees. The
process fixture and external tools are excluded from the daemon RSS.

## Scope

The recorded iOS matrix is macOS 26.6.2 arm64, Xcode 26.4.1, iOS Simulator
26.4.1, React Native 0.87.1, Node 26.7.0, Rust 1.98.0 and CocoaPods 1.16.2.
Any change to that matrix requires a fresh hardware result. The checked-in
four- and ten-environment runs use the process provider only; simulator runs
remain hardware-gated. Do not infer universal simulator capacity from any of
these fixtures.
