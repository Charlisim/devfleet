# Two-worktree iOS feasibility fixture

Bounded experiment for [issue #1](https://github.com/Charlisim/devfleet/issues/1), specification 0.1.0 AC-01, AC-05, AC-06 and AC-16. This is not the DevFleet CLI or a production adapter.

## Prerequisites and reproduction

The target matrix is macOS 26.6.2 (25G83), Apple Silicon arm64, Xcode 26.4.1 (17E202), iOS Simulator 26.4.1 (23E254a; runtime identifier iOS-26-4), React Native 0.87.1 and Community CLI 20.2.0. Use Node 26.7.0, Rust 1.98.0, Ruby 3.3.5 and CocoaPods 1.16.2. Confirm `pod --version` succeeds in the shell that starts the fixture. On the measured host, RVM CocoaPods worked; the Homebrew CocoaPods executable failed with a missing `nkf` dependency. React Native is pinned in `app/package-lock.json`; CocoaPods dependencies have their own lockfile. Install the specified simulator runtime explicitly before starting. No runtime downloads, personal simulator adoption, or global cache resets occur.

From the repository root, with `cargo`, `rustc`, `node`, `npm`, `pod`, `git`, `xcodebuild`, and `xcrun` on PATH:

```sh
cargo test --manifest-path validation/ios-isolation/Cargo.toml
cargo build --locked --manifest-path validation/ios-isolation/Cargo.toml
validation/ios-isolation/target/debug/ios-isolation prepare "$PWD" /tmp/devfleet-ios-proof
validation/ios-isolation/target/debug/ios-isolation run /tmp/devfleet-ios-proof
```

Commit fixture changes before `prepare`: it creates two **detached** worktrees from HEAD. This explicit setup installs dependencies separately and creates two owned iPhone 17 simulators. `run` consumes those existing worktrees and exact persisted UDIDs. Preparation refuses to overwrite an existing state directory. Keep the state directory after failure for logs; use a new directory for another preparation attempt.

Fixed loopback ports deliberately bound this experiment: A/B Metros 18081/18082, backends 19081/19082, identity-report listeners 19181/19182. Stop conflicting services before starting. The fixture fails instead of adopting an existing Metro. Builds are sequential with two Xcode jobs; Metro gets one worker each. Each worktree owns node_modules, Pods, generated native Swift, bundle identity, revision file and Metro FileStore cache. DerivedData directories are `<state>/A-build` and `<state>/B-build`.

## App integration and routing

`AppDelegate.swift.in` is a development-only hook. Rust generates ignored `AppDelegate.swift` inside each worktree. The Swift compiler embeds environment identity, build identity, an explicit Metro bundle URL, backend URL and report URL. A fresh launch nonce enters via `simctl launch` and becomes an initial React property. Release compilation fails deliberately.

The native bundle provider returns `http://127.0.0.1:<metro>/index.bundle?platform=ios&dev=true&minify=false` directly. There is no `RCTBundleURLProvider` fallback to 8081. The Metro host/port and native bundle URL are configured independently. The JS hook imports its own generated bundle identity, fetches its compiled backend URL, then reports native/bundle/backend/build identities to the compiled report listener. The listener compares these values with the expected environment and current launch nonce. This verifies application consumption, not merely port readiness.

All servers bind IPv4 loopback. The simulator-to-host route under test is `127.0.0.1`, with `NSAllowsLocalNetworking` enabled in the fixture Info.plist. This route is specific to the local iOS Simulator experiment; no claim is made about physical devices, Android, remote hosts, or production security. Nonces reject old launches; they are accidental-crosswiring controls, not authentication against malicious local processes.

## Scenarios and evidence

1. Build/install/launch A and B; both report `IDENTITY_VERIFIED`.
2. Edit A's JavaScript module `revision.js`, relaunch without building, observe `edited-A`; B still reports `initial`. A's native binary SHA-256 stays unchanged.
3. Change A's generated Swift backend endpoint to B, relaunch without rebuilding: the installed app still calls A with its old build identity and unchanged binary.
4. Rebuild/install A: its binary changes and the real backend response identifies B; require `IDENTITY_MISMATCH` with bundle A/backend B.
5. Rebuild A with Metro B and backend A: require `IDENTITY_MISMATCH` with bundle B/backend A.
6. Restore A's native configuration, rebuild, verify A and B again.

Each phase writes an app report, verdict, Unix timestamp, worktree HEAD, UDID, binary/source/revision/lockfile hashes and screenshot. `result.json` is invalidated before every run and records failure on error; only completion of all phases writes `passed: true`. Raw evidence, screenshots and build/Metro/install logs remain under the supplied state directory. Review `RESULTS.md` for the actual validation outcome; source code alone is not proof of acceptance.

## Boundaries

- The backend is a Rust HTTP identity endpoint for this experiment, not the future Docker Compose adapter or a fifteen-service application.
- The build ID hashes the generated native configuration/template. The fixture always invokes Xcode for native phases; it does not implement general native-cache invalidation across arbitrary projects/toolchains.
- The JS experiment uses a relaunch to request the edited bundle, not a claim about Fast Refresh timing.
- Xcode compilation and dependency installation can be long. Metro startup and app handshakes have deadlines; native commands currently depend on the operator if an external tool hangs.
- Metro child processes are stopped on normal return/error. Simulators, worktrees, native products and app data are retained for inspection. A hard process kill is not a production supervisor recovery scenario.
- Stop/delete only the exact fixture UDIDs listed in `environments.json` when finished. Remove fixture worktrees with `git worktree remove` only after preserving any desired evidence. No automatic deletion is performed.
