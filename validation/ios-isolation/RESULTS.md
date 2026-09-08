# Measured validation results

**Passed: all nine app checks**, 2026-09-07 (America/Guatemala). Rust suite: 7/7. TypeScript and Rust formatting/typechecking passed. Standards and spec reviews: no remaining actionable findings.

The final experiment changed an actual JavaScript module, `revision.js`, in A. It used app relaunches to request bundles; Fast Refresh timing was not measured.

## Tested matrix

- macOS 26.6.2 (25G83), arm64.
- Xcode 26.4.1 (17E202).
- iOS Simulator runtime identifier `com.apple.CoreSimulator.SimRuntime.iOS-26-4`, version **26.4.1**, build **23E254a**.
- React Native 0.87.1, Metro 0.87.0, Community CLI 20.2.0.
- Node v26.7.0, Ruby 3.3.5, CocoaPods 1.16.2; rustc 1.98.0 (88d9e12ae 2026-08-18) (Homebrew).
- Final run: **40 seconds**, with dependencies, booted simulators and existing DerivedData already present. This is a warm rerun, not a cold-start benchmark.

## Observations

| Environment | Phase | Native / bundle / backend | Revision | Verdict |
|---|---|---|---|---|
| A | initial | A / A / A | initial | IDENTITY_VERIFIED |
| B | initial | B / B / B | initial | IDENTITY_VERIFIED |
| A | js-edit | A / A / A | edited-A | IDENTITY_VERIFIED |
| B | js-unchanged | B / B / B | initial | IDENTITY_VERIFIED |
| A | endpoint-without-build | A / A / A | edited-A | IDENTITY_VERIFIED |
| A | cross-backend | A / A / B | edited-A | IDENTITY_MISMATCH |
| A | cross-metro | A / B / A | initial | IDENTITY_MISMATCH |
| A | restored | A / A / A | edited-A | IDENTITY_VERIFIED |
| B | final-unchanged | B / B / B | initial | IDENTITY_VERIFIED |

A's original, JS-edit and endpoint-without-build native binary SHA-256 remained `3a60feb6c146c82d205712cc63b2a4b618d452cf9f1bc4c060e6b213697dd282`. Compiling the B backend endpoint changed it to `cc6f62798fba3a8fc6df89ebc294a2101d5337113d77494677443ab7c29d4d42`. B's binary stayed `8bb00cf14fd9e0c4ab14464482063ba2e646ed057305efc978eea9a8a2952c36` throughout.

These reports came from the running apps after real HTTP requests. The Metro cross-wire reported bundle B/backend A; the backend cross-wire reported bundle A/backend B. Both were rejected as `IDENTITY_MISMATCH`. Restoring A returned both apps to `IDENTITY_VERIFIED`.

## Reproducibility and artifacts

- Orchestrator commit: `5d4a60654a2de7201baff91d27103eafc413fe55`. Its source and executable SHA-256 are in [metadata.json](evidence/metadata.json).
- Both worktree HEADs: `7ebfd5d70189af88e9a7b73af5adbbb6d65ae06d`. The different SHA reflects cherry-picking fixture corrections into the detached worktrees; the tested app content matches the orchestrator commit.
- [App evidence](evidence/evidence.json), [run result](evidence/result.json), [resource mapping](evidence/environments.json).
- [A after JS edit](evidence/A-js-edit.png), [B unchanged](evidence/B-js-unchanged.png), [backend mismatch](evidence/A-cross-backend.png), [Metro mismatch](evidence/A-cross-metro.png).
- Full local logs, products and worktrees retained at `/private/tmp/devfleet-ios-proof-2`. Screenshots and JSON evidence are copied here for durable review.

## Measured limitations and corrections

Initial Metro configuration disabled hierarchical lookup without declaring `nodeModulesPaths`; native build succeeded but JS failed to resolve Babel runtime. Setting the worktree's own node_modules path fixed it. The failed run correctly invalidated the previous result; the final complete rerun above succeeded.

The host's Homebrew CocoaPods entry point failed with a missing `nkf` dependency. RVM Ruby 3.3.5/CocoaPods 1.16.2 completed preparation. A partially prepared, unbooted fixture simulator/worktree remains recorded in `/private/tmp/devfleet-ios-proof-1/environments.json`; it was not adopted into the successful experiment.

Loopback routing was verified only on this local iOS Simulator matrix. The backend is an identity fixture, not Docker Compose. Native builds are explicit; this does not validate a general native-cache fingerprint. Readiness here is scoped to app identity, not production process supervision or arbitrary monorepos. Some screenshots retain React Native's transient download overlay; the report and rendered identity remain visible.

Metro/backend listeners stop when the experiment exits. The two dedicated simulators and installed apps are retained; they may show connection errors after services stop. Personal simulators were not modified. No remote push or issue update was performed.
