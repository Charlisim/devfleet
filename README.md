# DevFleet

DevFleet is a local macOS Apple Silicon orchestrator for isolated development
environments tied to existing Git worktrees. Its v0.1 target is multiple React
Native iOS worktrees, each with an independently verified backend, Metro server,
native build directory, simulator and evidence trail.

## Development

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
./scripts/check-boundaries.sh
```

The workspace uses Rust 1.98.0. `devfleet`, `devfleetd` and
`devfleet-supervisor` currently expose smoke-only entry points while the
supervised-process vertical slice is implemented. They do not yet claim to
manage an environment.

## Component boundaries

`devfleet-core` holds provider-neutral domain and planning types. It contains no
tool execution or Docker, Metro or Xcode integration. `devfleet-protocol` owns
the versioned serializable IPC/plugin boundary. `devfleet-state` owns SQLite
migrations and persistence mechanics. `devfleet-daemon` composes those layers;
`devfleet-supervisor` owns managed execution; and `devfleet-cli` is only a
control client. The SDK depends on the protocol, never on an official plugin;
official plugins depend on the SDK and may use the supervisor host boundary.

The enforced direct-dependency checks live in `scripts/check-boundaries.sh`.
Native plugins run with user permissions and are trusted code, not an OS sandbox.
