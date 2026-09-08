# DevFleet

DevFleet is a local macOS Apple Silicon orchestrator for isolated development
environments tied to existing Git worktrees. Its v0.1 target is multiple React
Native iOS worktrees, each with an independently verified backend, Metro server,
native build directory, simulator and evidence trail.

## Development

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo build --workspace
cargo test --workspace
./scripts/check-boundaries.sh
```

The workspace uses Rust 1.98.0. `devfleet`, `devfleetd` and
`devfleet-supervisor` now support the first local-process vertical slice. The
remaining v0.1 providers and lifecycle operations are still being implemented.

## Supervised local-process fixture

Start the daemon with explicit release-installed supervisor and process-plugin
paths. The state directory and socket parent must be private user-owned
directories; the socket must be short enough for Unix-domain socket limits.

```sh
devfleetd --serve \
  --state-dir /tmp/devfleet-state \
  --socket /tmp/df-$(id -u)/daemon.sock \
  --supervisor "$(dirname "$(command -v devfleetd)")/devfleet-supervisor" \
  --process-plugin "$(dirname "$(command -v devfleetd)")/devfleet-plugin-process"
```

Register the native provider and make a separate, explicit trust decision
before `up`. Installation records its descriptor and checksum in a private
lockfile; it does not authorize execution. Native plugins run with user
permissions and are trusted code, not an OS sandbox.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin install \
  "$(dirname "$(command -v devfleetd)")/devfleet-plugin-process"
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin trust official.process
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin list
```

In another terminal, execute an explicitly trusted fixture with a fully
specified program, working directory and environment. The command must follow
`--`; it is passed as argv and never interpolated by a shell.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock --json up \
  --trust --worktree "$PWD" --cwd "$PWD" \
  -- /bin/sleep 60
```

Use `status`, `list` and `stop <environment-id>` with the same socket. The
supervisor clears inherited process environment, owns a new process group and
uses SIGINT, SIGTERM and SIGKILL only for that owned group. `stop` retains the
environment, operation and process identity in SQLite. Local peer UID checks,
0600 sockets, a 0700 state directory, a single writer lock and 1 MiB JSON-RPC
frames protect this bounded slice. Plugins are trusted native code; these
controls are not an OS sandbox.

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
