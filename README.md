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
`devfleet-supervisor` expose the v0.1 noninteractive control slice through a
private Unix socket.

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

The checksum-verified release installer trusts bundled official providers by
default. For a manual provider installation, register it and make a separate,
explicit trust decision before `up`. Installation records its descriptor and
checksum in a private lockfile; it does not authorize execution. Native plugins
run with user permissions and are trusted code, not an OS sandbox.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin install \
  "$(dirname "$(command -v devfleetd)")/devfleet-plugin-process"
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin trust official.process
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin list
```

The Compose provider is installed and trusted separately. It only accepts a
local Docker Unix-socket context in v0.1, so a remote context returns an
actionable rejection instead of a misleading localhost endpoint.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin install \
  "$(dirname "$(command -v devfleetd)")/devfleet-plugin-compose"
devfleet --socket /tmp/df-$(id -u)/daemon.sock plugin trust official.compose
devfleet --socket /tmp/df-$(id -u)/daemon.sock compose up \
  --trust --worktree "$PWD" --file "$PWD/compose.yaml" \
  --endpoint api=api:3000:0:/health
```

Use `HOST_PORT=0` to allocate a retained automatic port from the host lease.
DevFleet injects the resolved value as `DEVFLEET_<ENDPOINT>_PORT`; the example
endpoint therefore uses `${DEVFLEET_API_PORT}` in `compose.yaml`. An explicit
host port remains available, but an explicitly supplied managed variable must
match DevFleet's resolved value.

## Project setup and diagnosis

`init` discovers an existing Git worktree, records the explicit project trust
decision in private state, and prints a proposed `devfleet.toml` plus required
integrations. It never writes tracked configuration or converts a monorepo.
Pass `--runtime` and `--device-type` together to pin an installed local iOS
selection after the trusted iOS plugin has validated it.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock init \
  --trust --worktree "$PWD" \
  --runtime com.apple.CoreSimulator.SimRuntime.iOS-18-0 \
  --device-type com.apple.CoreSimulator.SimDeviceType.iPhone-16
```

Copy the proposed manifest only after review. `doctor` performs read-only
checks for project trust, manifest presence, plugin trust/compatibility, local
Docker context, Node/React Native/Xcode tooling, pinned iOS selections, and
optional environment bindings/preparation. Its human and `--json` output
contains phase, resource, cause, retryability, and remediation for every
blocking finding.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock doctor --worktree "$PWD"
devfleet --socket /tmp/df-$(id -u)/daemon.sock --json doctor \
  --worktree "$PWD" --environment-id <environment-id>
```

## Ordered initialization

Run declared backend migrations and seeds through the environment's trusted
supervisor. The JSON file is an array of `InitializationTask` values; each task
has an `id`, `kind` (`migration` or `seed`), `depends_on`, `rerun_policy`
(`never`, `always`, or `on_environment_generation`), and a `process` with
literal `argv`, `cwd`, and explicit environment values.

```sh
devfleet initialize run <environment-id> --file initialization.json
```

Completed `never` tasks are retained and skipped on retry. A task left running
when the daemon loses its reply is shown as uncertain and is never retried
automatically. No inverse migration or data deletion is performed.

## Phase bindings

Apply an explicit `BindingApplyRequest` JSON document with `devfleet bindings
apply --file bindings.json`. It selects one matching phase/integration pair:
`process`/`process_environment`, `bundle`/`bundle_config`,
`native_build`/`native_build_settings`, or `runtime`/`runtime_configuration`.
Public keys use the `APP_PUBLIC_` prefix. Reserved endpoints apply last and
cannot be overridden. The daemon stores generated values in a private
per-environment artifact and exposes only public values, fingerprints, and the
restart/reload/rebuild/reconnect decision through status.

Before startup, the provider asks Docker for the effective merged JSON model.
It requires project-scoped names, loopback-only endpoint publications, and
private mutable storage. Built services need a project-specific image tag or
immutable digest. Flags such as `--allow-external-resources` require a
nonempty explanation. `status` reports internal, host, and device
endpoint semantics; health is an actual bounded HTTP probe of the declared
readiness path.

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

## Noninteractive client workflow

The public methods are `environment.plan`, `environment.up`,
`environment.status`, `environment.list`, `environment.stop`,
`environment.destroy`, `operation.get`, `operation.cancel`, `resource.action`,
`logs.tail`, and `events.subscribe`. The CLI's `--json` output is the matching
typed response envelope. `up --detach --json` returns a durable accepted
operation ID; acceptance is separate from a terminal operation state.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock --json up \
  --detach --trust --worktree "$PWD" --cwd "$PWD" -- /bin/sleep 60
devfleet --socket /tmp/df-$(id -u)/daemon.sock operation wait <operation-id> \
  --until accepted
devfleet --socket /tmp/df-$(id -u)/daemon.sock --json resource exec <environment-id> \
  --cwd "$PWD" --env CLIENT_MODE=noninteractive -- /usr/bin/env
devfleet --socket /tmp/df-$(id -u)/daemon.sock resource open <environment-id>
```

Scoped exec clears inherited variables, requires an existing cwd inside the
environment worktree, runs in an owned process group, and returns bounded
stdout/stderr through `logs <environment-id> --resource exec-<operation-id>`
after its retained operation reaches a terminal state. An unsupported resource
action returns `CAPABILITY_UNSUPPORTED`. The runnable
[`examples/noninteractive-client.sh`](examples/noninteractive-client.sh)
performs status recovery before following durable, deduplicated events.

## Events and logs

`events` uses the public `events.subscribe` contract. It reads a bounded batch,
can resume with `--after <sequence>`, and `--follow` repeats those bounded
reads without holding the daemon control connection open. Consumers must
deduplicate by event ID; the CLI keeps a bounded recent-ID window. Each
environment retains its most recent 2,048 events. An older cursor returns
`CURSOR_EXPIRED`; recover with `status`, then request events without `--after`.

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock --json events <environment-id> --after 42
devfleet --socket /tmp/df-$(id -u)/daemon.sock events <environment-id> --follow
devfleet --socket /tmp/df-$(id -u)/daemon.sock logs <environment-id> --resource process --follow
```

The supervisor writes process stdout and stderr through a bounded queue to a
private per-environment log directory. Each resource segment rotates at 1 MiB;
large payloads never enter SQLite event rows. Explicit `--env` values whose
names identify secrets are redacted before writing a complete log line. This
does not promise redaction of arbitrary transformed tool output; do not emit
secrets from a process.

## Declarative preview

`plan` reads a schema-versioned `devfleet.toml`, optionally applies a local
override and explicit operation values, then validates the graph without
contacting a provider or creating a child process. Provider descriptors must be
installed, but planning does not require plugin execution trust. Secret values
are never accepted by this command: use only `--secret-ref NAME` to prove that
a required reference can be supplied later.

```toml
schema_version = 1

[ports]
api = 4010

[profiles.process]
resources = ["api"]

[resources.api]
provider = "official.process"

[resources.api.config]
port = "${{ ports.api }}"
token = "${{ secrets.API_TOKEN }}"

[resources.api.config_schema]
required = ["port", "token"]

[resources.api.config_schema.types]
port = "integer"
```

```sh
devfleet --socket /tmp/df-$(id -u)/daemon.sock --json plan \
  --worktree "$PWD" --profile process --secret-ref API_TOKEN
```

An entire expression retains its resolved JSON type; an expression embedded in
text is rendered as a string. `depends_on`, `readiness_depends_on`, runtime
output expressions (`resources.<id>.outputs.<name>`) and reserved endpoint
expressions (`endpoints.<id>.<name>`) appear as distinct dependency edges. The
daemon stores only the redacted effective configuration and advances its plan
generation when that snapshot changes.

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
