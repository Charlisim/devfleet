# DevFleet v0.1 release and installation

DevFleet v0.1 ships one macOS Apple Silicon archive containing the CLI, daemon,
supervisor, five official Rust provider binaries, provider descriptors, a
distribution plugin lock, checksums, the locked dependency graph and the tested
compatibility matrix.

Build an archive only on macOS arm64:

```sh
./scripts/package-macos-arm64.sh /tmp/devfleet-release
./scripts/test-clean-install.sh /tmp/devfleet-release/devfleet-v0.1.0-macos-arm64.tar.gz "$PWD"
```

The package script uses `Cargo.lock` and Rust 1.98.0. `SHA256SUMS` covers every
binary, descriptor and packaged document. The clean-install smoke verifies those
checksums and descriptors, creates two detached worktrees, starts two separate
process environments, reads each status and stops both.

## Installer, dependencies and trust

The archive includes `install-macos-arm64.sh`. It verifies every packaged file
before copying it to a versioned user prefix, installs Git, Node and CocoaPods
with Homebrew, detects an existing Docker-compatible CLI such as OrbStack, and
otherwise installs Docker Desktop with Homebrew. It registers a private
per-user launch agent, then installs and trusts all five checksum-verified
official providers. The default paths are `~/.local/share/devfleet/v0.1.0`,
`~/Library/Application Support/DevFleet`, and
`/tmp/df-$(id -u)/devfleet.sock`.

```sh
tar -xzf devfleet-v0.1.0-macos-arm64.tar.gz
cd devfleet-v0.1.0-macos-arm64
./install-macos-arm64.sh --doctor /absolute/path/to/git-worktree
```

The installer will not replace an unrelated binary, link, package prefix or
launch-agent configuration. Official providers are trusted by default only
through this checksum-verified package path. Third-party providers remain
untrusted and must be approved before execution; the current v0.1 client is
noninteractive, so that approval is the explicit `devfleet plugin trust <id>`
command. A future TUI confirmation must present the plugin ID, checksum,
descriptor and requested capabilities before sending that trust command. Use
`--no-start` or `--no-trust` to install files without starting the daemon or
authorizing official plugin execution. When Homebrew is absent, install it from
[brew.sh](https://brew.sh/) first, or pass `--install-homebrew` to run
Homebrew's official installer.

Homebrew cannot install the complete Xcode Simulator runtime. Install Xcode
from Apple, open it once, run `xcodebuild -runFirstLaunch`, and download the
required simulator runtime before creating iOS environments. Start OrbStack,
Docker Desktop, or another local Docker-compatible runtime before Compose use.
Run `pod --version` and `devfleet doctor` in each worktree before using iOS.

## Manual installation and trust

Unpack the archive, add `bin` to PATH, and start a private daemon state
directory. Install every provider path you intend to use, then trust each one
explicitly after inspecting its descriptor and checksum. Installation writes a
private `plugins.lock.json`; trust remains a separate decision. Plugin changes
are rejected while environments are active, and a checksum/descriptor mismatch
is rejected before execution.

```sh
tar -xzf devfleet-v0.1.0-macos-arm64.tar.gz
export PATH="$PWD/devfleet-v0.1.0-macos-arm64/bin:$PATH"
devfleetd --serve --state-dir /tmp/devfleet-state --socket /tmp/devfleet.sock \
  --supervisor "$(command -v devfleet-supervisor)" \
  --process-plugin "$(command -v devfleet-plugin-process)"
devfleet --socket /tmp/devfleet.sock plugin install "$(command -v devfleet-plugin-process)"
devfleet --socket /tmp/devfleet.sock plugin trust official.process
```

Use `init --trust --worktree <path>` to record project trust and propose a
manifest. `doctor` is read-only and reports missing trust, tools, pins,
compatibility and remediation. `plan` is provider-free; `up`, `status`, `list`,
`stop`, `destroy`, `operation`, `resource`, `logs` and `events` expose the
noninteractive JSON client contract described in the root README.

`stop` retains SQLite state, operation history, bounded logs and owned artifacts
for inspection. `destroy` requires explicit authorization and refuses
unverifiable or symlinked targets. A database newer than supported is rejected
without migration; upgrade from a compatible older schema runs the versioned
migrations. Back up the private state directory before changing release lines.

## Official providers and plugin authors

Official Rust providers are `official.process`, `official.compose`,
`official.metro`, `official.ios`, and `official.react-native`. They implement
the SDK's typed JSON-RPC boundary, declare capabilities, protocol version and
plugin state version. A plugin author must reject incompatible protocol/state
versions, accept cancellation through `lifecycle.cancel`, and make an uncertain
effect inspectable before retry. Plugins do not receive daemon storage or
plugin-to-plugin channels.

## Matrix and boundaries

The packaged matrix is in `share/devfleet/compatibility.json`: macOS 26.6.2,
Apple Silicon, Xcode 26.4.1, iOS Simulator 26.4.1, React Native 0.87.1, Node
26.7.0, CocoaPods 1.16.2 and Rust 1.98.0. It records tested versions; it does
not mean latest versions are supported.

Remote telemetry is disabled by default: v0.1 has no telemetry transport or
credential. v0.1 excludes Windows, distributed execution, physical devices, a
plugin marketplace, GUI workflows and untested Expo projects. The local iOS
fixture covers only the documented simulator matrix and loopback route.
