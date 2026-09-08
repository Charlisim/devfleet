# v0.1 component boundaries

| Component | Owns | Must not own |
| --- | --- | --- |
| `devfleet-protocol` | Versioned, serializable boundary DTOs and protocol errors | SQLite, process execution, provider implementations |
| `devfleet-core` | Provider-neutral identity, plans, graph and lifecycle concepts | Tool commands, database access, official plugins |
| `devfleet-state` | SQLite schema migrations, transactions and persisted records | Planning policy, external process control |
| `devfleet-plugin-sdk` | Provider trait, frame validation and supervised-host requests | Daemon storage, official plugin implementations |
| `devfleet-daemon` | Orchestration, state composition, global allocations and IPC | Provider-specific tool execution |
| `devfleet-supervisor` | Per-environment process ownership, signals and logs | Global resource allocation, SQLite writes |
| `devfleet-cli` | Checkout context detection and control-client presentation | Lifecycle ownership and provider execution |
| `plugins/*` | A declared external tool integration | Direct daemon database access or plugin-to-plugin calls |

The CI boundary check rejects forbidden workspace dependencies. The table is the
authoritative rule for responsibilities not visible in a Cargo dependency graph.
