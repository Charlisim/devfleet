use clap::Parser;
use devfleet_plugin_sdk::{PluginError, Provider, ProviderDescriptor, serve_once};
use devfleet_protocol::{HostSpawnRequest, PROTOCOL_VERSION, PluginReconcileRequest};
use semver::Version;
use serde_json::Value;
use std::io::{self, BufReader};

struct ProcessProvider;

impl Provider for ProcessProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            id: "official.process".into(),
            version: Version::new(0, 1, 0),
            platform: current_platform(),
            protocol: PROTOCOL_VERSION,
            capabilities: vec![
                "lifecycle.initialize".into(),
                "lifecycle.validate".into(),
                "lifecycle.plan".into(),
                "lifecycle.inspect".into(),
                "process.reconcile".into(),
                "lifecycle.stop".into(),
                "lifecycle.destroy".into(),
                "lifecycle.action".into(),
                "lifecycle.cancel".into(),
            ],
            plugin_state_version: 1,
        }
    }

    fn reconcile(&self, request: PluginReconcileRequest) -> Result<Value, PluginError> {
        if !PROTOCOL_VERSION.compatible_with(request.protocol) {
            return Err(PluginError::IncompatibleProtocol);
        }
        if request.process.argv.is_empty() || request.process.cwd.is_empty() {
            return Err(PluginError::InvalidCall(
                "official.process requires argv and cwd".into(),
            ));
        }
        serde_json::to_value(HostSpawnRequest {
            argv: request.process.argv,
            cwd: request.process.cwd,
            environment: request.process.environment,
        })
        .map_err(|error| PluginError::RequestFailed(error.to_string()))
    }
}

#[derive(Parser)]
#[command(about = "DevFleet official local-process provider")]
struct Args {
    /// Print the versioned descriptor without accepting a protocol request.
    #[arg(long)]
    descriptor: bool,
    /// Compatibility alias for the executable smoke check.
    #[arg(long)]
    smoke: bool,
    /// Serve exactly one bounded SDK JSON-RPC request over stdin/stdout.
    #[arg(long)]
    serve: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.descriptor || args.smoke {
        println!("{}", serde_json::to_string(&ProcessProvider.descriptor())?);
        return Ok(());
    }
    if args.serve {
        let stdin = io::stdin();
        let mut reader = BufReader::new(stdin.lock());
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        return Ok(serve_once(&ProcessProvider, &mut reader, &mut writer)?);
    }
    anyhow::bail!("process plugin requires --descriptor, --serve, or --smoke")
}

fn current_platform() -> String {
    format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use devfleet_protocol::{ProcessSpec, ProtocolVersion};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn rejects_empty_argv_before_requesting_a_host_spawn() {
        let request = PluginReconcileRequest {
            protocol: ProtocolVersion { major: 1, minor: 0 },
            operation_id: Uuid::now_v7(),
            process: ProcessSpec {
                argv: Vec::new(),
                cwd: "/tmp".into(),
                environment: BTreeMap::new(),
            },
        };
        assert!(ProcessProvider.reconcile(request).is_err());
    }
}
