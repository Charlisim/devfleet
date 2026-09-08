use clap::Parser;
use devfleet_plugin_sdk::{Provider, ProviderDescriptor};
use devfleet_protocol::{
    ErrorCode, HostSpawnRequest, JsonRpcRequest, JsonRpcResponse, PROTOCOL_VERSION,
    PluginReconcileRequest, RpcFailure, read_json_line, write_json_line,
};
use semver::Version;
use std::io::{self, BufReader, Write};

struct ProcessProvider;

impl Provider for ProcessProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            id: "official.process".into(),
            version: Version::new(0, 1, 0),
            protocol: PROTOCOL_VERSION,
            capabilities: vec!["process.reconcile".into()],
            plugin_state_version: 1,
        }
    }
}

#[derive(Parser)]
#[command(about = "DevFleet official local-process provider")]
struct Args {
    /// Print a descriptor without accepting a protocol request.
    #[arg(long)]
    smoke: bool,
    /// Serve exactly one bounded JSON-RPC request over stdin/stdout.
    #[arg(long)]
    serve: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.smoke {
        println!("{}", serde_json::to_string(&ProcessProvider.descriptor())?);
        return Ok(());
    }
    if args.serve {
        return serve();
    }
    eprintln!("process plugin requires --serve or --smoke");
    Ok(())
}

fn serve() -> anyhow::Result<()> {
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let request = read_json_line::<JsonRpcRequest>(&mut reader)?;
    let response = match request.method.as_str() {
        "initialize" => JsonRpcResponse::success(request.id, &ProcessProvider.descriptor()),
        "reconcile" => reconcile_response(&request),
        _ => JsonRpcResponse::failure(
            request.id,
            RpcFailure {
                code: ErrorCode::ProtocolInvalid,
                message: "unsupported official.process method".into(),
                retryable: false,
            },
        ),
    };
    let mut stdout = io::stdout().lock();
    stdout.write_all(&write_json_line(&response)?)?;
    stdout.flush()?;
    Ok(())
}

fn reconcile_response(request: &JsonRpcRequest) -> JsonRpcResponse {
    let rpc_id = request.id;
    let parsed = serde_json::from_value::<PluginReconcileRequest>(request.params.clone());
    let Ok(request) = parsed else {
        return JsonRpcResponse::failure(
            rpc_id,
            RpcFailure {
                code: ErrorCode::ConfigInvalid,
                message: "official.process reconcile parameters are invalid".into(),
                retryable: false,
            },
        );
    };
    if !PROTOCOL_VERSION.compatible_with(request.protocol) {
        return JsonRpcResponse::failure(
            rpc_id,
            RpcFailure {
                code: ErrorCode::ProtocolIncompatible,
                message: "official.process protocol major version is incompatible".into(),
                retryable: false,
            },
        );
    }
    if request.process.argv.is_empty() || request.process.cwd.is_empty() {
        return JsonRpcResponse::failure(
            rpc_id,
            RpcFailure {
                code: ErrorCode::ConfigInvalid,
                message: "official.process requires argv and cwd".into(),
                retryable: false,
            },
        );
    }
    JsonRpcResponse::success(
        rpc_id,
        &HostSpawnRequest {
            argv: request.process.argv,
            cwd: request.process.cwd,
            environment: request.process.environment,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use devfleet_protocol::{ProcessSpec, ProtocolVersion};
    use serde_json::json;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    #[test]
    fn rejects_empty_argv_before_requesting_a_spawn() {
        let operation_id = Uuid::now_v7();
        let request = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: operation_id,
            method: "reconcile".into(),
            params: json!(PluginReconcileRequest {
                protocol: ProtocolVersion { major: 1, minor: 0 },
                operation_id,
                process: ProcessSpec {
                    argv: Vec::new(),
                    cwd: "/tmp".into(),
                    environment: BTreeMap::new(),
                },
            }),
        };
        let response = reconcile_response(&request);
        assert_eq!(
            response.error.expect("failure").code,
            ErrorCode::ConfigInvalid
        );
    }
}
