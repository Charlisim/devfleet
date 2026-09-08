use clap::Parser;
use devfleet_daemon::{DaemonConfig, default_socket_dir, run};
use devfleet_protocol::{PROTOCOL_VERSION, SmokeResponse, SmokeStatus};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "DevFleet local daemon")]
struct Args {
    /// Verify daemon composition without creating a state directory or socket.
    #[arg(long)]
    smoke: bool,
    /// Run the private local control server.
    #[arg(long, requires_all = ["state_dir", "socket", "supervisor", "process_plugin"])]
    serve: bool,
    #[arg(long)]
    state_dir: Option<PathBuf>,
    #[arg(long)]
    socket: Option<PathBuf>,
    #[arg(long)]
    supervisor: Option<PathBuf>,
    #[arg(long)]
    process_plugin: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.smoke {
        let state = devfleet_state::StateStore::open_in_memory()?;
        let _schema_version = state.schema_version()?;
        println!(
            "{}",
            serde_json::to_string(&SmokeResponse {
                component: "daemon".into(),
                protocol: PROTOCOL_VERSION,
                status: SmokeStatus::Ready,
            })?
        );
        return Ok(());
    }
    if args.serve {
        return run(DaemonConfig {
            state_dir: args.state_dir.expect("clap requires state directory"),
            socket: args.socket.expect("clap requires socket"),
            supervisor: args.supervisor.expect("clap requires supervisor"),
            process_plugin: args.process_plugin.expect("clap requires process plugin"),
        });
    }
    eprintln!(
        "daemon requires --serve or --smoke; default private socket directory: {}",
        default_socket_dir().display()
    );
    Ok(())
}
