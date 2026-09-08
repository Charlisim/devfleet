use clap::Parser;
use devfleet_protocol::{PROTOCOL_VERSION, SmokeResponse, SmokeStatus};
use std::path::PathBuf;

#[derive(Parser)]
#[command(about = "DevFleet per-environment supervisor")]
struct Args {
    /// Verify the executable boundary without creating or managing a process.
    #[arg(long)]
    smoke: bool,
    /// Private Unix socket used only by the owning daemon.
    #[arg(long, requires_all = ["control_socket", "process_plugin"])]
    serve: bool,
    #[arg(long)]
    control_socket: Option<PathBuf>,
    #[arg(long)]
    process_plugin: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.smoke {
        println!(
            "{}",
            serde_json::to_string(&SmokeResponse {
                component: "supervisor".into(),
                protocol: PROTOCOL_VERSION,
                status: SmokeStatus::Ready,
            })?
        );
        return Ok(());
    }
    if args.serve {
        return devfleet_supervisor::serve(
            args.control_socket
                .as_deref()
                .expect("clap requires control socket"),
            args.process_plugin
                .as_deref()
                .expect("clap requires process plugin"),
        );
    }
    eprintln!("supervisor requires --serve or --smoke");
    Ok(())
}
