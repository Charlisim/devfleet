use clap::Parser;
use devfleet_protocol::{PROTOCOL_VERSION, SmokeResponse, SmokeStatus};

#[derive(Parser)]
#[command(about = "DevFleet per-environment supervisor")]
struct Args {
    /// Verify the executable boundary without creating or managing a process.
    #[arg(long)]
    smoke: bool,
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
    println!("DevFleet supervisor scaffold: process management is not available yet.");
    Ok(())
}
