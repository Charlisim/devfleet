use clap::Parser;
use devfleet_protocol::{PROTOCOL_VERSION, SmokeResponse, SmokeStatus};

#[derive(Parser)]
#[command(about = "DevFleet control client")]
struct Args {
    /// Verify the executable boundary without starting an environment.
    #[arg(long)]
    smoke: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.smoke {
        println!(
            "{}",
            serde_json::to_string(&SmokeResponse {
                component: "cli".into(),
                protocol: PROTOCOL_VERSION,
                status: SmokeStatus::Ready,
            })?
        );
        return Ok(());
    }
    println!("DevFleet CLI scaffold: environment lifecycle is not available yet.");
    Ok(())
}
