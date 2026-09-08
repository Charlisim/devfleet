use clap::Parser;
use devfleet_protocol::{PROTOCOL_VERSION, SmokeResponse, SmokeStatus};

#[derive(Parser)]
#[command(about = "DevFleet local daemon")]
struct Args {
    /// Verify daemon composition without creating a state directory or socket.
    #[arg(long)]
    smoke: bool,
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
    println!("DevFleet daemon scaffold: environment lifecycle is not available yet.");
    Ok(())
}
