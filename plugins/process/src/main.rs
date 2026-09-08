use clap::Parser;
use devfleet_plugin_sdk::{Provider, ProviderDescriptor};
use devfleet_protocol::PROTOCOL_VERSION;
use semver::Version;

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
    #[arg(long)]
    smoke: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    if args.smoke {
        println!("{}", serde_json::to_string(&ProcessProvider.descriptor())?);
        return Ok(());
    }
    eprintln!("process plugin scaffold: protocol serving is not available yet");
    Ok(())
}
