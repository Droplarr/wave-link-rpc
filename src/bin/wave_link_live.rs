use std::process::ExitCode;
use wave_link_rpc::{Discovery, WaveLinkClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "live validation failed: {} ({})",
                error.kind(),
                error.context()
            );
            ExitCode::FAILURE
        }
    }
}

async fn run() -> wave_link_rpc::Result<()> {
    let mode = std::env::var("WAVE_LINK_LIVE_MODE").unwrap_or_else(|_| "read".to_owned());
    if mode != "read" {
        return Err(wave_link_rpc::Error::new(
            wave_link_rpc::ErrorKind::CapabilityUnavailable,
            "write mode is not implemented in the read-only validation harness",
        ));
    }

    let endpoint = Discovery::msix_default()?.discover().await?;
    let client = WaveLinkClient::connect(&endpoint).await?;
    let snapshot = client.snapshot().await?;
    println!(
        "interface_revision={}",
        snapshot.application.interface_revision
    );
    println!("compatibility={:?}", client.compatibility());
    println!("channels={}", snapshot.channels.len());
    println!("mixes={}", snapshot.mixes.len());
    println!("input_devices={}", snapshot.input_devices.len());
    println!("output_devices={}", snapshot.output_devices.len());
    client.close().await?;
    Ok(())
}
