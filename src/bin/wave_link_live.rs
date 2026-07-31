use std::process::ExitCode;
use std::time::Duration;
use wave_link_rpc::{
    ChannelId, Discovery, FadeCurve, FadeOptions, Operation, Volume, WaveLinkClient,
};

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
    if mode != "read" && mode != "bounded-write" {
        return Err(wave_link_rpc::Error::new(
            wave_link_rpc::ErrorKind::CapabilityUnavailable,
            "mode must be read or bounded-write",
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
    if mode == "bounded-write" {
        run_bounded_writes(&client, &snapshot).await?;
    }
    client.close().await?;
    Ok(())
}

async fn run_bounded_writes(
    client: &WaveLinkClient,
    snapshot: &wave_link_rpc::MixerSnapshot,
) -> wave_link_rpc::Result<()> {
    let channel = snapshot.channels.first().ok_or_else(|| {
        wave_link_rpc::Error::new(
            wave_link_rpc::ErrorKind::CapabilityUnavailable,
            "bounded write validation requires one channel",
        )
    })?;
    let mix = snapshot.mixes.first().ok_or_else(|| {
        wave_link_rpc::Error::new(
            wave_link_rpc::ErrorKind::CapabilityUnavailable,
            "bounded write validation requires one mix",
        )
    })?;
    let channel_level = number(&channel.state, "level")?;
    let channel_mute = boolean(&channel.state, "isMuted")?;
    let mix_level = number(&mix.state, "level")?;
    let mix_mute = boolean(&mix.state, "isMuted")?;
    let channel_target = bounded_target(channel_level)?;
    let mix_target = bounded_target(mix_level)?;

    mutate_and_restore(
        client,
        Operation::SetChannelVolume {
            channel: channel.id.clone(),
            volume: channel_target,
        },
        Operation::SetChannelVolume {
            channel: channel.id.clone(),
            volume: Volume::new(channel_level)?,
        },
    )
    .await?;
    mutate_and_restore(
        client,
        Operation::SetChannelMute {
            channel: channel.id.clone(),
            muted: !channel_mute,
        },
        Operation::SetChannelMute {
            channel: channel.id.clone(),
            muted: channel_mute,
        },
    )
    .await?;
    mutate_and_restore(
        client,
        Operation::SetMixVolume {
            mix: mix.id.clone(),
            volume: mix_target,
        },
        Operation::SetMixVolume {
            mix: mix.id.clone(),
            volume: Volume::new(mix_level)?,
        },
    )
    .await?;
    mutate_and_restore(
        client,
        Operation::SetMixMute {
            mix: mix.id.clone(),
            muted: !mix_mute,
        },
        Operation::SetMixMute {
            mix: mix.id.clone(),
            muted: mix_mute,
        },
    )
    .await?;

    client
        .fade_channel_volume(
            channel.id.clone(),
            Volume::new(channel_level)?,
            channel_target,
            FadeOptions {
                duration: Duration::from_millis(250),
                curve: FadeCurve::Perceptual,
            },
        )
        .await?;
    client
        .apply(&Operation::SetChannelVolume {
            channel: channel.id.clone(),
            volume: Volume::new(channel_level)?,
        })
        .await?;
    let restored = client.snapshot().await?;
    verify_channel_level(&restored, &channel.id, channel_level)?;
    println!("bounded_write_validation=passed");
    Ok(())
}

async fn mutate_and_restore(
    client: &WaveLinkClient,
    mutation: Operation,
    restoration: Operation,
) -> wave_link_rpc::Result<()> {
    if let Err(error) = client.apply(&mutation).await {
        let _ = client.apply(&restoration).await;
        return Err(error);
    }
    client.apply(&restoration).await?;
    let _ = client.snapshot().await?;
    Ok(())
}

fn bounded_target(current: f32) -> wave_link_rpc::Result<Volume> {
    let target = if current >= 0.02 {
        current - 0.02
    } else {
        (current + 0.02).min(1.0)
    };
    Volume::new(target)
}

#[allow(clippy::cast_possible_truncation)]
fn number(
    state: &std::collections::BTreeMap<String, serde_json::Value>,
    key: &str,
) -> wave_link_rpc::Result<f32> {
    state
        .get(key)
        .and_then(serde_json::Value::as_f64)
        .map(|value| value as f32)
        .ok_or_else(|| {
            wave_link_rpc::Error::new(
                wave_link_rpc::ErrorKind::CapabilityUnavailable,
                format!("live target omitted numeric {key}"),
            )
        })
}

fn boolean(
    state: &std::collections::BTreeMap<String, serde_json::Value>,
    key: &str,
) -> wave_link_rpc::Result<bool> {
    state
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| {
            wave_link_rpc::Error::new(
                wave_link_rpc::ErrorKind::CapabilityUnavailable,
                format!("live target omitted boolean {key}"),
            )
        })
}

fn verify_channel_level(
    snapshot: &wave_link_rpc::MixerSnapshot,
    channel_id: &ChannelId,
    expected: f32,
) -> wave_link_rpc::Result<()> {
    let channel = snapshot
        .channels
        .iter()
        .find(|channel| &channel.id == channel_id)
        .ok_or_else(|| {
            wave_link_rpc::Error::new(
                wave_link_rpc::ErrorKind::Protocol,
                "restored channel disappeared from snapshot",
            )
        })?;
    let actual = number(&channel.state, "level")?;
    if (actual - expected).abs() > 0.001 {
        return Err(wave_link_rpc::Error::new(
            wave_link_rpc::ErrorKind::Protocol,
            "channel level restoration did not round trip",
        ));
    }
    Ok(())
}
