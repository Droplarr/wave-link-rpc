use std::time::Duration;
use wave_link_rpc::{
    BatchResult, Capabilities, Channel, ChannelId, ChannelMixState, ConnectionState, Discovery,
    FadeCurve, FadeOptions, MixId, Operation, ReadCapability, Result, SynchronizedClient, Volume,
    WaveLinkClient, WriteCapability,
};

#[test]
fn public_models_round_trip_through_serde() {
    let id = ChannelId::new("redacted-channel-1");
    let json = serde_json::to_string(&id).expect("serialize ID");
    assert_eq!(json, "\"redacted-channel-1\"");
    assert_eq!(
        serde_json::from_str::<ChannelId>(&json).expect("deserialize ID"),
        id
    );

    let volume = Volume::new(0.75).expect("valid volume");
    let json = serde_json::to_string(&volume).expect("serialize volume");
    assert_eq!(
        serde_json::from_str::<Volume>(&json).expect("deserialize volume"),
        volume
    );
}

#[test]
fn read_and_write_capabilities_are_independent() {
    let capabilities = Capabilities::new([ReadCapability::Channels], []);
    assert!(capabilities.can_read(ReadCapability::Channels));
    assert!(!capabilities.can_write(WriteCapability::Volume));
}

#[test]
#[allow(clippy::no_effect_underscore_binding)]
fn version_0_1_0_public_entry_points_remain_source_compatible() {
    let _connect = WaveLinkClient::connect;
    let _spawn: fn(Discovery) -> SynchronizedClient = SynchronizedClient::spawn;
    let _state: fn(&SynchronizedClient) -> ConnectionState = SynchronizedClient::state;
    let _snapshot = SynchronizedClient::snapshot;
    let _subscribe = SynchronizedClient::subscribe;
    let _ready = SynchronizedClient::ready;
    let _shutdown = SynchronizedClient::shutdown;
    let _low_level_apply = WaveLinkClient::apply;
    let _low_level_batch = WaveLinkClient::apply_batch;
    let _low_level_fade = WaveLinkClient::fade_channel_volume;

    let operation = Operation::SetChannelMixVolume {
        channel: ChannelId::new("channel-1"),
        mix: MixId::new("mix-1"),
        volume: Volume::new(0.5).expect("volume"),
    };
    let options = FadeOptions {
        duration: Duration::ZERO,
        curve: FadeCurve::Perceptual,
    };
    let _: (Operation, FadeOptions, Option<BatchResult>) = (operation, options, None);
    let _: Option<Result<()>> = None;
}

#[test]
fn new_normalized_models_are_public() {
    let _: Option<Channel> = None;
    let _: Option<ChannelMixState> = None;
}
