use wave_link_rpc::{Capabilities, ChannelId, ReadCapability, Volume, WriteCapability};

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
