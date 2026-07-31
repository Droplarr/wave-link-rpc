use crate::{ChannelId, Error, ErrorKind, InputDeviceId, MixId, OutputDeviceId, Result, Volume};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationInfo {
    #[serde(default)]
    pub app_id: Option<String>,
    #[serde(default)]
    pub application_version: Option<String>,
    #[serde(default)]
    pub build: Option<Value>,
    pub interface_revision: u32,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

macro_rules! entity {
    ($name:ident, $id:ty) => {
        #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
            pub id: $id,
            #[serde(default)]
            pub name: Option<String>,
            #[serde(flatten)]
            pub state: BTreeMap<String, Value>,
        }
    };
}

entity!(Channel, ChannelId);
entity!(OutputMix, MixId);
entity!(InputDevice, InputDeviceId);
entity!(OutputDevice, OutputDeviceId);

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelMixState {
    pub id: MixId,
    #[serde(default, rename = "level")]
    pub volume: Option<Volume>,
    #[serde(default, rename = "isMuted")]
    pub muted: Option<bool>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl Channel {
    /// Returns the normalized channel volume when Wave Link reported one.
    ///
    /// # Errors
    ///
    /// Returns a protocol error when `level` is present but is not a valid
    /// normalized volume.
    pub fn volume(&self) -> Result<Option<Volume>> {
        optional_volume(&self.state, "level")
    }

    #[must_use]
    pub fn muted(&self) -> Option<bool> {
        self.state.get("isMuted").and_then(Value::as_bool)
    }

    /// Returns the participating output mixes with normalized typed state.
    ///
    /// # Errors
    ///
    /// Returns a protocol error when the `mixes` field is present but malformed.
    pub fn participating_mixes(&self) -> Result<Vec<ChannelMixState>> {
        match self.state.get("mixes") {
            None | Some(Value::Null) => Ok(Vec::new()),
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string())),
        }
    }

    /// Looks up one participating mix by its opaque identifier.
    ///
    /// # Errors
    ///
    /// Returns a protocol error when the channel's mix state is malformed.
    pub fn mix(&self, id: &MixId) -> Result<Option<ChannelMixState>> {
        Ok(self
            .participating_mixes()?
            .into_iter()
            .find(|mix| &mix.id == id))
    }
}

fn optional_volume(state: &BTreeMap<String, Value>, key: &str) -> Result<Option<Volume>> {
    let Some(value) = state.get(key) else {
        return Ok(None);
    };
    serde_json::from_value::<Volume>(value.clone())
        .map(Some)
        .map_err(|_| {
            Error::new(
                ErrorKind::Protocol,
                format!("{key} must be a number between 0.0 and 1.0"),
            )
        })
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MixerSnapshot {
    pub application: ApplicationInfo,
    pub channels: Vec<Channel>,
    pub mixes: Vec<OutputMix>,
    pub input_devices: Vec<InputDevice>,
    pub output_devices: Vec<OutputDevice>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn channel_accessors_normalize_volume_mute_and_mixes() {
        let channel: Channel = serde_json::from_value(json!({
            "id": "channel-1",
            "name": "Synthetic",
            "level": 0.75,
            "isMuted": false,
            "mixes": [{
                "id": "mix-1",
                "level": 0.25,
                "isMuted": true,
                "futureField": 1
            }]
        }))
        .expect("channel");

        assert_eq!(
            channel.volume().expect("volume"),
            Some(Volume::new(0.75).unwrap())
        );
        assert_eq!(channel.muted(), Some(false));
        let mixes = channel.participating_mixes().expect("mixes");
        assert_eq!(mixes.len(), 1);
        assert_eq!(mixes[0].volume, Some(Volume::new(0.25).unwrap()));
        assert_eq!(mixes[0].muted, Some(true));
        assert!(mixes[0].extra.contains_key("futureField"));
    }

    #[test]
    fn malformed_normalized_state_is_a_protocol_error() {
        let channel: Channel = serde_json::from_value(json!({
            "id": "channel-1",
            "level": 2.0
        }))
        .expect("channel");
        assert_eq!(channel.volume().unwrap_err().kind(), ErrorKind::Protocol);
    }
}
