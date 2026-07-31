use crate::{ChannelId, InputDeviceId, MixId, OutputDeviceId};
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
pub struct MixerSnapshot {
    pub application: ApplicationInfo,
    pub channels: Vec<Channel>,
    pub mixes: Vec<OutputMix>,
    pub input_devices: Vec<InputDevice>,
    pub output_devices: Vec<OutputDevice>,
}
