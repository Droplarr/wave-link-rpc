//! Typed building blocks for an unofficial Wave Link RPC SDK.
//!
//! Transport and revision-specific wire models are intentionally private. The
//! public API is expressed through normalized IDs, values, capabilities, and
//! errors so consumers do not depend on private JSON-RPC details.

mod capability;
mod discovery;
mod error;
mod id;
mod lifecycle;
mod snapshot;
mod transport;
mod value;

pub use capability::{Capabilities, ReadCapability, WriteCapability};
pub use discovery::{Discovery, Endpoint};
pub use error::{Error, ErrorKind, Result};
pub use id::{ChannelId, InputDeviceId, MixId, OutputDeviceId};
pub use lifecycle::{ConnectionState, Event, Subscription, SynchronizedClient};
pub use snapshot::{ApplicationInfo, Channel, InputDevice, MixerSnapshot, OutputDevice, OutputMix};
pub use transport::{Compatibility, WaveLinkClient};
pub use value::{Gain, MixBalance, MuteState, Volume};
