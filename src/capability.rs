use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[non_exhaustive]
pub enum ReadCapability {
    Application,
    Channels,
    Mixes,
    InputDevices,
    OutputDevices,
    Notifications,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[non_exhaustive]
pub enum WriteCapability {
    Volume,
    Mute,
    PerMixState,
    Routing,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Capabilities {
    reads: BTreeSet<ReadCapability>,
    writes: BTreeSet<WriteCapability>,
}

impl Capabilities {
    #[must_use]
    pub fn new(
        reads: impl IntoIterator<Item = ReadCapability>,
        writes: impl IntoIterator<Item = WriteCapability>,
    ) -> Self {
        Self {
            reads: reads.into_iter().collect(),
            writes: writes.into_iter().collect(),
        }
    }

    #[must_use]
    pub fn can_read(&self, capability: ReadCapability) -> bool {
        self.reads.contains(&capability)
    }

    #[must_use]
    pub fn can_write(&self, capability: WriteCapability) -> bool {
        self.writes.contains(&capability)
    }
}
