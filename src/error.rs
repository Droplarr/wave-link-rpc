use serde::{Deserialize, Serialize};
use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, thiserror::Error)]
#[non_exhaustive]
pub enum ErrorKind {
    #[error("invalid value")]
    InvalidValue,
    #[error("unsupported interface revision")]
    UnsupportedRevision,
    #[error("capability is unavailable")]
    CapabilityUnavailable,
    #[error("ambiguous name")]
    AmbiguousName,
    #[error("protocol error")]
    Protocol,
    #[error("transport error")]
    Transport,
    #[error("discovery failed")]
    Discovery,
    #[error("request timed out")]
    Timeout,
    #[error("client is shut down")]
    Shutdown,
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::new(ErrorKind::Discovery, error.to_string())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, thiserror::Error)]
#[error("{kind}: {context}")]
pub struct Error {
    kind: ErrorKind,
    context: Cow<'static, str>,
}

impl Error {
    #[must_use]
    pub fn new(kind: ErrorKind, context: impl Into<Cow<'static, str>>) -> Self {
        Self {
            kind,
            context: context.into(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    #[must_use]
    pub fn context(&self) -> &str {
        &self.context
    }
}
