use crate::{Error, ErrorKind, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};

const PACKAGE_LOCAL_STATE: &str = "Packages/Elgato.WaveLink_g54w8ztgkx496/LocalState/ws-info.json";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoint {
    port: u16,
}

impl Endpoint {
    /// Creates a validated loopback endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when `port` is zero.
    pub fn new(port: u16) -> Result<Self> {
        if port == 0 {
            return Err(Error::new(
                ErrorKind::Discovery,
                "Wave Link advertised port zero",
            ));
        }
        Ok(Self { port })
    }

    #[must_use]
    pub const fn port(&self) -> u16 {
        self.port
    }

    #[must_use]
    pub fn websocket_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }
}

#[derive(Clone, Debug)]
pub struct Discovery {
    metadata_path: PathBuf,
}

impl Discovery {
    /// Locates the Microsoft Store/MSIX Wave Link metadata file.
    ///
    /// # Errors
    ///
    /// Returns an error when `LOCALAPPDATA` is unavailable.
    pub fn msix_default() -> Result<Self> {
        let local_app_data = std::env::var_os("LOCALAPPDATA").ok_or_else(|| {
            Error::new(
                ErrorKind::Discovery,
                "LOCALAPPDATA is not set; automatic discovery requires Windows",
            )
        })?;
        Ok(Self::from_metadata_path(
            PathBuf::from(local_app_data).join(PACKAGE_LOCAL_STATE),
        ))
    }

    #[must_use]
    pub fn from_metadata_path(path: impl Into<PathBuf>) -> Self {
        Self {
            metadata_path: path.into(),
        }
    }

    #[must_use]
    pub fn metadata_path(&self) -> &Path {
        &self.metadata_path
    }

    /// Reads and validates the advertised loopback endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error for unreadable metadata, invalid JSON, or an invalid
    /// port.
    pub async fn discover(&self) -> Result<Endpoint> {
        #[derive(Deserialize)]
        struct Metadata {
            port: u16,
        }

        let bytes = tokio::fs::read(&self.metadata_path).await?;
        let metadata: Metadata = serde_json::from_slice(&bytes).map_err(|error| {
            Error::new(
                ErrorKind::Discovery,
                format!("invalid ws-info.json: {error}"),
            )
        })?;
        Endpoint::new(metadata.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_advertised_port() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("ws-info.json");
        tokio::fs::write(&path, br#"{"port":1824,"extra":"ignored"}"#)
            .await
            .expect("write metadata");
        let endpoint = Discovery::from_metadata_path(path)
            .discover()
            .await
            .expect("discover endpoint");
        assert_eq!(endpoint.port(), 1824);
        assert_eq!(endpoint.websocket_url(), "ws://127.0.0.1:1824");
    }

    #[tokio::test]
    async fn rejects_zero_port() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("ws-info.json");
        tokio::fs::write(&path, br#"{"port":0}"#)
            .await
            .expect("write metadata");
        assert!(
            Discovery::from_metadata_path(path)
                .discover()
                .await
                .is_err()
        );
    }
}
