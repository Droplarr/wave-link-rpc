use crate::{
    ApplicationInfo, Capabilities, Endpoint, Error, ErrorKind, MixerSnapshot, ReadCapability,
    Result,
};
use futures_util::{SinkExt, StreamExt};
use http::{HeaderValue, header::ORIGIN};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, sync::Mutex, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};

const ORIGIN_VALUE: &str = "streamdeck://";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const CLOSE_TIMEOUT: Duration = Duration::from_millis(500);

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compatibility {
    Revision2,
    ReadOnlyUnknownRevision(u32),
}

impl Compatibility {
    #[must_use]
    pub const fn writes_allowed(self) -> bool {
        matches!(self, Self::Revision2)
    }
}

#[derive(Debug)]
struct State {
    socket: Socket,
    next_id: u64,
}

#[derive(Clone, Debug)]
pub struct WaveLinkClient {
    state: Arc<Mutex<State>>,
    application: ApplicationInfo,
    compatibility: Compatibility,
    capabilities: Capabilities,
}

impl WaveLinkClient {
    /// Connects to Wave Link and performs the mandatory compatibility gate.
    ///
    /// # Errors
    ///
    /// Returns an error when the loopback WebSocket connection, origin header,
    /// JSON-RPC exchange, or application-info validation fails.
    pub async fn connect(endpoint: &Endpoint) -> Result<Self> {
        let mut request = endpoint
            .websocket_url()
            .into_client_request()
            .map_err(map_transport)?;
        request
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static(ORIGIN_VALUE));
        let (socket, _) = connect_async(request).await.map_err(map_transport)?;
        let state = Arc::new(Mutex::new(State { socket, next_id: 1 }));
        let application = rpc::<ApplicationInfo>(&state, "getApplicationInfo").await?;
        let compatibility = if application.interface_revision == 2 {
            Compatibility::Revision2
        } else {
            Compatibility::ReadOnlyUnknownRevision(application.interface_revision)
        };
        let writes = if compatibility.writes_allowed() {
            vec![
                crate::WriteCapability::Volume,
                crate::WriteCapability::Mute,
                crate::WriteCapability::PerMixState,
            ]
        } else {
            Vec::new()
        };
        let capabilities = Capabilities::new(
            [
                ReadCapability::Application,
                ReadCapability::Channels,
                ReadCapability::Mixes,
                ReadCapability::InputDevices,
                ReadCapability::OutputDevices,
            ],
            writes,
        );
        Ok(Self {
            state,
            application,
            compatibility,
            capabilities,
        })
    }

    #[must_use]
    pub fn application_info(&self) -> &ApplicationInfo {
        &self.application
    }

    #[must_use]
    pub const fn compatibility(&self) -> Compatibility {
        self.compatibility
    }

    #[must_use]
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Reads a complete normalized mixer snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if any required read request fails or returns an invalid
    /// shape. Collection wrappers and additive fields are decoded tolerantly.
    pub async fn snapshot(&self) -> Result<MixerSnapshot> {
        let channels = rpc_collection(&self.state, "getChannels", "channels").await?;
        let mixes = rpc_collection(&self.state, "getMixes", "mixes").await?;
        let input_devices = rpc_collection(&self.state, "getInputDevices", "inputDevices").await?;
        let output_devices =
            rpc_collection(&self.state, "getOutputDevices", "outputDevices").await?;
        Ok(MixerSnapshot {
            application: self.application.clone(),
            channels,
            mixes,
            input_devices,
            output_devices,
        })
    }

    pub(crate) async fn call_with_params<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T> {
        if !self.compatibility.writes_allowed() {
            return Err(Error::new(
                ErrorKind::UnsupportedRevision,
                "writes are locked for this interface revision",
            ));
        }
        rpc_with_params(&self.state, method, params).await
    }

    /// Closes the transport with a bounded handshake wait.
    ///
    /// # Errors
    ///
    /// Returns a transport error when sending the close frame fails. A peer
    /// that does not finish its close handshake within the bound is tolerated.
    pub async fn close(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        state.socket.close(None).await.map_err(map_transport)?;
        let _ = timeout(CLOSE_TIMEOUT, state.socket.next()).await;
        Ok(())
    }

    pub(crate) async fn wait_for_notification(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        loop {
            let message = state.socket.next().await.ok_or_else(|| {
                Error::new(ErrorKind::Transport, "Wave Link closed the connection")
            })?;
            let message = message.map_err(map_transport)?;
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text)
                        .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
                    if value.get("id").is_none() && value.get("method").is_some() {
                        return Ok(());
                    }
                }
                Message::Ping(payload) => state
                    .socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(map_transport)?,
                Message::Close(_) => {
                    return Err(Error::new(
                        ErrorKind::Transport,
                        "Wave Link closed the connection",
                    ));
                }
                _ => {}
            }
        }
    }

    #[cfg(feature = "unstable-raw")]
    /// Performs an unchecked raw RPC request.
    ///
    /// This API is intentionally unstable and bypasses typed compatibility
    /// guarantees. Consumers must not use it for trusted writes.
    ///
    /// # Errors
    ///
    /// Returns transport, timeout, protocol, or remote JSON-RPC errors.
    pub async fn unstable_raw(&self, method: &str) -> Result<Value> {
        rpc(&self.state, method).await
    }
}

async fn rpc_collection<T: DeserializeOwned>(
    state: &Arc<Mutex<State>>,
    method: &str,
    wrapper: &str,
) -> Result<Vec<T>> {
    let value: Value = rpc(state, method).await?;
    let collection = match value {
        Value::Array(values) => Value::Array(values),
        Value::Object(mut object) => object.remove(wrapper).ok_or_else(|| {
            Error::new(
                ErrorKind::Protocol,
                format!("{method} response omitted {wrapper}"),
            )
        })?,
        _ => {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("{method} returned a non-collection result"),
            ));
        }
    };
    serde_json::from_value(collection)
        .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))
}

async fn rpc<T: DeserializeOwned>(state: &Arc<Mutex<State>>, method: &str) -> Result<T> {
    rpc_with_params(state, method, Value::Null).await
}

async fn rpc_with_params<T: DeserializeOwned>(
    state: &Arc<Mutex<State>>,
    method: &str,
    params: Value,
) -> Result<T> {
    let mut state = state.lock().await;
    let id = state.next_id;
    state.next_id = state
        .next_id
        .checked_add(1)
        .ok_or_else(|| Error::new(ErrorKind::Protocol, "JSON-RPC request ID space exhausted"))?;
    let request = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
    state
        .socket
        .send(Message::Text(request.to_string().into()))
        .await
        .map_err(map_transport)?;

    let response = timeout(REQUEST_TIMEOUT, async {
        loop {
            let message = state.socket.next().await.ok_or_else(|| {
                Error::new(ErrorKind::Transport, "Wave Link closed the connection")
            })?;
            let message = message.map_err(map_transport)?;
            if !message.is_text() {
                continue;
            }
            let value: Value = serde_json::from_str(message.to_text().map_err(map_transport)?)
                .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))?;
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(Error::new(ErrorKind::Protocol, error.to_string()));
            }
            return value.get("result").cloned().ok_or_else(|| {
                Error::new(ErrorKind::Protocol, "JSON-RPC response omitted result")
            });
        }
    })
    .await
    .map_err(|_| Error::new(ErrorKind::Timeout, format!("{method} timed out")))??;

    serde_json::from_value(response)
        .map_err(|error| Error::new(ErrorKind::Protocol, error.to_string()))
}

fn map_transport(error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Transport, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_hdr_async, tungstenite::handshake::server::Response};

    #[allow(clippy::result_large_err)]
    async fn server(revision: u32) -> (Endpoint, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind server");
        let port = listener.local_addr().expect("server address").port();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept client");
            let mut socket =
                accept_hdr_async(stream, |request: &http::Request<()>, response: Response| {
                    assert_eq!(request.headers().get(ORIGIN).expect("origin"), ORIGIN_VALUE);
                    Ok(response)
                })
                .await
                .expect("WebSocket handshake");
            while let Some(Ok(message)) = socket.next().await {
                if message.is_close() {
                    break;
                }
                if !message.is_text() {
                    continue;
                }
                let request: Value = serde_json::from_str(message.to_text().expect("text"))
                    .expect("JSON-RPC request");
                let method = request["method"].as_str().expect("method");
                let result = match method {
                    "getApplicationInfo" => json!({
                        "appId": "EWL",
                        "applicationVersion": "3.2.9.4002",
                        "build": 4002,
                        "interfaceRevision": revision,
                        "futureField": true
                    }),
                    "getChannels" => {
                        json!({"channels": [{"id": "channel-1", "name": "Mic", "level": 0.5}]})
                    }
                    "getMixes" => {
                        json!({"mixes": [{"id": "mix-1", "name": "Stream", "mute": false}]})
                    }
                    "getInputDevices" => json!({"inputDevices": [{"id": "input-1", "gain": 12}]}),
                    "getOutputDevices" => {
                        json!({"outputDevices": [{"id": "output-1", "mixId": "mix-1"}]})
                    }
                    _ => json!(null),
                };
                let response = json!({"jsonrpc": "2.0", "id": request["id"], "result": result});
                socket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .expect("send response");
            }
        });
        (Endpoint::new(port).expect("valid endpoint"), task)
    }

    #[tokio::test]
    async fn connects_with_origin_and_reads_tolerant_snapshot() {
        let (endpoint, server) = server(2).await;
        let client = WaveLinkClient::connect(&endpoint)
            .await
            .expect("connect client");
        assert_eq!(client.compatibility(), Compatibility::Revision2);
        let snapshot = client.snapshot().await.expect("read snapshot");
        assert_eq!(snapshot.channels.len(), 1);
        assert_eq!(snapshot.mixes.len(), 1);
        assert_eq!(snapshot.input_devices.len(), 1);
        assert_eq!(snapshot.output_devices.len(), 1);
        assert!(snapshot.application.extra.contains_key("futureField"));
        client.close().await.expect("close client");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn unknown_revision_is_explicitly_read_only() {
        let (endpoint, server) = server(99).await;
        let client = WaveLinkClient::connect(&endpoint)
            .await
            .expect("connect client");
        assert_eq!(
            client.compatibility(),
            Compatibility::ReadOnlyUnknownRevision(99)
        );
        assert!(!client.compatibility().writes_allowed());
        client.close().await.expect("close client");
        server.await.expect("server task");
    }
}
