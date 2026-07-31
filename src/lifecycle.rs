use crate::{
    BatchResult, ChannelId, Discovery, Error, ErrorKind, FadeCurve, FadeOptions, MixId,
    MixerSnapshot, Operation, Result, Volume, WaveLinkClient,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, broadcast, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Instant, sleep_until},
};

const EVENT_CAPACITY: usize = 32;
const COMMAND_CAPACITY: usize = 32;
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const MAX_FADE: Duration = Duration::from_secs(5);
const FADE_STEP: Duration = Duration::from_millis(34);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Synchronizing,
    Ready,
    ReadOnlyUnknownRevision,
    Reconnecting,
    Shutdown,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event {
    StateChanged(ConnectionState),
    Snapshot(Arc<MixerSnapshot>),
    Resynchronized(Arc<MixerSnapshot>),
    LaggedAndResynchronized {
        skipped: u64,
        snapshot: Arc<MixerSnapshot>,
    },
    Shutdown,
}

pub struct Subscription {
    events: broadcast::Receiver<Event>,
    snapshot: watch::Receiver<Option<Arc<MixerSnapshot>>>,
}

impl Subscription {
    /// Receives the next lifecycle event.
    ///
    /// A lagging subscriber receives one resynchronization event containing the
    /// latest complete snapshot rather than an unbounded replay.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::Shutdown`] after the client closes all event senders.
    pub async fn recv(&mut self) -> Result<Event> {
        match self.events.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                let snapshot = self.snapshot.borrow().clone().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Protocol,
                        "subscriber lagged before a snapshot was available",
                    )
                })?;
                Ok(Event::LaggedAndResynchronized { skipped, snapshot })
            }
            Err(broadcast::error::RecvError::Closed) => Err(Error::new(
                ErrorKind::Shutdown,
                "synchronized client is shut down",
            )),
        }
    }
}

enum Command {
    Apply(Operation, bool, oneshot::Sender<Result<()>>),
    Batch(Vec<Operation>, oneshot::Sender<Result<BatchResult>>),
    Refresh(oneshot::Sender<Result<Arc<MixerSnapshot>>>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum FadeTarget {
    Channel(ChannelId),
    ChannelMix(ChannelId, MixId),
}

struct Inner {
    state: watch::Receiver<ConnectionState>,
    snapshot: watch::Receiver<Option<Arc<MixerSnapshot>>>,
    events: broadcast::Sender<Event>,
    commands: mpsc::Sender<Command>,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
    fades: StdMutex<HashMap<FadeTarget, (u64, watch::Sender<bool>)>>,
    next_fade: AtomicU64,
}

#[derive(Clone)]
pub struct SynchronizedClient {
    inner: Arc<Inner>,
}

impl SynchronizedClient {
    #[must_use]
    pub fn spawn(discovery: Discovery) -> Self {
        let (state_tx, state) = watch::channel(ConnectionState::Disconnected);
        let (snapshot_tx, snapshot) = watch::channel(None);
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let (commands, command_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task_events = events.clone();
        let task = tokio::spawn(run(
            discovery,
            state_tx,
            snapshot_tx,
            task_events,
            command_rx,
            shutdown_rx,
        ));
        Self {
            inner: Arc::new(Inner {
                state,
                snapshot,
                events,
                commands,
                shutdown,
                task: Mutex::new(Some(task)),
                fades: StdMutex::new(HashMap::new()),
                next_fade: AtomicU64::new(1),
            }),
        }
    }

    #[must_use]
    pub fn state(&self) -> ConnectionState {
        *self.inner.state.borrow()
    }

    #[must_use]
    pub fn snapshot(&self) -> Option<Arc<MixerSnapshot>> {
        self.inner.snapshot.borrow().clone()
    }

    #[must_use]
    pub fn subscribe(&self) -> Subscription {
        Subscription {
            events: self.inner.events.subscribe(),
            snapshot: self.inner.snapshot.clone(),
        }
    }

    /// Waits until a synchronized snapshot is ready.
    ///
    /// # Errors
    ///
    /// Returns a shutdown error if the lifecycle task exits first.
    pub async fn ready(&self) -> Result<Arc<MixerSnapshot>> {
        let mut snapshots = self.inner.snapshot.clone();
        loop {
            if let Some(snapshot) = snapshots.borrow().clone() {
                return Ok(snapshot);
            }
            snapshots.changed().await.map_err(|_| {
                Error::new(
                    ErrorKind::Shutdown,
                    "client shut down before becoming ready",
                )
            })?;
        }
    }

    /// Applies one mutation through the lifecycle-owned serialized transport.
    /// A fresh authoritative snapshot is obtained after success or failure.
    ///
    /// # Errors
    ///
    /// Rejects writes unless the client is ready and write-capable, and returns
    /// the mutation or resynchronization error when either operation fails.
    pub async fn apply(&self, operation: &Operation) -> Result<()> {
        self.cancel_operation_fade(operation);
        self.dispatch_apply(operation.clone(), true).await
    }

    /// Applies an ordered, non-atomic batch and resynchronizes afterward.
    ///
    /// # Errors
    ///
    /// Returns an error when the lifecycle is not writable or the authoritative
    /// post-batch snapshot cannot be read. Individual mutation failures remain
    /// represented in [`BatchResult`].
    pub async fn apply_batch(&self, operations: Vec<Operation>) -> Result<BatchResult> {
        for operation in &operations {
            self.cancel_operation_fade(operation);
        }
        self.ensure_writable()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Batch(operations, reply))
            .await
            .map_err(|_| self.unavailable_error())?;
        response.await.map_err(|_| self.unavailable_error())?
    }

    /// Forces an authoritative snapshot through the active connection.
    ///
    /// # Errors
    ///
    /// Returns an error unless the lifecycle currently owns a ready connection,
    /// or when any required read fails.
    pub async fn refresh(&self) -> Result<Arc<MixerSnapshot>> {
        self.ensure_connected()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Refresh(reply))
            .await
            .map_err(|_| self.unavailable_error())?;
        response.await.map_err(|_| self.unavailable_error())?
    }

    /// Fades a channel from the synchronized volume to an exact target.
    ///
    /// # Errors
    ///
    /// Returns an invalid-value, missing-state, lifecycle, cancellation, or RPC
    /// error. A replacement/direct set/disconnect/shutdown cancels this fade.
    pub async fn fade_channel_volume(
        &self,
        channel: ChannelId,
        target: Volume,
        options: FadeOptions,
    ) -> Result<()> {
        self.fade(FadeTarget::Channel(channel), target, options)
            .await
    }

    /// Fades one channel/mix pair from synchronized state to an exact target.
    ///
    /// # Errors
    ///
    /// Returns an invalid-value, missing-state, lifecycle, cancellation, or RPC
    /// error. Independent targets may fade concurrently.
    pub async fn fade_channel_mix_volume(
        &self,
        channel: ChannelId,
        mix: MixId,
        target: Volume,
        options: FadeOptions,
    ) -> Result<()> {
        self.fade(FadeTarget::ChannelMix(channel, mix), target, options)
            .await
    }

    /// Cancels all active fades. Already-completed writes are not rolled back.
    pub fn cancel_fades(&self) {
        let mut fades = self
            .inner
            .fades
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, cancel) in fades.values() {
            let _ = cancel.send(true);
        }
        fades.clear();
    }

    /// Stops fades and reconnect work, closes the transport, and joins the task.
    ///
    /// # Errors
    ///
    /// Returns an error if the lifecycle task panicked.
    pub async fn shutdown(&self) -> Result<()> {
        self.cancel_fades();
        let _ = self.inner.shutdown.send(true);
        if let Some(task) = self.inner.task.lock().await.take() {
            task.await.map_err(|error| {
                Error::new(
                    ErrorKind::Shutdown,
                    format!("lifecycle task failed: {error}"),
                )
            })?;
        }
        Ok(())
    }

    async fn dispatch_apply(&self, operation: Operation, resnapshot: bool) -> Result<()> {
        self.ensure_writable()?;
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .send(Command::Apply(operation, resnapshot, reply))
            .await
            .map_err(|_| self.unavailable_error())?;
        response.await.map_err(|_| self.unavailable_error())?
    }

    async fn fade(
        &self,
        target_key: FadeTarget,
        target: Volume,
        options: FadeOptions,
    ) -> Result<()> {
        if options.duration > MAX_FADE {
            return Err(Error::new(
                ErrorKind::InvalidValue,
                "fade duration must be between 0 and 5000 ms",
            ));
        }
        let id = self.inner.next_fade.fetch_add(1, Ordering::Relaxed);
        let (cancel, mut cancelled) = watch::channel(false);
        if let Some((_, previous)) = self
            .inner
            .fades
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(target_key.clone(), (id, cancel))
        {
            let _ = previous.send(true);
        }

        let result = async {
            let snapshot = self.refresh().await?;
            let start = target_volume(&snapshot, &target_key)?;
            if options.duration.is_zero() {
                return self
                    .dispatch_apply(fade_operation(&target_key, target), true)
                    .await;
            }
            let started = Instant::now();
            let mut deadline = started + FADE_STEP;
            let mut state = self.inner.state.clone();
            loop {
                tokio::select! {
                    () = sleep_until(deadline) => {}
                    changed = cancelled.changed() => {
                        break Err(cancelled_error(changed.is_err()));
                    }
                    changed = state.changed() => {
                        if changed.is_err() || *state.borrow() != ConnectionState::Ready {
                            break Err(self.unavailable_error());
                        }
                        continue;
                    }
                }
                let elapsed = started.elapsed();
                if elapsed >= options.duration {
                    break self
                        .dispatch_apply(fade_operation(&target_key, target), true)
                        .await;
                }
                let progress =
                    (elapsed.as_secs_f32() / options.duration.as_secs_f32()).clamp(0.0, 1.0);
                let curved = match options.curve {
                    FadeCurve::Linear => progress,
                    FadeCurve::Perceptual => progress * progress,
                };
                let value = start.get() + ((target.get() - start.get()) * curved);
                if let Err(error) = self
                    .dispatch_apply(fade_operation(&target_key, Volume::new(value)?), false)
                    .await
                {
                    break Err(error);
                }
                deadline += FADE_STEP;
                while deadline <= Instant::now() {
                    deadline += FADE_STEP;
                }
            }
        }
        .await;

        let mut fades = self
            .inner
            .fades
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if fades.get(&target_key).map(|(current, _)| *current) == Some(id) {
            fades.remove(&target_key);
        }
        result
    }

    fn ensure_writable(&self) -> Result<()> {
        match self.state() {
            ConnectionState::Ready => Ok(()),
            ConnectionState::ReadOnlyUnknownRevision => Err(Error::new(
                ErrorKind::UnsupportedRevision,
                "writes are locked for this interface revision",
            )),
            ConnectionState::Shutdown => Err(Error::new(
                ErrorKind::Shutdown,
                "synchronized client is shut down",
            )),
            _ => Err(Error::new(
                ErrorKind::Transport,
                "synchronized client is not ready",
            )),
        }
    }

    fn unavailable_error(&self) -> Error {
        match self.state() {
            ConnectionState::ReadOnlyUnknownRevision => Error::new(
                ErrorKind::UnsupportedRevision,
                "writes are locked for this interface revision",
            ),
            ConnectionState::Shutdown => {
                Error::new(ErrorKind::Shutdown, "synchronized client is shut down")
            }
            _ => Error::new(ErrorKind::Transport, "synchronized client is not ready"),
        }
    }

    fn ensure_connected(&self) -> Result<()> {
        match self.state() {
            ConnectionState::Ready | ConnectionState::ReadOnlyUnknownRevision => Ok(()),
            _ => Err(self.unavailable_error()),
        }
    }

    fn cancel_operation_fade(&self, operation: &Operation) {
        let target = match operation {
            Operation::SetChannelVolume { channel, .. } => {
                Some(FadeTarget::Channel(channel.clone()))
            }
            Operation::SetChannelMixVolume { channel, mix, .. } => {
                Some(FadeTarget::ChannelMix(channel.clone(), mix.clone()))
            }
            _ => None,
        };
        if let Some(target) = target {
            if let Some((_, cancel)) = self
                .inner
                .fades
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&target)
            {
                let _ = cancel.send(true);
            }
        }
    }
}

fn fade_operation(target: &FadeTarget, volume: Volume) -> Operation {
    match target {
        FadeTarget::Channel(channel) => Operation::SetChannelVolume {
            channel: channel.clone(),
            volume,
        },
        FadeTarget::ChannelMix(channel, mix) => Operation::SetChannelMixVolume {
            channel: channel.clone(),
            mix: mix.clone(),
            volume,
        },
    }
}

fn target_volume(snapshot: &MixerSnapshot, target: &FadeTarget) -> Result<Volume> {
    match target {
        FadeTarget::Channel(channel) => snapshot
            .channels
            .iter()
            .find(|item| &item.id == channel)
            .ok_or_else(|| Error::new(ErrorKind::CapabilityUnavailable, "channel is absent"))?
            .volume()?
            .ok_or_else(|| {
                Error::new(ErrorKind::CapabilityUnavailable, "channel volume is absent")
            }),
        FadeTarget::ChannelMix(channel, mix) => snapshot
            .channels
            .iter()
            .find(|item| &item.id == channel)
            .ok_or_else(|| Error::new(ErrorKind::CapabilityUnavailable, "channel is absent"))?
            .mix(mix)?
            .and_then(|state| state.volume)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::CapabilityUnavailable,
                    "channel mix volume is absent",
                )
            }),
    }
}

fn cancelled_error(channel_closed: bool) -> Error {
    Error::new(
        if channel_closed {
            ErrorKind::Shutdown
        } else {
            ErrorKind::CapabilityUnavailable
        },
        "fade was cancelled",
    )
}

async fn run(
    discovery: Discovery,
    state: watch::Sender<ConnectionState>,
    snapshot: watch::Sender<Option<Arc<MixerSnapshot>>>,
    events: broadcast::Sender<Event>,
    mut commands: mpsc::Receiver<Command>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut attempt = 0_u32;
    loop {
        if *shutdown.borrow() {
            break;
        }
        transition(
            &state,
            &events,
            if attempt == 0 {
                ConnectionState::Connecting
            } else {
                ConnectionState::Reconnecting
            },
        );
        let connection = async {
            let endpoint = discovery.discover().await?;
            WaveLinkClient::connect(&endpoint).await
        }
        .await;
        let Ok(client) = connection else {
            if wait_or_shutdown(backoff(attempt), &mut commands, &mut shutdown).await {
                break;
            }
            attempt = attempt.saturating_add(1);
            continue;
        };
        transition(&state, &events, ConnectionState::Synchronizing);
        if let Ok(value) = client.snapshot().await {
            publish_snapshot(&snapshot, &events, value);
            let ready_state = if client.compatibility().writes_allowed() {
                ConnectionState::Ready
            } else {
                ConnectionState::ReadOnlyUnknownRevision
            };
            transition(&state, &events, ready_state);
            attempt = 0;
        } else {
            let _ = client.close().await;
            continue;
        }

        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = client.close().await;
                        transition(&state, &events, ConnectionState::Shutdown);
                        let _ = events.send(Event::Shutdown);
                        return;
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else {
                        let _ = client.close().await;
                        return;
                    };
                    if !handle_command(command, &client, &snapshot, &events).await {
                        break;
                    }
                }
                notification = client.wait_for_notification() => {
                    if notification.is_err() {
                        break;
                    }
                    transition(&state, &events, ConnectionState::Synchronizing);
                    match client.snapshot().await {
                        Ok(value) => {
                            publish_snapshot(&snapshot, &events, value);
                            let ready_state = if client.compatibility().writes_allowed() {
                                ConnectionState::Ready
                            } else {
                                ConnectionState::ReadOnlyUnknownRevision
                            };
                            transition(&state, &events, ready_state);
                        }
                        Err(_) => break,
                    }
                }
            }
        }
        transition(&state, &events, ConnectionState::Reconnecting);
    }
    transition(&state, &events, ConnectionState::Shutdown);
    let _ = events.send(Event::Shutdown);
}

async fn handle_command(
    command: Command,
    client: &WaveLinkClient,
    snapshot: &watch::Sender<Option<Arc<MixerSnapshot>>>,
    events: &broadcast::Sender<Event>,
) -> bool {
    match command {
        Command::Apply(operation, resnapshot, reply) => {
            let mutation = client.apply(&operation).await;
            let should_snapshot = resnapshot || mutation.is_err();
            let refreshed = if should_snapshot {
                resnapshot_client(client, snapshot, events).await
            } else {
                Ok(snapshot.borrow().clone().expect("ready snapshot"))
            };
            let keep_connected = refreshed.is_ok();
            let result = mutation.and(refreshed.map(|_| ()));
            let _ = reply.send(result);
            keep_connected
        }
        Command::Batch(operations, reply) => {
            let result = client.apply_batch(operations).await;
            let refreshed = resnapshot_client(client, snapshot, events).await;
            let keep_connected = refreshed.is_ok();
            let _ = reply.send(refreshed.map(|_| result));
            keep_connected
        }
        Command::Refresh(reply) => {
            let result = resnapshot_client(client, snapshot, events).await;
            let keep_connected = result.is_ok();
            let _ = reply.send(result);
            keep_connected
        }
    }
}

async fn resnapshot_client(
    client: &WaveLinkClient,
    snapshot: &watch::Sender<Option<Arc<MixerSnapshot>>>,
    events: &broadcast::Sender<Event>,
) -> Result<Arc<MixerSnapshot>> {
    let value = Arc::new(client.snapshot().await?);
    snapshot.send_replace(Some(value.clone()));
    let _ = events.send(Event::Resynchronized(value.clone()));
    Ok(value)
}

fn publish_snapshot(
    snapshot: &watch::Sender<Option<Arc<MixerSnapshot>>>,
    events: &broadcast::Sender<Event>,
    value: MixerSnapshot,
) {
    let value = Arc::new(value);
    let replacing = snapshot.borrow().is_some();
    snapshot.send_replace(Some(value.clone()));
    let _ = events.send(if replacing {
        Event::Resynchronized(value)
    } else {
        Event::Snapshot(value)
    });
}

fn transition(
    state: &watch::Sender<ConnectionState>,
    events: &broadcast::Sender<Event>,
    next: ConnectionState,
) {
    state.send_replace(next);
    let _ = events.send(Event::StateChanged(next));
}

async fn wait_or_shutdown(
    duration: Duration,
    commands: &mut mpsc::Receiver<Command>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let deadline = Instant::now() + duration;
    loop {
        tokio::select! {
            () = sleep_until(deadline) => return false,
            changed = shutdown.changed() => return changed.is_err() || *shutdown.borrow(),
            command = commands.recv() => {
                let Some(command) = command else { return true; };
                reject_command(command);
            }
        }
    }
}

fn reject_command(command: Command) {
    let error = || Error::new(ErrorKind::Transport, "synchronized client is not ready");
    match command {
        Command::Apply(_, _, reply) => {
            let _ = reply.send(Err(error()));
        }
        Command::Batch(_, reply) => {
            let _ = reply.send(Err(error()));
        }
        Command::Refresh(reply) => {
            let _ = reply.send(Err(error()));
        }
    }
}

fn backoff(attempt: u32) -> Duration {
    let shift = attempt.min(4);
    let base = INITIAL_BACKOFF
        .saturating_mul(1_u32 << shift)
        .min(MAX_BACKOFF);
    let factor = u128::from(80 + (attempt.wrapping_mul(37) % 41));
    let milliseconds = (base.as_millis() * factor) / 100;
    Duration::from_millis(u64::try_from(milliseconds).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use tokio::{net::TcpListener, sync::mpsc, time::sleep};
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[allow(clippy::too_many_lines)]
    async fn mock_client(
        revision: u32,
        fail_set_mix: bool,
    ) -> (SynchronizedClient, mpsc::UnboundedReceiver<String>, TempDir) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let directory = tempfile::tempdir().expect("temporary directory");
        let metadata = directory.path().join("ws-info.json");
        tokio::fs::write(&metadata, format!(r#"{{"port":{port}}}"#))
            .await
            .expect("metadata");
        let (calls, call_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(stream).await.expect("websocket");
            let mut channel_volume = 0.25_f64;
            let mut channel_muted = false;
            let mut channel_mix_volume = 0.4_f64;
            let mut channel_mix_muted = false;
            let mut mix_volume = 0.5_f64;
            let mut mix_muted = false;
            while let Some(Ok(message)) = socket.next().await {
                if message.is_close() {
                    break;
                }
                if !message.is_text() {
                    continue;
                }
                let request: Value =
                    serde_json::from_str(message.to_text().expect("text")).expect("request");
                let method = request["method"].as_str().expect("method");
                let _ = calls.send(method.to_owned());
                let result = match method {
                    "getApplicationInfo" => json!({"interfaceRevision": revision}),
                    "getChannels" => json!({"channels": [{
                        "id": "channel-1",
                        "level": channel_volume,
                        "isMuted": channel_muted,
                        "mixes": [{
                            "id": "mix-1",
                            "level": channel_mix_volume,
                            "isMuted": channel_mix_muted
                        }]
                    }]}),
                    "getMixes" => json!({"mixes": [{
                        "id": "mix-1",
                        "level": mix_volume,
                        "isMuted": mix_muted
                    }]}),
                    "getInputDevices" => json!({"inputDevices": []}),
                    "getOutputDevices" => json!({"outputDevices": []}),
                    "setChannel" => {
                        let params = &request["params"];
                        if let Some(value) = params.get("level").and_then(Value::as_f64) {
                            channel_volume = value;
                        }
                        if let Some(value) = params.get("isMuted").and_then(Value::as_bool) {
                            channel_muted = value;
                        }
                        if let Some(state) = params
                            .get("mixes")
                            .and_then(Value::as_array)
                            .and_then(|v| v.first())
                        {
                            if let Some(value) = state.get("level").and_then(Value::as_f64) {
                                channel_mix_volume = value;
                            }
                            if let Some(value) = state.get("isMuted").and_then(Value::as_bool) {
                                channel_mix_muted = value;
                            }
                        }
                        json!(null)
                    }
                    "setMix" if fail_set_mix => {
                        let response = json!({
                            "jsonrpc": "2.0",
                            "id": request["id"],
                            "error": {"code": -1, "message": "synthetic failure"}
                        });
                        socket
                            .send(Message::Text(response.to_string().into()))
                            .await
                            .expect("response");
                        continue;
                    }
                    "setMix" => {
                        let params = &request["params"];
                        if let Some(value) = params.get("level").and_then(Value::as_f64) {
                            mix_volume = value;
                        }
                        if let Some(value) = params.get("isMuted").and_then(Value::as_bool) {
                            mix_muted = value;
                        }
                        json!(null)
                    }
                    _ => panic!("unexpected method {method}"),
                };
                let response = json!({"jsonrpc": "2.0", "id": request["id"], "result": result});
                socket
                    .send(Message::Text(response.to_string().into()))
                    .await
                    .expect("response");
            }
        });
        let client = SynchronizedClient::spawn(Discovery::from_metadata_path(metadata));
        (client, call_rx, directory)
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_jittered() {
        let values: Vec<_> = (0..20).map(backoff).collect();
        assert!(values.iter().all(|value| *value <= Duration::from_secs(6)));
        assert!(values[0] >= Duration::from_millis(200));
        assert_ne!(values[4], values[5]);
    }

    #[test]
    fn fade_operations_are_scoped_to_the_target() {
        let volume = Volume::new(0.5).expect("volume");
        assert!(matches!(
            fade_operation(&FadeTarget::Channel(ChannelId::new("channel")), volume),
            Operation::SetChannelVolume { .. }
        ));
        assert!(matches!(
            fade_operation(
                &FadeTarget::ChannelMix(ChannelId::new("channel"), MixId::new("mix")),
                volume
            ),
            Operation::SetChannelMixVolume { .. }
        ));
    }

    #[tokio::test]
    async fn synchronized_apply_and_refresh_publish_authoritative_state() {
        let (client, mut calls, _directory) = mock_client(2, false).await;
        client.ready().await.expect("ready");
        client
            .apply(&Operation::SetChannelVolume {
                channel: ChannelId::new("channel-1"),
                volume: Volume::new(0.8).expect("volume"),
            })
            .await
            .expect("apply");
        let snapshot = client.snapshot().expect("snapshot");
        assert_eq!(
            snapshot.channels[0].volume().expect("volume"),
            Some(Volume::new(0.8).expect("volume"))
        );
        client.refresh().await.expect("refresh");
        client.shutdown().await.expect("shutdown");

        let mut methods = Vec::new();
        while let Ok(method) = calls.try_recv() {
            methods.push(method);
        }
        assert!(methods.contains(&"setChannel".to_owned()));
        assert!(
            methods
                .iter()
                .filter(|method| method.as_str() == "getChannels")
                .count()
                >= 3
        );
    }

    #[tokio::test]
    async fn batch_failure_stops_and_resynchronizes() {
        let (client, _calls, _directory) = mock_client(2, true).await;
        client.ready().await.expect("ready");
        let result = client
            .apply_batch(vec![
                Operation::SetChannelMute {
                    channel: ChannelId::new("channel-1"),
                    muted: true,
                },
                Operation::SetMixVolume {
                    mix: MixId::new("mix-1"),
                    volume: Volume::new(0.8).expect("volume"),
                },
                Operation::SetChannelVolume {
                    channel: ChannelId::new("channel-1"),
                    volume: Volume::new(0.9).expect("volume"),
                },
            ])
            .await
            .expect("batch result");
        assert_eq!(
            result.operations[0].status,
            crate::OperationStatus::Succeeded
        );
        assert_eq!(result.operations[1].status, crate::OperationStatus::Failed);
        assert_eq!(
            result.operations[2].status,
            crate::OperationStatus::NotAttempted
        );
        assert_eq!(
            client.snapshot().expect("snapshot").channels[0].muted(),
            Some(true)
        );
        client.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn mix_fade_reaches_endpoint_and_direct_set_cancels_channel_fade() {
        let (client, _calls, _directory) = mock_client(2, false).await;
        client.ready().await.expect("ready");
        client
            .fade_channel_mix_volume(
                ChannelId::new("channel-1"),
                MixId::new("mix-1"),
                Volume::new(0.7).expect("volume"),
                FadeOptions {
                    duration: Duration::from_millis(80),
                    curve: FadeCurve::Linear,
                },
            )
            .await
            .expect("mix fade");
        assert_eq!(
            client.snapshot().expect("snapshot").channels[0]
                .mix(&MixId::new("mix-1"))
                .expect("mix")
                .expect("present")
                .volume,
            Some(Volume::new(0.7).expect("volume"))
        );

        let fading = client.clone();
        let fade = tokio::spawn(async move {
            fading
                .fade_channel_volume(
                    ChannelId::new("channel-1"),
                    Volume::new(1.0).expect("volume"),
                    FadeOptions {
                        duration: Duration::from_millis(300),
                        curve: FadeCurve::Perceptual,
                    },
                )
                .await
        });
        sleep(Duration::from_millis(60)).await;
        client
            .apply(&Operation::SetChannelVolume {
                channel: ChannelId::new("channel-1"),
                volume: Volume::new(0.1).expect("volume"),
            })
            .await
            .expect("direct set");
        assert!(fade.await.expect("fade task").is_err());
        assert_eq!(
            client.snapshot().expect("snapshot").channels[0]
                .volume()
                .expect("volume"),
            Some(Volume::new(0.1).expect("volume"))
        );
        client.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn unknown_revision_refreshes_but_rejects_writes() {
        let (client, _calls, _directory) = mock_client(99, false).await;
        client.ready().await.expect("read-only snapshot");
        assert_eq!(client.state(), ConnectionState::ReadOnlyUnknownRevision);
        client.refresh().await.expect("read-only refresh");
        let error = client
            .apply(&Operation::SetChannelMute {
                channel: ChannelId::new("channel-1"),
                muted: true,
            })
            .await
            .expect_err("write locked");
        assert_eq!(error.kind(), ErrorKind::UnsupportedRevision);
        client.shutdown().await.expect("shutdown");
    }
}
