use crate::{Discovery, Error, ErrorKind, MixerSnapshot, Result, WaveLinkClient};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, broadcast, watch},
    task::JoinHandle,
    time::sleep,
};

const EVENT_CAPACITY: usize = 32;
const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

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

pub struct SynchronizedClient {
    state: watch::Receiver<ConnectionState>,
    snapshot: watch::Receiver<Option<Arc<MixerSnapshot>>>,
    events: broadcast::Sender<Event>,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl SynchronizedClient {
    #[must_use]
    pub fn spawn(discovery: Discovery) -> Self {
        let (state_tx, state) = watch::channel(ConnectionState::Disconnected);
        let (snapshot_tx, snapshot) = watch::channel(None);
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let task_events = events.clone();
        let task = tokio::spawn(run(
            discovery,
            state_tx,
            snapshot_tx,
            task_events,
            shutdown_rx,
        ));
        Self {
            state,
            snapshot,
            events,
            shutdown,
            task: Mutex::new(Some(task)),
        }
    }

    #[must_use]
    pub fn state(&self) -> ConnectionState {
        *self.state.borrow()
    }

    #[must_use]
    pub fn snapshot(&self) -> Option<Arc<MixerSnapshot>> {
        self.snapshot.borrow().clone()
    }

    #[must_use]
    pub fn subscribe(&self) -> Subscription {
        Subscription {
            events: self.events.subscribe(),
            snapshot: self.snapshot.clone(),
        }
    }

    /// Waits until a synchronized snapshot is ready.
    ///
    /// # Errors
    ///
    /// Returns a shutdown error if the lifecycle task exits first.
    pub async fn ready(&self) -> Result<Arc<MixerSnapshot>> {
        let mut snapshots = self.snapshot.clone();
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

    /// Stops reconnect work, closes the active transport, and joins the task.
    ///
    /// # Errors
    ///
    /// Returns an error if the lifecycle task panicked.
    pub async fn shutdown(&self) -> Result<()> {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.lock().await.take() {
            task.await.map_err(|error| {
                Error::new(
                    ErrorKind::Shutdown,
                    format!("lifecycle task failed: {error}"),
                )
            })?;
        }
        Ok(())
    }
}

async fn run(
    discovery: Discovery,
    state: watch::Sender<ConnectionState>,
    snapshot: watch::Sender<Option<Arc<MixerSnapshot>>>,
    events: broadcast::Sender<Event>,
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
            if wait_or_shutdown(backoff(attempt), &mut shutdown).await {
                break;
            }
            attempt = attempt.saturating_add(1);
            continue;
        };
        transition(&state, &events, ConnectionState::Synchronizing);
        if let Ok(value) = client.snapshot().await {
            let value = Arc::new(value);
            let replacing = snapshot.borrow().is_some();
            snapshot.send_replace(Some(value.clone()));
            let ready_state = if client.compatibility().writes_allowed() {
                ConnectionState::Ready
            } else {
                ConnectionState::ReadOnlyUnknownRevision
            };
            transition(&state, &events, ready_state);
            let _ = events.send(if replacing {
                Event::Resynchronized(value)
            } else {
                Event::Snapshot(value)
            });
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
                notification = client.wait_for_notification() => {
                    if notification.is_err() {
                        break;
                    }
                    transition(&state, &events, ConnectionState::Synchronizing);
                    match client.snapshot().await {
                        Ok(value) => {
                            let value = Arc::new(value);
                            snapshot.send_replace(Some(value.clone()));
                            let ready_state = if client.compatibility().writes_allowed() {
                                ConnectionState::Ready
                            } else {
                                ConnectionState::ReadOnlyUnknownRevision
                            };
                            transition(&state, &events, ready_state);
                            let _ = events.send(Event::Resynchronized(value));
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

fn transition(
    state: &watch::Sender<ConnectionState>,
    events: &broadcast::Sender<Event>,
    next: ConnectionState,
) {
    state.send_replace(next);
    let _ = events.send(Event::StateChanged(next));
}

async fn wait_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        () = sleep(duration) => false,
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
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

    #[test]
    fn reconnect_backoff_is_bounded_and_jittered() {
        let values: Vec<_> = (0..20).map(backoff).collect();
        assert!(values.iter().all(|value| *value <= Duration::from_secs(6)));
        assert!(values[0] >= Duration::from_millis(200));
        assert_ne!(values[4], values[5]);
    }
}
