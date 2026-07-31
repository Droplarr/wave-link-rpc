use crate::{ChannelId, Error, ErrorKind, MixId, Result, Volume, WaveLinkClient};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::time::{Instant, sleep_until};

const MAX_FADE: Duration = Duration::from_secs(5);
const FADE_STEP: Duration = Duration::from_millis(34);

#[derive(Clone, Debug, PartialEq, Serialize)]
#[non_exhaustive]
pub enum Operation {
    SetChannelVolume {
        channel: ChannelId,
        volume: Volume,
    },
    SetChannelMute {
        channel: ChannelId,
        muted: bool,
    },
    SetChannelMixVolume {
        channel: ChannelId,
        mix: MixId,
        volume: Volume,
    },
    SetChannelMixMute {
        channel: ChannelId,
        mix: MixId,
        muted: bool,
    },
    SetMixVolume {
        mix: MixId,
        volume: Volume,
    },
    SetMixMute {
        mix: MixId,
        muted: bool,
    },
}

impl Operation {
    fn request(&self) -> (&'static str, Value) {
        match self {
            Self::SetChannelVolume { channel, volume } => {
                ("setChannel", json!({"id": channel, "level": volume.get()}))
            }
            Self::SetChannelMute { channel, muted } => {
                ("setChannel", json!({"id": channel, "isMuted": muted}))
            }
            Self::SetChannelMixVolume {
                channel,
                mix,
                volume,
            } => (
                "setChannel",
                json!({"id": channel, "mixes": [{"id": mix, "level": volume.get()}]}),
            ),
            Self::SetChannelMixMute {
                channel,
                mix,
                muted,
            } => (
                "setChannel",
                json!({"id": channel, "mixes": [{"id": mix, "isMuted": muted}]}),
            ),
            Self::SetMixVolume { mix, volume } => {
                ("setMix", json!({"id": mix, "level": volume.get()}))
            }
            Self::SetMixMute { mix, muted } => ("setMix", json!({"id": mix, "isMuted": muted})),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum OperationStatus {
    Succeeded,
    Failed,
    NotAttempted,
}

#[derive(Clone, Debug, Serialize)]
pub struct OperationResult {
    pub operation: Operation,
    pub status: OperationStatus,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct BatchResult {
    pub operations: Vec<OperationResult>,
}

impl BatchResult {
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.operations
            .iter()
            .all(|result| result.status == OperationStatus::Succeeded)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum FadeCurve {
    #[default]
    Perceptual,
    Linear,
}

#[derive(Clone, Copy, Debug)]
pub struct FadeOptions {
    pub duration: Duration,
    pub curve: FadeCurve,
}

impl Default for FadeOptions {
    fn default() -> Self {
        Self {
            duration: Duration::ZERO,
            curve: FadeCurve::Perceptual,
        }
    }
}

impl WaveLinkClient {
    /// Applies one capability-gated mutation through the serialized transport.
    ///
    /// # Errors
    ///
    /// Returns an unsupported-version, protocol, timeout, or transport error.
    pub async fn apply(&self, operation: &Operation) -> Result<()> {
        let (method, params) = operation.request();
        let _: Value = self.call_with_params(method, params).await?;
        Ok(())
    }

    /// Applies an ordered non-atomic batch and stops at the first failure.
    ///
    /// The returned result explicitly marks operations after the failure as not
    /// attempted. Callers should obtain a fresh snapshot after every batch.
    pub async fn apply_batch(&self, operations: Vec<Operation>) -> BatchResult {
        let mut results = Vec::with_capacity(operations.len());
        let mut failed = false;
        for operation in operations {
            if failed {
                results.push(OperationResult {
                    operation,
                    status: OperationStatus::NotAttempted,
                    error: None,
                });
                continue;
            }
            match self.apply(&operation).await {
                Ok(()) => results.push(OperationResult {
                    operation,
                    status: OperationStatus::Succeeded,
                    error: None,
                }),
                Err(error) => {
                    failed = true;
                    results.push(OperationResult {
                        operation,
                        status: OperationStatus::Failed,
                        error: Some(error.to_string()),
                    });
                }
            }
        }
        BatchResult {
            operations: results,
        }
    }

    /// Fades a channel from `start` to `target` at no more than 30 updates per
    /// second, using monotonic elapsed time and an exact final endpoint.
    ///
    /// # Errors
    ///
    /// Returns an invalid-value error for durations above five seconds, or the
    /// first mutation error. A zero-duration fade performs one immediate write.
    pub async fn fade_channel_volume(
        &self,
        channel: ChannelId,
        start: Volume,
        target: Volume,
        options: FadeOptions,
    ) -> Result<()> {
        if options.duration > MAX_FADE {
            return Err(Error::new(
                ErrorKind::InvalidValue,
                "fade duration must be between 0 and 5000 ms",
            ));
        }
        if options.duration.is_zero() {
            return self
                .apply(&Operation::SetChannelVolume {
                    channel,
                    volume: target,
                })
                .await;
        }

        let started = Instant::now();
        let duration_seconds = options.duration.as_secs_f32();
        let step = FADE_STEP;
        let mut deadline = started + step;
        loop {
            sleep_until(deadline).await;
            let elapsed = started.elapsed();
            if elapsed >= options.duration {
                break;
            }
            let progress = (elapsed.as_secs_f32() / duration_seconds).clamp(0.0, 1.0);
            let curved = match options.curve {
                FadeCurve::Linear => progress,
                FadeCurve::Perceptual => progress * progress,
            };
            let value = start.get() + ((target.get() - start.get()) * curved);
            self.apply(&Operation::SetChannelVolume {
                channel: channel.clone(),
                volume: Volume::new(value)?,
            })
            .await?;
            deadline = started + elapsed + step;
        }
        self.apply(&Operation::SetChannelVolume {
            channel,
            volume: target,
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_payloads_are_partial_and_exact() {
        let operation = Operation::SetChannelMixVolume {
            channel: ChannelId::new("channel-1"),
            mix: MixId::new("mix-1"),
            volume: Volume::new(0.5).expect("volume"),
        };
        assert_eq!(
            operation.request(),
            (
                "setChannel",
                json!({"id": "channel-1", "mixes": [{"id": "mix-1", "level": 0.5}]})
            )
        );
    }

    #[test]
    fn fade_defaults_match_stream_deck_behavior() {
        let options = FadeOptions::default();
        assert!(options.duration.is_zero());
        assert_eq!(options.curve, FadeCurve::Perceptual);
    }
}
