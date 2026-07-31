# Changelog

All notable changes are documented here. Releases follow Semantic Versioning.

## 0.1.1 - 2026-07-31

- Added normalized channel volume, mute, and participating-mix accessors with
  typed `ChannelMixState` values.
- Added lifecycle-safe mutation, ordered batch, and authoritative refresh APIs
  to `SynchronizedClient`.
- Added channel and channel/mix fades with per-target replacement, explicit
  cancellation, disconnect/shutdown cancellation, monotonic timing, and exact
  final endpoints.
- Added post-mutation resynchronization and deterministic mock coverage for
  success, partial failure, read-only revisions, fades, and cancellation.
- Preserved the public 0.1.0 API while extending the synchronized client.

## 0.1.0 - 2026-07-31

- Added Windows MSIX discovery and revision-aware loopback JSON-RPC transport.
- Added tolerant revision-2 application, channel, mix, input, and output reads.
- Added synchronized snapshots, lifecycle states, bounded event subscriptions,
  reconnect/resnapshot behavior, and explicit shutdown.
- Added capability-gated channel/mix volume and mute operations, ordered
  non-atomic batches, and 0–5000 ms volume fades capped below 30 updates/second.
- Added deterministic mock/conformance tests and protected Windows live
  validation.
- Established the compatibility, security, contribution, privacy, and release
  policies for the initial unofficial Rust SDK.
