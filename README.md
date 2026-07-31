# wave-link-rpc

An unofficial, community-maintained Rust SDK for Wave Link's loopback RPC
interface. This project is not affiliated with or endorsed by Elgato or Corsair.

The crate is under active development and is not yet published. Rust `0.1`
targets Windows 11 x64 and Wave Link interface revision 2. Unknown revisions are
write-locked; the legacy revision-1 protocol family is unsupported.

The public API uses normalized typed IDs, values, capabilities, and errors.
Transport-specific wire models remain private. Raw RPC access will only be
available through the disabled-by-default `unstable-raw` feature and is outside
the typed API's normal compatibility guarantees.

## Example

```rust,no_run
use wave_link_rpc::{Discovery, WaveLinkClient};

#[tokio::main(flavor = "current_thread")]
async fn main() -> wave_link_rpc::Result<()> {
    let endpoint = Discovery::msix_default()?.discover().await?;
    let client = WaveLinkClient::connect(&endpoint).await?;
    let snapshot = client.snapshot().await?;
    println!("{} channels", snapshot.channels.len());
    client.close().await
}
```

Every connection calls `getApplicationInfo` before exposing capabilities.
Revision 2 enables only the write families validated by the compatibility
matrix. Unknown revisions remain read-only. Disconnected operations are never
queued for replay.

See [COMPATIBILITY.md](COMPATIBILITY.md) for the support matrix and
[SECURITY.md](SECURITY.md) before enabling detailed protocol diagnostics.

## Development

```text
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo doc --no-deps --all-features
cargo package --allow-dirty
```

Committed conformance fixtures must be synthetic and privacy-safe.

Live tests are manual and require a repository-scoped Windows runner labeled
`wave-link-live`. They are read-only unless the protected bounded-write input is
explicitly supplied. Live writes capture current state, limit temporary level
changes to 0.02, and attempt immediate restoration; restoration is not
transactional.

## License

Apache License 2.0.
