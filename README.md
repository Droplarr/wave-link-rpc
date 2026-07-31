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

## Development

```text
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo doc --no-deps --all-features
cargo package --allow-dirty
```

Committed conformance fixtures must be synthetic and privacy-safe.

## License

Apache License 2.0.

