# Contributing

This is an unofficial, independently observed implementation. Do not contribute
proprietary Stream Deck plugin source, code, assets, credentials, or unredacted
machine captures.

Protocol changes require:

1. A description of independently observed behavior and its provenance.
2. Synthetic or redacted language-neutral fixtures.
3. Deterministic mock tests for compatible and failure paths.
4. A compatibility-matrix update.
5. Separate authorization before any live state-changing validation.

Run before opening a pull request:

```text
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
cargo +1.85.0 test --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
cargo package --locked --allow-dirty
```

Use GitHub Issues for actionable bugs and features. Use GitHub Discussions for
protocol observations and unverified compatibility reports.

