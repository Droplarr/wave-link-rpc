# Releasing

Releases are permanent on crates.io and must use the protected GitHub Actions
workflow.

1. Update `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` in a reviewed pull
   request.
2. Confirm ordinary CI and the authorized Windows live validation pass on the
   exact `main` commit.
3. Review `cargo package --list` and the generated `.crate` archive.
4. Ensure the `crates-io` GitHub environment requires approval and contains the
   least-privilege `CARGO_REGISTRY_TOKEN` secret.
5. Run **Release** with the exact manifest version and confirmation text. First
   use `publish: false` to validate and attest the candidate.
6. After explicit publication approval, rerun with `publish: true`. The workflow
   publishes the immutable crate, creates tag `v<version>`, and creates the
   GitHub Release with the attested `.crate` asset.

Never publish an empty placeholder. If a published version is defective, yank
it and issue a new SemVer version; crates.io versions cannot be overwritten or
deleted.

