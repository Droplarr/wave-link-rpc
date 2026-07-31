# Compatibility policy

| SDK | Platform | Wave Link interface revision | Reads | Writes |
| --- | --- | --- | --- | --- |
| Rust 0.1 | Windows 11 x64 | 2 | Application, channels, mixes, inputs, outputs | Channel/mix volume and mute; per-mix channel state |
| Rust 0.1 | Windows 11 x64 | Unknown | Validated structural reads only | Locked |
| Rust 0.1 | Any | 1 | Unsupported | Unsupported |

Application versions and builds are diagnostic metadata. Compatibility is
routed by `interfaceRevision`; additive fields within revision 2 are tolerated.

Output routing is represented as a future capability but remains disabled until
an interface-revision-2 host with output devices passes the separately
authorized live round-trip matrix. Effects, DSP, Gain Lock, input gain,
main-output selection, application routing, and device-specific controls are
not part of Rust 0.1.

Support for a new revision requires redacted structural fixtures, deterministic
mock coverage, capability review, and separately authorized live validation
before any write is enabled.

