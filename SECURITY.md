# Security policy

## Reporting

Please report suspected vulnerabilities privately through GitHub's security
advisory interface for this repository. Do not open a public issue for a
credential leak, unsafe write, privacy exposure, or remote-code-execution risk.

## Supported versions

Before 1.0, only the latest published release receives best-effort security
updates. There is no response-time SLA.

## Local interface and privacy

Wave Link exposes an unauthenticated loopback WebSocket. This SDK connects only
to `127.0.0.1` using the port advertised by the local MSIX metadata file. Do not
proxy or expose that socket to another host.

Default diagnostics must not contain raw RPC payloads, usernames, application
or device names, identifiers, icons, or image data. Detailed protocol capture
is sensitive and must be explicitly enabled, redacted before sharing, and never
committed verbatim.

The `unstable-raw` feature bypasses typed compatibility guarantees. It is
disabled by default and must not be used to issue trusted writes.

