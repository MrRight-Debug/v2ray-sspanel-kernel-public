# V2Ray SSPanel build

This branch adds native, per-inbound SSPanel UIM synchronization to the VMess
inbound and a small `main/slim` entry point for the current VMess/WebSocket
deployment. It does not contain a shared SSPanel manager, shared user snapshot
or shared heartbeat loop.

## Implemented behavior

- Reads `settings.sspanel` from each tagged VMess inbound.
- Every inbound independently synchronizes and heartbeats its own
  `/mod_mu/users?node_id=...` endpoint.
- Keeps independent ETag and Last-Modified state for each inbound; HTTP 304 does
  not decode a user list.
- Reads node `alter_id` from `/mod_mu/nodes/{id}/info`.
- Reports upload/download traffic and active client IPs.
- Supports mixed client `alterId` 0 (AEAD) and legacy `alterId` 32.
- Supports IPv4, IPv6 and dual-stack panel/API connections.
- Accepts numeric or string values for SSPanel `node_id`, `refresh` and
  `conn_limit`.
- Enforces the configured per-inbound connection limit.

## Build

The baseline is V2Fly v5.56.0 with its official module graph, built with
Go 1.27.4:

```bash
ARTIFACT_DIR=/path/to/new/artifacts bash build/build-v2ray-sspanel.sh
```

The build produces a statically linked Linux amd64 binary in
`/opt/v2ray-sspanel-build/v5.56.0/artifacts`.

## Protocol scope

The runtime entry point only registers the modules required by the current
deployment. The upstream v4 JSON parser still links protocol configuration
packages, so removing unused protocol entries from runtime configuration does
not yet produce a fully protocol-minimal binary.
