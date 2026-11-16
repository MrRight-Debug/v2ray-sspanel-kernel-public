# V2Ray SSPanel build

This branch adds native SSPanel UIM synchronization to the VMess inbound and a
small `main/slim` entry point for the current VMess/WebSocket deployment.

## Implemented behavior

- Reads `settings.sspanel` from each tagged VMess inbound.
- Synchronizes dynamic users from `/mod_mu/users`.
- Reads node `alter_id` from `/mod_mu/nodes/{id}/info`.
- Reports upload/download traffic and active client IPs.
- Supports mixed client `alterId` 0 (AEAD) and legacy `alterId` 32.
- Supports IPv4, IPv6 and dual-stack panel/API connections.
- Accepts numeric or string values for SSPanel `node_id`, `refresh` and
  `conn_limit`.
- Enforces the configured per-inbound connection limit.

## Build

The baseline is V2Fly v5.56.0, built with Go 1.27.4:

```bash
bash build/build-v2ray-sspanel.sh
```

The build produces a statically linked Linux amd64 binary in
`/opt/v2ray-sspanel-build/v5.56.0/artifacts`.

## Protocol scope

The runtime entry point only registers the modules required by the current
deployment. The upstream v4 JSON parser still links protocol configuration
packages, so removing unused protocol entries from runtime configuration does
not yet produce a fully protocol-minimal binary.
