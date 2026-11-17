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
- Shares one HTTP/TLS client pool and one `/users` polling manager for inbounds
  with the same panel URL, authentication key and `share_group`.
- Uses ETag, Last-Modified and semantic response hashing to avoid rebuilding
  unchanged user snapshots.
- Publishes immutable snapshots atomically. Each inbound still owns its node
  info polling, projection, VMess validator, connection and reporting state.
- Retains the last valid snapshot across panel failures with exponential
  retry backoff.

`share_group` defaults to `default`. Configure a different value when nodes
have different panel user eligibility scopes.

## Build

The baseline is V2Fly v5.56.0, built with Go 1.27.4:

```bash
bash build/build-v2ray-sspanel.sh
```

The build produces statically linked Linux amd64 binaries in
`/opt/v2ray-sspanel-build/v5.56.0-shared/artifacts`:

- `v2ray-sspanel-shared` for production candidates.
- `v2ray-sspanel-shared-pprof`, which enables metrics only when
  `V2RAY_DEBUG_METRICS_ADDR` explicitly points to a loopback address.

The ordinary production build has no pprof listener.

## Protocol scope

The runtime entry point only registers the modules required by the current
deployment. The upstream v4 JSON parser still links protocol configuration
packages, so removing unused protocol entries from runtime configuration does
not yet produce a fully protocol-minimal binary.
