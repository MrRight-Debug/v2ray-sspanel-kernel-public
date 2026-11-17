# Shared SSPanel user synchronization

## Design

The manager key is derived from panel URL, authentication key and
`share_group`. One manager owns:

- a shared HTTP transport/TLS pool;
- one polling loop that sends one conditional `/mod_mu/users?node_id=...`
  request for every registered node;
- independent ETag and Last-Modified state for every node;
- an immutable, atomically published user snapshot;
- a shared VMess account metadata cache keyed by UUID and alter ID.

Each inbound keeps its own `node_id` info polling, projected user map,
connection limit, VMess validator, traffic counters and active-IP state.

The per-node users request is required even when the user snapshot is shared:
SSPanel updates `node_heartbeat` and checks node bandwidth in this handler.
ETag `304` therefore still performs the required node-side effects. A 304
response is not decoded. The source node's full response is parsed at most once;
other node responses are streamed to a hash and discarded when they match the
source. Unchanged semantic response hashes do not create a new snapshot
generation. If a node's response hash differs, that node is isolated from the
shared snapshot rather than receiving another node's user permissions.

A completely failed poll keeps the last valid snapshot and retries with
exponential backoff capped at five minutes. Partial failures do not delay the
next normal refresh cycle.

## Compatibility

- Existing v4 JSON is accepted without new mandatory fields.
- `share_group` is optional and defaults to `default`.
- Nodes with different user eligibility scope must use different
  `share_group` values.
- alter ID remains inbound-specific, preserving 0/32 coexistence.

## Isolated measurement

The backend-1 comparison used 300 users, two-second refresh and 32 concurrent
test connections:

- 4 inbound `/users` requests: 72 without sharing, 21 with sharing.
- Shared requests: 20 of 21 received ETag `304`.
- RSS: 1,346,544 KiB without sharing, 1,311,568 KiB with sharing.
- cgroup peak: 1,397,923,840 bytes without sharing,
  1,329,889,280 bytes with sharing.
- First user synchronization: about 73 ms without sharing, 39 ms with sharing.
- Traffic/online-IP reporting succeeded; no panic, fatal or sync errors.

These are single-run A/B results and not production capacity guarantees.
