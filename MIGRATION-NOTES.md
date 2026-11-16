# V2Fly v5.56.0 SSPanel migration

## Baseline

- V2Fly tag: `v5.56.0`
- V2Fly commit: `99957b4dcc9e3b40d387ba1584a17cebc5238d9f`
- Go toolchain: `go1.27.4 linux/amd64`
- SSPanel patch migrated from commit:
  `be34d060529e907956090fc7acbe80bd23347078`

The patch applied without source conflicts. The migrated patch commit is
created after dependency updates.

## Dependency update

- Ran `go get -u ./...`
- Updated `golang.org/x/mod` to `v0.43.0`
- Ran `go mod tidy`
- `go mod verify`: all modules verified
- `go mod tidy -diff`: no output

Full module lists and changes are stored outside the source tree in
`/opt/v2ray-sspanel-build/v5.56.0/evidence`.

## Explicit dependency exceptions

### `github.com/apernet/quic-go`

- Current: `v0.59.1-0.20260217092621-db4786c77a22`
- Latest advertised: `v0.65.0`
- Not upgraded because the newer tags declare their module path as
  `github.com/quic-go/quic-go`, which does not match the required
  `github.com/apernet/quic-go` path.

### `gvisor.dev/gvisor`

- Current: `v0.0.0-20260218152508-eed1cebf761e`
- Latest advertised: `v0.0.0-20261116061457-89cacadae21e`
- The latest pseudo-version contains conflicting package declarations in
  `pkg/tcpip/stack` and breaks v5.56's TUN packages during `go test ./...`.
- Explicitly retained the v5.56.0 baseline gVisor commit instead of silently
  introducing a broken build.

## Vulnerability scan

`govulncheck@v1.9.0` reports one repository-wide finding:

- `GO-2026-4479`
- `github.com/pion/dtls/v2@v2.2.12`
- No fixed version is currently listed.
- A scan restricted to the production entry point `./main/slim` reports no
  vulnerabilities.

## Known upstream test limitations

The full non-cached suite passes the modified and runtime-critical packages.
Network-dependent upstream tests still fail in this build environment because
they require external DoH, GitHub geodata, or GeoIP endpoints.

Full `go vet ./...` reports three pre-existing upstream findings outside the
SSPanel patch. Targeted vet for the modified packages passes.
