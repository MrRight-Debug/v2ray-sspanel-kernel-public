#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
go_root=${GO_ROOT:-/opt/codex-toolchains/go}
artifact_dir=${ARTIFACT_DIR:-/opt/v2ray-sspanel-build/artifacts}

export PATH="$go_root/bin:$PATH"
export GOCACHE=${GOCACHE:-/opt/v2ray-sspanel-build/cache/go-build}
export GOMODCACHE=${GOMODCACHE:-/opt/v2ray-sspanel-build/cache/go-mod}
export GOPROXY=${GOPROXY:-https://goproxy.cn,direct}
export GOSUMDB=${GOSUMDB:-sum.golang.google.cn}

mkdir -p "$GOCACHE" "$GOMODCACHE" "$artifact_dir"
cd "$repo_root"

go test ./proxy/vmess/inbound ./infra/conf/v4 ./app/proxyman/inbound
go vet ./proxy/vmess/inbound ./infra/conf/v4 ./app/proxyman/inbound
go test -race ./proxy/vmess/inbound

CGO_ENABLED=0 \
GOOS=linux \
GOARCH=amd64 \
GOAMD64=v1 \
go build \
  -trimpath \
  -ldflags="-s -w -buildid= -X github.com/v2fly/v2ray-core/v5.build=SSPanel-Go1.26.1-static" \
  -o "$artifact_dir/v2ray-sspanel" \
  ./main/slim

"$artifact_dir/v2ray-sspanel" version
"$artifact_dir/v2ray-sspanel" test -config testdata/acceptance/static-server.json
"$artifact_dir/v2ray-sspanel" test -config testdata/acceptance/sspanel-server.json
"$artifact_dir/v2ray-sspanel" test -config testdata/acceptance/sspanel-server-ipv6.json

sha256sum "$artifact_dir/v2ray-sspanel" | tee "$artifact_dir/v2ray-sspanel.sha256"
