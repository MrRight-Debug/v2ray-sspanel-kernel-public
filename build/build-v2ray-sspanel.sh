#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
go_root=${GO_ROOT:-/opt/codex-toolchains/go1.27.4/go}
artifact_dir=${ARTIFACT_DIR:?ARTIFACT_DIR must be set to a new directory}
evidence_dir=${EVIDENCE_DIR:-$(dirname "$artifact_dir")/evidence}

export PATH="$go_root/bin:$PATH"
export GOTOOLCHAIN=local
export GOCACHE=${GOCACHE:-/opt/v2ray-sspanel-build/cache/go1.27-build}
export GOMODCACHE=${GOMODCACHE:-/opt/v2ray-sspanel-build/cache/go1.27-mod}
export GOPROXY=${GOPROXY:-https://goproxy.cn,direct}
export GOSUMDB=${GOSUMDB:-sum.golang.google.cn}

mkdir -p "$GOCACHE" "$GOMODCACHE" "$artifact_dir" "$evidence_dir"
cd "$repo_root"

git diff --check 2>&1 | tee "$evidence_dir/git-diff-check.log"
go mod verify 2>&1 | tee "$evidence_dir/go-mod-verify.log"
if ! go mod tidy -diff >"$evidence_dir/go-mod-tidy.diff" 2>&1; then
  cat "$evidence_dir/go-mod-tidy.diff" >&2
  exit 1
fi
test ! -s "$evidence_dir/go-mod-tidy.diff"
go test -count=1 ./proxy/vmess/inbound ./infra/conf/v4 ./app/proxyman/inbound 2>&1 |
  tee "$evidence_dir/targeted-test.log"
go vet ./proxy/vmess/inbound ./infra/conf/v4 ./app/proxyman/inbound 2>&1 |
  tee "$evidence_dir/targeted-vet.log"
go test -race -count=1 ./proxy/vmess/inbound 2>&1 | tee "$evidence_dir/race-test.log"

CGO_ENABLED=0 \
GOOS=linux \
GOARCH=amd64 \
GOAMD64=v1 \
go build \
  -trimpath \
  -ldflags="-s -w -buildid= -X github.com/v2fly/v2ray-core/v5.build=SSPanel-Go1.27.4-static" \
  -o "$artifact_dir/v2ray-sspanel" \
  ./main/slim

"$artifact_dir/v2ray-sspanel" version
"$artifact_dir/v2ray-sspanel" test -config testdata/acceptance/static-server.json
"$artifact_dir/v2ray-sspanel" test -config testdata/acceptance/sspanel-server.json
"$artifact_dir/v2ray-sspanel" test -config testdata/acceptance/sspanel-server-ipv6.json
file "$artifact_dir/v2ray-sspanel" | tee "$evidence_dir/file.txt"
ldd "$artifact_dir/v2ray-sspanel" >"$evidence_dir/ldd.txt" 2>&1 || true
grep -q "statically linked" "$evidence_dir/file.txt"
grep -q "not a dynamic executable" "$evidence_dir/ldd.txt"

{
  echo "commit=$(git rev-parse HEAD)"
  echo "branch=$(git rev-parse --abbrev-ref HEAD)"
  echo "go=$(go version)"
  echo "source_status=$(git status --porcelain | wc -l) changed files"
  sha256sum "$artifact_dir/v2ray-sspanel"
} | tee "$artifact_dir/v2ray-sspanel.sha256"
