#!/usr/bin/env bash
set -euo pipefail

candidate=${1:?usage: deploy-binary.sh CANDIDATE EXPECTED_SHA256 SERVICE CONFIG TARGET_BINARY}
expected_sha256=${2:?missing expected SHA-256}
service=${3:?missing service name}
config=${4:?missing config path}
target=${5:?missing target binary}
temporary="${target}.deploy-${expected_sha256:0:8}.$$"

cleanup() {
  [ ! -e "$temporary" ] || unlink "$temporary"
}
trap cleanup EXIT

test -x "$candidate"
test -f "$config"
test -x "$target"
test "$(sha256sum "$candidate" | awk '{print $1}')" = "$expected_sha256"

"$candidate" test -config "$config"
install -m 0755 -o root -g root "$candidate" "$temporary"
test "$(sha256sum "$temporary" | awk '{print $1}')" = "$expected_sha256"
mv -f "$temporary" "$target"
systemctl restart "$service"

for _ in $(seq 1 30); do
  state=$(systemctl is-active "$service" 2>/dev/null || true)
  pid=$(systemctl show -p MainPID --value "$service")
  if [ "$state" = active ] && [ -n "$pid" ] && [ "$pid" != 0 ] &&
     [ -d "/proc/$pid" ] &&
     [ "$(sha256sum "/proc/$pid/exe" | awk '{print $1}')" = "$expected_sha256" ]; then
    printf 'deployed service=%s pid=%s sha256=%s\n' "$service" "$pid" "$expected_sha256"
    exit 0
  fi
  sleep 1
done

printf 'deployment health check failed for %s\n' "$service" >&2
exit 1

