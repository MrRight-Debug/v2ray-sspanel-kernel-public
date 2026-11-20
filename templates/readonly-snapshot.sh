#!/usr/bin/env bash
set -euo pipefail

unit=${UNIT:-v2ray-sspanel.service}
pid=$(systemctl show -p MainPID --value "$unit")

printf 'time=%s\n' "$(date -Is)"
printf 'unit=%s\n' "$unit"
printf 'active=%s\n' "$(systemctl is-active "$unit" 2>/dev/null || true)"
printf 'pid=%s\n' "$pid"
printf 'restarts=%s\n' "$(systemctl show -p NRestarts --value "$unit")"
printf 'load=%s\n' "$(awk '{print $1, $2, $3}' /proc/loadavg)"

if [ -n "$pid" ] && [ "$pid" != 0 ] && [ -d "/proc/$pid" ]; then
  printf 'exe=%s\n' "$(readlink -f "/proc/$pid/exe")"
  printf 'sha256=%s\n' "$(sha256sum "/proc/$pid/exe" | awk '{print $1}')"
  printf 'rss_kib=%s\n' "$(awk '/^VmRSS:/{print $2}' "/proc/$pid/status")"
  printf 'threads=%s\n' "$(awk '/^Threads:/{print $2}' "/proc/$pid/status")"
  printf 'fds=%s\n' "$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 | wc -l)"
  printf 'established=%s\n' "$(ss -Htnp state established | awk -v p="pid=$pid," 'index($0,p){n++}END{print n+0}')"
  printf 'listeners=%s\n' "$(ss -Hltnp | awk -v p="pid=$pid," 'index($0,p){n++}END{print n+0}')"
  cgroup=$(systemctl show -p ControlGroup --value "$unit")
  printf 'cgroup_current=%s\n' "$(cat "/sys/fs/cgroup$cgroup/memory.current" 2>/dev/null || printf 0)"
  printf 'cgroup_peak=%s\n' "$(cat "/sys/fs/cgroup$cgroup/memory.peak" 2>/dev/null || printf 0)"
  printf 'cgroup_swap=%s\n' "$(cat "/sys/fs/cgroup$cgroup/memory.swap.current" 2>/dev/null || printf 0)"
fi

