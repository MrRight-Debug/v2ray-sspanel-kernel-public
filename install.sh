#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

readonly REPO_SLUG="MrRight-Debug/v2ray-sspanel-kernel-public"
readonly RELEASE_TAG="v5.56.0-sspanel-3d0e6b54"
readonly BINARY_SHA256="3d0e6b54b8d6480ccf8e38666202356da05fbea12d0fde30e529fa1478d737a0"
readonly SERVICE_NAME="v2ray-sspanel.service"
readonly INSTALL_DIR="/opt/v2ray-sspanel"
readonly CONFIG_DIR="/etc/v2ray-sspanel"
readonly TARGET_BINARY="${INSTALL_DIR}/v2ray-sspanel"
readonly TARGET_CONFIG="${CONFIG_DIR}/server.json"
readonly UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}"
readonly DOWNLOAD_URL="https://github.com/${REPO_SLUG}/releases/download/${RELEASE_TAG}/v2ray-sspanel"

config_source=""
binary_source=""
allow_upgrade=0

usage() {
  cat <<'EOF'
Usage:
  sudo bash install.sh
  sudo bash install.sh --config /path/to/server.json
  sudo bash install.sh --config /path/to/server.json --upgrade
  sudo bash install.sh --config /path/to/server.json --binary /path/to/v2ray-sspanel

The script installs the verified static V2Ray SSPanel kernel, a systemd unit,
and a validated v4 JSON configuration. Without --config it asks interactively
for one panel node and keeps the panel key off the terminal.
EOF
}

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      [ "$#" -ge 2 ] || die "--config requires a path"
      config_source=$2
      shift 2
      ;;
    --binary)
      [ "$#" -ge 2 ] || die "--binary requires a path"
      binary_source=$2
      shift 2
      ;;
    --upgrade)
      allow_upgrade=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1"
      ;;
  esac
done

[ "$(id -u)" -eq 0 ] || die "run as root"
[ "$(uname -s)" = Linux ] || die "Linux is required"
[ "$(uname -m)" = x86_64 ] || die "this release supports x86_64 only"
for command in curl python3 sha256sum systemctl install ss; do
  command -v "$command" >/dev/null || die "missing required command: $command"
done

if [ -e "$UNIT_PATH" ] || [ -e "$TARGET_BINARY" ]; then
  [ "$allow_upgrade" -eq 1 ] || die "existing installation detected; use --upgrade explicitly"
fi

if [ -n "$config_source" ]; then
  [ -f "$config_source" ] || die "configuration not found: $config_source"
fi

work_dir=$(mktemp -d /tmp/v2ray-sspanel-install.XXXXXX)
backup_dir=""
trap 'rm -rf "$work_dir"' EXIT

validate_integer() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

generate_config() {
  [ -r /dev/tty ] || die "interactive setup needs a terminal; rerun with --config"

  local panel_url panel_key node_id listen_addr listen_port ws_path refresh conn_limit
  printf 'SSPanel URL: '
  read -r -u 3 panel_url
  printf 'SSPanel key (hidden): '
  read -r -s -u 3 panel_key
  printf '\nNode ID: '
  read -r -u 3 node_id
  printf 'Listen address [127.0.0.1]: '
  read -r -u 3 listen_addr
  listen_addr=${listen_addr:-127.0.0.1}
  printf 'Listen port [10086]: '
  read -r -u 3 listen_port
  listen_port=${listen_port:-10086}
  printf 'WebSocket path [/replace-with-random-path]: '
  read -r -u 3 ws_path
  ws_path=${ws_path:-/replace-with-random-path}
  printf 'Refresh seconds [60]: '
  read -r -u 3 refresh
  refresh=${refresh:-60}
  printf 'Connection limit; 0=unlimited [0]: '
  read -r -u 3 conn_limit
  conn_limit=${conn_limit:-0}

  [[ "$panel_url" =~ ^https?:// ]] || die "panel URL must start with http:// or https://"
  [ -n "$panel_key" ] || die "panel key must not be empty"
  validate_integer "$node_id" && [ "$node_id" -gt 0 ] || die "node ID must be positive"
  validate_integer "$listen_port" && [ "$listen_port" -ge 1 ] && [ "$listen_port" -le 65535 ] ||
    die "listen port is invalid"
  validate_integer "$refresh" && [ "$refresh" -gt 0 ] || die "refresh must be positive"
  validate_integer "$conn_limit" || die "connection limit must be numeric"
  [[ "$ws_path" == /* ]] || die "WebSocket path must start with /"

  printf '%s\0' "$panel_url" "$panel_key" "$node_id" "$listen_addr" "$listen_port" \
    "$ws_path" "$refresh" "$conn_limit" |
    python3 -c '
import json
import sys

values = [item.decode("utf-8") for item in sys.stdin.buffer.read().split(b"\0")]
panel_url, panel_key, node_id, listen_addr, listen_port, ws_path, refresh, conn_limit = values[:8]
config = {
    "log": {"loglevel": "warning"},
    "inbounds": [{
        "tag": "sspanel-node",
        "listen": listen_addr,
        "port": int(listen_port),
        "protocol": "vmess",
        "settings": {"sspanel": {
            "url": panel_url,
            "key": panel_key,
            "node_id": node_id,
            "refresh": int(refresh),
            "conn_limit": int(conn_limit),
        }},
        "streamSettings": {
            "network": "ws",
            "wsSettings": {"path": ws_path},
        },
    }],
    "outbounds": [{"tag": "direct", "protocol": "freedom"}],
    "stats": {},
    "policy": {
        "system": {
            "statsInboundUplink": True,
            "statsInboundDownlink": True,
            "statsOutboundUplink": True,
            "statsOutboundDownlink": True,
        },
        "levels": {"0": {
            "statsUserUplink": True,
            "statsUserDownlink": True,
        }},
    },
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(config, handle, indent=2)
    handle.write("\n")
' "$work_dir/server.json"
  chmod 600 "$work_dir/server.json"
  config_source="$work_dir/server.json"
}

if [ -z "$config_source" ]; then
  exec 3<>/dev/tty
  generate_config
fi

candidate="$work_dir/v2ray-sspanel"
if [ -n "$binary_source" ]; then
  [ -f "$binary_source" ] || die "binary not found: $binary_source"
  install -m 0700 "$binary_source" "$candidate"
else
  curl --fail --location --silent --show-error --retry 3 \
    --proto '=https' --tlsv1.2 "$DOWNLOAD_URL" -o "$candidate"
fi

printf '%s  %s\n' "$BINARY_SHA256" "$candidate" | sha256sum --check --status ||
  die "binary SHA-256 verification failed"
chmod 700 "$candidate"
"$candidate" test -config "$config_source"

if [ -e "$TARGET_BINARY" ] || [ -e "$TARGET_CONFIG" ] || [ -e "$UNIT_PATH" ]; then
  backup_dir="/var/backups/v2ray-sspanel/install-$(date -u +%Y%m%dT%H%M%SZ)"
  install -d -m 0700 "$backup_dir"
  [ ! -e "$TARGET_BINARY" ] || cp -a "$TARGET_BINARY" "$backup_dir/v2ray-sspanel"
  [ ! -e "$TARGET_CONFIG" ] || cp -a "$TARGET_CONFIG" "$backup_dir/server.json"
  [ ! -e "$UNIT_PATH" ] || cp -a "$UNIT_PATH" "$backup_dir/v2ray-sspanel.service"
  (
    cd "$backup_dir"
    sha256sum v2ray-sspanel server.json v2ray-sspanel.service 2>/dev/null > SHA256SUMS || true
  )
fi

install -d -m 0755 "$INSTALL_DIR"
install -d -m 0700 "$CONFIG_DIR"
if [ -z "$binary_source" ]; then
  install -m 0755 "$candidate" "${TARGET_BINARY}.new"
  mv -f "${TARGET_BINARY}.new" "$TARGET_BINARY"
else
  install -m 0755 "$candidate" "${TARGET_BINARY}.new"
  mv -f "${TARGET_BINARY}.new" "$TARGET_BINARY"
fi
install -m 0600 "$config_source" "$TARGET_CONFIG"

if [ ! -e "$UNIT_PATH" ]; then
  cat >"${UNIT_PATH}.new" <<'EOF'
[Unit]
Description=V2Ray SSPanel VMess service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=-/etc/v2ray-sspanel/config.env
ExecStart=/opt/v2ray-sspanel/v2ray-sspanel run -config /etc/v2ray-sspanel/server.json
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576
MemoryAccounting=yes
TasksAccounting=yes

[Install]
WantedBy=multi-user.target
EOF
  chmod 644 "${UNIT_PATH}.new"
  mv -f "${UNIT_PATH}.new" "$UNIT_PATH"
fi

systemctl daemon-reload
systemctl enable "$SERVICE_NAME" >/dev/null
systemctl restart "$SERVICE_NAME"

healthy=0
for _ in $(seq 1 30); do
  state=$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)
  pid=$(systemctl show -p MainPID --value "$SERVICE_NAME")
  listeners=$(ss -Hltnp | awk -v p="pid=${pid}," 'index($0,p){n++}END{print n+0}')
  running_sha=""
  [ "$pid" = 0 ] || running_sha=$(sha256sum "/proc/${pid}/exe" 2>/dev/null | awk '{print $1}')
  if [ "$state" = active ] && [ "$listeners" -gt 0 ] && [ "$running_sha" = "$BINARY_SHA256" ]; then
    healthy=1
    break
  fi
  sleep 1
done

if [ "$healthy" -ne 1 ]; then
  printf 'health check failed\n' >&2
  journalctl -u "$SERVICE_NAME" -n 40 --no-pager -o cat >&2 || true
  systemctl stop "$SERVICE_NAME" >/dev/null 2>&1 || true
  if [ -n "$backup_dir" ] && [ -e "$backup_dir/v2ray-sspanel" ]; then
    install -m 0755 "$backup_dir/v2ray-sspanel" "$TARGET_BINARY"
    [ ! -e "$backup_dir/server.json" ] || install -m 0600 "$backup_dir/server.json" "$TARGET_CONFIG"
    [ ! -e "$backup_dir/v2ray-sspanel.service" ] || install -m 0644 "$backup_dir/v2ray-sspanel.service" "$UNIT_PATH"
    systemctl daemon-reload
    systemctl restart "$SERVICE_NAME" || true
    printf 'existing installation restored from %s\n' "$backup_dir" >&2
  fi
  exit 1
fi

printf '\ninstalled successfully\n'
printf 'service: %s\n' "$SERVICE_NAME"
printf 'binary:  %s\n' "$TARGET_BINARY"
printf 'config:  %s (mode 0600)\n' "$TARGET_CONFIG"
if [ -n "$backup_dir" ]; then
  printf 'backup:  %s\n' "$backup_dir"
fi
printf '\nNext steps:\n'
printf '1. Put the listener behind Nginx/Caddy TLS WebSocket routing.\n'
printf '2. Verify the matching node heartbeat in SSPanel.\n'
printf '3. Test alterId 0 and, if required, alterId 32 clients.\n'
