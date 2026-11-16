package slim

import (
	// Mandatory core features.
	_ "github.com/v2fly/v2ray-core/v5/app/dispatcher"
	_ "github.com/v2fly/v2ray-core/v5/app/proxyman/inbound"
	_ "github.com/v2fly/v2ray-core/v5/app/proxyman/outbound"

	// Features required by the current SSPanel VMess/WebSocket deployment.
	_ "github.com/v2fly/v2ray-core/v5/app/dns"
	_ "github.com/v2fly/v2ray-core/v5/app/log"
	_ "github.com/v2fly/v2ray-core/v5/app/policy"
	_ "github.com/v2fly/v2ray-core/v5/app/router"
	_ "github.com/v2fly/v2ray-core/v5/app/stats"

	// Routing initialization requires a geodata loader even when rules only use
	// literal domains and IP CIDRs.
	_ "github.com/v2fly/v2ray-core/v5/infra/conf/geodata/memconservative"
	_ "github.com/v2fly/v2ray-core/v5/infra/conf/geodata/standard"

	_ "github.com/v2fly/v2ray-core/v5/transport/internet/tagged/taggedimpl"

	// Current inbound and outbound protocols.
	_ "github.com/v2fly/v2ray-core/v5/proxy/blackhole"
	_ "github.com/v2fly/v2ray-core/v5/proxy/freedom"
	_ "github.com/v2fly/v2ray-core/v5/proxy/vmess/inbound"

	// Current transports: TCP, UDP and server-side WebSocket.
	_ "github.com/v2fly/v2ray-core/v5/transport/internet/tcp"
	_ "github.com/v2fly/v2ray-core/v5/transport/internet/udp"
	_ "github.com/v2fly/v2ray-core/v5/transport/internet/websocket"

	// Existing JSON/TOML/YAML configuration support and v5 config parsing.
	_ "github.com/v2fly/v2ray-core/v5/infra/conf/v5cfg"
	_ "github.com/v2fly/v2ray-core/v5/main/formats"
)
