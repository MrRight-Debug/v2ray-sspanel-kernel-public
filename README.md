# V2Ray / SSPanel 内核安装与部署

面向 SSPanel 的 V2Fly VMess 后端内核，固定版本：

- V2Fly `v5.56.0`
- Go `1.27.4`
- Linux amd64 静态链接
- SHA-256 `3d0e6b54b8d6480ccf8e38666202356da05fbea12d0fde30e529fa1478d737a0`

本仓库可直接在未登录 GitHub 的环境下载。安装脚本不需要 GitHub CLI 或 API Token。

## 特性

- VMess + WebSocket
- IPv4 / IPv6
- `alterId: 0` 与 `alterId: 32`
- SSPanel 用户同步、流量与在线 IP 上报
- 每个 node_id 独立 `/users` 心跳
- ETag / Last-Modified / 响应正文哈希去重
- 相同用户集合共享不可变认证索引，节点流量和连接限制仍独立
- systemd 服务
- 安装前配置校验、失败自动尝试恢复

## 一键安装

Linux amd64、systemd、root 权限：

```bash
curl -fsSL -o /tmp/install-v2ray-sspanel.sh \
  https://raw.githubusercontent.com/MrRight-Debug/v2ray-sspanel-kernel-public/main/install.sh
sudo bash /tmp/install-v2ray-sspanel.sh
```

使用已有配置：

```bash
sudo bash /tmp/install-v2ray-sspanel.sh \
  --config /root/server.json
```

升级已有安装：

```bash
sudo bash /tmp/install-v2ray-sspanel.sh \
  --config /root/server.json \
  --upgrade
```

脚本会下载固定二进制并校验 SHA-256，然后安装：

- 二进制：`/opt/v2ray-sspanel/v2ray-sspanel`
- 配置：`/etc/v2ray-sspanel/server.json`
- 服务：`v2ray-sspanel.service`

面板 key 不会显示到终端。配置文件权限为 `0600`。

## 完整教程

见 [docs/deployment.md](docs/deployment.md)，包含：

- 面板节点准备
- 多节点和 WebSocket 配置
- Nginx TLS/WebSocket 示例
- systemd 操作
- 用户同步、心跳、流量和在线 IP 验收
- alterId 0/32 测试
- 升级和回滚
- 常见问题

## 配置示例

[templates/server.example.json](templates/server.example.json) 是只有占位符的多入站示例。

替代环境变量也受支持：

```text
V2RAY_SSPANEL_URL
V2RAY_SSPANEL_KEY
V2RAY_SSPANEL_NODE_ID
V2RAY_SSPANEL_REFRESH
V2RAY_SSPANEL_CONN_LIMIT
```

注意：使用环境变量时仍要保护环境文件和进程权限。

## 验证

```bash
systemctl is-active v2ray-sspanel.service
ss -ltnp
journalctl -u v2ray-sspanel.service -n 100 --no-pager
```

随后在 SSPanel 中确认对应 node_id 的心跳、在线用户、流量和在线 IP。只检查进程存活不足以证明业务正常。

## 只读监测

```bash
curl -fsSL -o /tmp/v2ray-readonly-snapshot.sh \
  https://raw.githubusercontent.com/MrRight-Debug/v2ray-sspanel-kernel-public/main/templates/readonly-snapshot.sh
bash /tmp/v2ray-readonly-snapshot.sh
```

## 源码

完整可构建源码位于本仓库的 `source-final` 分支，包含上游历史和最终
`main/slim`、SSPanel 实现、`go.mod/go.sum` 与测试代码。

```bash
git clone --branch source-final \
  https://github.com/MrRight-Debug/v2ray-sspanel-kernel-public.git
```

## 安全边界

本仓库不包含生产凭据、真实节点 UUID 或实际生产配置。上游已有的自签名测试证书和示例 UUID 仅用于单元测试。

