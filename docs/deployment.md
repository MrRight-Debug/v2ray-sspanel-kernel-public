# 完整部署流程

## 1. 适用范围

本文用于 Ubuntu/Debian x86_64 主机部署一个或多个后接入 SSPanel 的 VMess/WebSocket 入站。最终二进制已经是静态链接，不需要在目标机安装 Go。

部署前准备：

- SSPanel 面板地址
- 面板 API key
- 与当前主机对应的 node_id
- 每个节点对应的 WebSocket 路径
- 已配置好的 TLS 终止层，例如 Nginx、Caddy 或其他反向代理

不要把面板 key 发到聊天、公开日志或代码仓库。

## 2. alterId 与用户隔离

最终服务端以 32 个 legacy alter ID 建立账户，可以同时接受 `alterId: 0` 的 AEAD 主身份和 `alterId: 32` 的 legacy 客户端。

每个入站都独立请求自己的 `/mod_mu/users?node_id=...`，因此节点心跳不会被共享。只有不同入站从面板获得完全相同的用户集合、UUID、alterId 和 level 时，才共享不可变认证索引；节点用户计数、流量、在线 IP 和连接限制仍按入站独立。

不同 node_id 在面板上属于不同 class/group 时，只要最终返回的认证用户集合不同，就会自动拆成不同认证桶，不会错误共享权限。

## 3. 一键安装

公开安装仓库无需登录 GitHub：

```bash
curl -fsSL -o /tmp/install-v2ray-sspanel.sh \
  https://raw.githubusercontent.com/MrRight-Debug/v2ray-sspanel-kernel-public/main/install.sh
sudo bash /tmp/install-v2ray-sspanel.sh
```

脚本会交互询问面板 URL、API key、node_id、监听地址、WebSocket 路径、刷新周期和连接限制。

非交互安装时：

```bash
sudo bash /tmp/install-v2ray-sspanel.sh \
  --config /root/server.json
```

升级安装时：

```bash
sudo bash /tmp/install-v2ray-sspanel.sh \
  --config /root/server.json \
  --upgrade
```

脚本会：

1. 下载固定版本的静态二进制。
2. 校验 SHA-256：
   `3d0e6b54b8d6480ccf8e38666202356da05fbea12d0fde30e529fa1478d737a0`
3. 用新二进制校验现有配置。
4. 将二进制安装到 `/opt/v2ray-sspanel/`。
5. 将配置安装为 `/etc/v2ray-sspanel/server.json`，权限 `0600`。
6. 安装并启动 `v2ray-sspanel.service`。
7. 核对服务、监听端口和实际运行文件哈希。
8. 失败时尝试恢复安装前二进制、配置和 unit。

## 4. 手工配置

单节点最小结构：

```json
{
  "tag": "node-a",
  "listen": "127.0.0.1",
  "port": 10086,
  "protocol": "vmess",
  "settings": {
    "sspanel": {
      "url": "https://panel.example.invalid",
      "key": "REPLACE_WITH_PANEL_KEY",
      "node_id": "NODE_ID",
      "refresh": 60,
      "conn_limit": 0
    }
  },
  "streamSettings": {
    "network": "ws",
    "wsSettings": {"path": "/REPLACE_WITH_WS_PATH"}
  }
}
```

完整多入站示例见 `templates/server.example.json`。

## 5. systemd

模板见 `templates/v2ray-sspanel.service`。

常用检查：

```bash
systemctl status v2ray-sspanel.service
systemctl is-active v2ray-sspanel.service
journalctl -u v2ray-sspanel.service -n 100 --no-pager
```

## 6. Nginx WebSocket 转发

推荐只让 V2Ray 监听 `127.0.0.1`，由 Nginx 负责公网 TLS：

```nginx
location /REPLACE_WITH_WS_PATH {
    proxy_pass http://127.0.0.1:10086;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 300s;
}
```

路径、端口和上游地址必须与 V2Ray 配置一致。修改 Nginx 前先执行 `nginx -t` 并保留现有配置备份。

## 7. SSPanel 验收

1. 确认服务处于 active。
2. 确认目标端口处于 LISTEN。
3. 在面板确认对应 node_id 的 heartbeat 在两个刷新周期内更新。
4. 确认 `/users` 请求正常，304 或 200 均可。
5. 用 `alterId: 0` 客户端发送实际请求。
6. 如仍需 legacy 客户端，用 `alterId: 32` 再测一次。
7. 检查面板在线 IP 和流量是否增长。
8. 检查日志中没有 panic、fatal、sync failure 或持续 invalid-user。

## 8. 升级与回滚

升级前保存当前二进制、unit、配置、权限和哈希。使用 `--upgrade` 时，脚本默认将现有文件放入独立备份目录。

新二进制必须先通过：

```bash
/path/to/new/v2ray-sspanel test -config /etc/v2ray-sspanel/server.json
```

回滚后要重新执行配置校验、启动、端口、节点心跳和实际转发检查。

## 9. 监测

免登录监测脚本：

```bash
curl -fsSL -o /tmp/v2ray-readonly-snapshot.sh \
  https://raw.githubusercontent.com/MrRight-Debug/v2ray-sspanel-kernel-public/main/templates/readonly-snapshot.sh
bash /tmp/v2ray-readonly-snapshot.sh
```

该脚本只读当前主机，不包含 SSH 凭据或面板凭据。

## 10. 常见问题

### 面板不更新心跳

确认每个入站都配置了独立 node_id，并确认 `/users` 请求带有对应 `node_id`。不能用一个节点的共享请求代替其他节点心跳。

### alterId 32 客户端无法连接

确认面板节点返回的 `alter_id` 为 32，或服务端配置允许 legacy VMess。成功启动时出现一次 non-AEAD warning 不一定是失败，但持续出现 invalid-user 需要排查。

### 内存持续上涨

同时观察 RSS、cgroup current/peak、swap、连接数和用户数。不要仅凭一次 RSS 峰值判断泄漏；短时锯齿可能来自 Go 分配器。必要时做可控 A/B 后再评估 GOGC。

### 多节点权限是否共享

实现只比较实际返回的认证用户集合。集合不同时不共享认证索引；每节点连接限制和流量归属仍独立。
