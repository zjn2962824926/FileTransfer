# 轻量级文件传输工具

基于 **Python Socket + PyQt5** 的局域网/广域网文件传输工具，支持多客户端互传文件与简单聊天。

---

## 目录结构

```
file_transfer/
├── dist
    ├── client.exe     # x64版本客户端
├── server.py          # 服务端
├── client.py          # PyQt5 客户端
├── requirements.txt   # 依赖
└── README.md          # 本文档
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

> 服务端仅使用 Python 标准库，无需额外依赖。

### 2. 启动服务端

```bash
python server.py
# 默认监听 0.0.0.0:9999

# 自定义地址和端口
python server.py --host 0.0.0.0 --port 8888
```

### 3. 启动客户端

```bash
python client.py
```
### 4. 客户端打包
```bash
pyinstaller -w -i file_transfer.ico -F client.py
```
也可直接使用dist/client.exe文件，无需安装py环境

在 GUI 界面中填写：
- **服务器地址**：默认：`127.0.0.1`），若没有服务器地址可尝试使用：`49.235.40.3`进行临时文件传输
- **端口**：与服务端一致（默认 17887）
- **用户名**：在线唯一标识（不能重复）

点击 **连接** 即可上线。

---

## 功能说明

| 功能 | 说明 |
|------|------|
| 多客户端同时在线 | 服务端多线程处理，支持任意数量客户端 |
| 在线用户列表 | 自动实时刷新，显示所有在线用户 |
| 点对点文件传输 | 选中用户 → 发送文件，服务端负责中继转发 |
| 发送进度条 | 大文件实时显示发送百分比 |
| 自动接收文件 | 来文件时自动接收并保存到指定目录 |
| 私聊 / 广播消息 | 双击用户可切换私聊，不选用户则广播 |
| 自定义接收目录 | 点击"更改"按钮选择文件保存位置（默认桌面） |

---

## 通信协议

所有消息采用统一格式：

```
[4字节 header 长度（大端）] + [JSON header bytes] + [可选 payload bytes]
```

### 消息类型一览

| type | 方向 | 说明 |
|------|------|------|
| `register` | Client→Server | 注册用户名 |
| `register_ok` | Server→Client | 注册成功 |
| `get_users` | Client→Server | 请求在线列表 |
| `user_list` | Server→Client | 推送/响应在线列表 |
| `file_request` | Client→Server | 发送文件请求（header 后紧跟二进制数据） |
| `incoming_file` | Server→Client | 通知接收方有文件到来 |
| `file_sent_ok` | Server→Client | 通知发送方文件已中继完成 |
| `file_received_ok` | Server→Client | 通知双方传输完成 |
| `chat` | 双向 | 聊天消息 |
| `ping` / `pong` | 双向 | 心跳检测 |
| `error` | Server→Client | 错误通知 |

---

## 技术架构

```
┌─────────────┐       TCP Socket        ┌─────────────────────┐
│  Client A   │ ───────────────────────▶│                     │
│  (PyQt5)    │ ◀───────────────────────│   Server (Python)   │
└─────────────┘    JSON header +        │   多线程  /  中继   │
                   binary payload       │                     │
┌─────────────┐                         │                     │
│  Client B   │ ◀──────────────────────▶│                     │
│  (PyQt5)    │    文件由服务端中继     └─────────────────────┘
└─────────────┘
```

- **服务端**：每个连接独立线程，文件流式中继（不落盘），内存占用低
- **客户端**：网络收发在独立 QThread 中运行，UI 完全不阻塞
- **文件传输**：`BUFFER_SIZE=64KB` 分块传输，支持大文件

---

## 注意事项

1. 同一局域网内可直接使用服务端机器的内网 IP；跨网络需配置端口映射或使用公网 IP。
2. 服务端**不存储文件**，仅负责实时中继，接收方必须在线。
3. 默认接收目录为**桌面**，可在客户端界面更改。
4. 用户名在同一服务端唯一，重复注册会被拒绝。
