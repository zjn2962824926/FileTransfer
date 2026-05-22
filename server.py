"""
文件传输服务端
- 基于 TCP Socket + 多线程
- 支持多客户端注册、在线列表查询、文件中转转发
- 协议格式（所有消息均为 JSON header + 可选二进制 payload）:
  [4字节 header长度(大端)] + [header JSON bytes] + [payload bytes(可选)]
"""

import socket
import threading
import json
import struct
import logging
import time
from typing import Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("server")

HOST = "0.0.0.0"
PORT = 9999
BUFFER_SIZE = 65536  # 64KB 读取块大小


# ─────────────────────────────────────────────
#  协议工具函数
# ─────────────────────────────────────────────

def send_msg(sock: socket.socket, header: dict, payload: bytes = b""):
    """发送消息：4字节长度前缀 + header JSON + payload"""
    header_bytes = json.dumps(header, ensure_ascii=False).encode("utf-8")
    length = struct.pack(">I", len(header_bytes))
    try:
        sock.sendall(length + header_bytes + payload)
    except (BrokenPipeError, ConnectionResetError, OSError):
        pass


def recv_header(sock: socket.socket) -> Optional[dict]:
    """接收并解析 header（不含 payload）"""
    try:
        raw = _recv_exact(sock, 4)
        if not raw:
            return None
        header_len = struct.unpack(">I", raw)[0]
        if header_len == 0 or header_len > 1_048_576:  # 防止恶意超大 header
            return None
        header_bytes = _recv_exact(sock, header_len)
        if not header_bytes:
            return None
        return json.loads(header_bytes.decode("utf-8"))
    except Exception:
        return None


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    """精确接收 n 字节"""
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except OSError:
            return None
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


# ─────────────────────────────────────────────
#  服务端核心
# ─────────────────────────────────────────────

class FileTransferServer:
    def __init__(self, host: str = HOST, port: int = PORT):
        self.host = host
        self.port = port
        self.clients: Dict[str, dict] = {}   # username -> {sock, addr, thread}
        self.lock = threading.Lock()

    # ---------- 启动 ----------

    def start(self):
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(50)
        log.info(f"服务端已启动，监听 {self.host}:{self.port}")

        try:
            while True:
                conn, addr = server_sock.accept()
                log.info(f"新连接来自 {addr}")
                t = threading.Thread(
                    target=self._handle_client, args=(conn, addr), daemon=True
                )
                t.start()
        except KeyboardInterrupt:
            log.info("服务端关闭")
        finally:
            server_sock.close()

    # ---------- 客户端处理线程 ----------

    def _handle_client(self, conn: socket.socket, addr):
        username = None
        try:
            # 第一条消息必须是 register
            header = recv_header(conn)
            if not header or header.get("type") != "register":
                send_msg(conn, {"type": "error", "msg": "请先注册用户名"})
                conn.close()
                return

            username = header.get("username", "").strip()
            if not username:
                send_msg(conn, {"type": "error", "msg": "用户名不能为空"})
                conn.close()
                return

            with self.lock:
                if username in self.clients:
                    send_msg(conn, {"type": "error", "msg": f"用户名 '{username}' 已被占用"})
                    conn.close()
                    return
                self.clients[username] = {"sock": conn, "addr": addr}

            log.info(f"用户 '{username}' 注册成功，当前在线：{list(self.clients.keys())}")
            send_msg(conn, {"type": "register_ok", "msg": f"欢迎 {username}！"})
            self._broadcast_user_list()

            # 主循环
            while True:
                header = recv_header(conn)
                if not header:
                    break
                self._dispatch(conn, username, header)

        except Exception as e:
            log.exception(f"处理客户端 {addr} 时出错: {e}")
        finally:
            if username:
                with self.lock:
                    self.clients.pop(username, None)
                log.info(f"用户 '{username}' 断线，当前在线：{list(self.clients.keys())}")
                self._broadcast_user_list()
            try:
                conn.close()
            except Exception:
                pass

    # ---------- 消息分发 ----------

    def _dispatch(self, conn: socket.socket, sender: str, header: dict):
        msg_type = header.get("type")

        if msg_type == "get_users":
            self._send_user_list(conn)

        elif msg_type == "file_request":
            self._handle_file_request(conn, sender, header)

        elif msg_type == "chat":
            self._handle_chat(sender, header)

        elif msg_type == "ping":
            send_msg(conn, {"type": "pong"})

        else:
            send_msg(conn, {"type": "error", "msg": f"未知消息类型: {msg_type}"})

    # ---------- 文件传输处理 ----------

    def _handle_file_request(self, sender_conn: socket.socket, sender: str, header: dict):
        """
        客户端先发送 file_request header，然后立即跟上 payload(文件二进制)
        header 示例:
        {
            "type": "file_request",
            "to": "bob",
            "filename": "photo.jpg",
            "filesize": 102400
        }
        """
        to = header.get("to", "").strip()
        filename = header.get("filename", "unknown")
        filesize = header.get("filesize", 0)

        if not to or to == sender:
            send_msg(sender_conn, {"type": "error", "msg": "无效的目标用户"})
            return

        with self.lock:
            target = self.clients.get(to)

        if not target:
            send_msg(sender_conn, {"type": "error", "msg": f"用户 '{to}' 不在线"})
            return

        target_sock: socket.socket = target["sock"]

        # 1. 通知目标端即将收到文件
        send_msg(target_sock, {
            "type": "incoming_file",
            "from": sender,
            "filename": filename,
            "filesize": filesize,
        })

        # 2. 流式中继数据
        log.info(f"中继文件 '{filename}' ({filesize}B) 从 '{sender}' 到 '{to}'")
        remaining = filesize
        try:
            while remaining > 0:
                chunk_size = min(BUFFER_SIZE, remaining)
                chunk = _recv_exact(sender_conn, chunk_size)
                if not chunk:
                    break
                target_sock.sendall(chunk)
                remaining -= len(chunk)
        except Exception as e:
            log.error(f"文件中继失败: {e}")
            send_msg(sender_conn, {"type": "error", "msg": "文件传输中继失败"})
            return

        log.info(f"文件 '{filename}' 中继完成")
        send_msg(sender_conn, {"type": "file_sent_ok", "filename": filename, "to": to})
        send_msg(target_sock, {"type": "file_received_ok", "filename": filename, "from": sender})

    # ---------- 聊天消息 ----------

    def _handle_chat(self, sender: str, header: dict):
        to = header.get("to", "").strip()
        content = header.get("content", "")

        with self.lock:
            if to:
                target = self.clients.get(to)
                if target:
                    send_msg(target["sock"], {
                        "type": "chat",
                        "from": sender,
                        "content": content,
                        "ts": time.strftime("%H:%M:%S"),
                    })
            else:
                # 广播
                for uname, info in self.clients.items():
                    if uname != sender:
                        send_msg(info["sock"], {
                            "type": "chat",
                            "from": sender,
                            "content": content,
                            "ts": time.strftime("%H:%M:%S"),
                        })

    # ---------- 用户列表 ----------

    def _send_user_list(self, conn: socket.socket):
        with self.lock:
            users = list(self.clients.keys())
        send_msg(conn, {"type": "user_list", "users": users})

    def _broadcast_user_list(self):
        with self.lock:
            users = list(self.clients.keys())
            socks = [info["sock"] for info in self.clients.values()]
        for s in socks:
            send_msg(s, {"type": "user_list", "users": users})


# ─────────────────────────────────────────────
#  入口
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="文件传输服务端")
    parser.add_argument("--host", default=HOST, help="监听地址")
    parser.add_argument("--port", type=int, default=PORT, help="监听端口")
    args = parser.parse_args()

    srv = FileTransferServer(host=args.host, port=args.port)
    srv.start()
