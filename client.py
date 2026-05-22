"""
文件传输客户端 - PyQt5 GUI
- 连接服务端、注册用户名
- 查看在线用户列表
- 向指定用户发送任意文件（显示进度条）
- 自动接收来自其他用户的文件（显示保存对话框）
- 内置简单聊天功能
"""

import sys
import os
import json
import struct
import socket
import threading
import time
import logging
from typing import Optional

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QListWidget, QTextEdit,
    QFileDialog, QProgressDialog, QMessageBox, QSplitter,
    QGroupBox, QStatusBar, QFrame, QTabWidget, QListWidgetItem,
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt5.QtGui import QFont, QIcon, QColor, QPalette

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("client")

BUFFER_SIZE = 65536
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 17887


# ─────────────────────────────────────────────
#  协议工具（与服务端保持一致）
# ─────────────────────────────────────────────

def send_msg(sock: socket.socket, header: dict, payload: bytes = b""):
    header_bytes = json.dumps(header, ensure_ascii=False).encode("utf-8")
    length = struct.pack(">I", len(header_bytes))
    sock.sendall(length + header_bytes + payload)


def recv_header(sock: socket.socket) -> Optional[dict]:
    try:
        raw = _recv_exact(sock, 4)
        if not raw:
            return None
        header_len = struct.unpack(">I", raw)[0]
        if header_len == 0 or header_len > 1_048_576:
            return None
        header_bytes = _recv_exact(sock, header_len)
        if not header_bytes:
            return None
        return json.loads(header_bytes.decode("utf-8"))
    except Exception:
        return None


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
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


def _recv_to_file(sock: socket.socket, filepath: str, filesize: int,
                  progress_cb=None) -> bool:
    received = 0
    try:
        with open(filepath, "wb") as f:
            while received < filesize:
                chunk_size = min(BUFFER_SIZE, filesize - received)
                chunk = _recv_exact(sock, chunk_size)
                if not chunk:
                    return False
                f.write(chunk)
                received += len(chunk)
                if progress_cb:
                    progress_cb(received, filesize)
        return True
    except Exception as e:
        log.error(f"接收文件失败: {e}")
        return False


# ─────────────────────────────────────────────
#  后台网络线程
# ─────────────────────────────────────────────

class NetworkWorker(QThread):
    """
    负责与服务端保持长连接，接收推送消息并通过信号通知 UI。
    发送操作由 UI 线程直接调用（加锁保护）。
    """
    sig_connected = pyqtSignal()
    sig_disconnected = pyqtSignal(str)
    sig_error = pyqtSignal(str)
    sig_user_list = pyqtSignal(list)
    sig_chat = pyqtSignal(str, str, str)  # from, content, ts
    sig_file_sent_ok = pyqtSignal(str, str)  # filename, to
    sig_incoming_file = pyqtSignal(str, str, int)  # from, filename, filesize
    sig_file_received_ok = pyqtSignal(str, str)  # filename, from

    def __init__(self, host: str, port: int, username: str):
        super().__init__()
        self.host = host
        self.port = port
        self.username = username
        self.sock: Optional[socket.socket] = None
        self._send_lock = threading.Lock()
        self._running = True
        self._save_dir = os.path.expanduser("~/Desktop")  # 默认接收目录
        self._pending_incoming: Optional[dict] = None

    def set_save_dir(self, path: str):
        self._save_dir = path

    # ---- 发送接口（线程安全）----

    def send(self, header: dict, payload: bytes = b""):
        if not self.sock:
            return
        with self._send_lock:
            try:
                send_msg(self.sock, header, payload)
            except Exception as e:
                self.sig_error.emit(f"发送失败: {e}")

    def send_file(self, to: str, filepath: str, progress_cb=None):
        """在调用线程内同步发送文件（大文件请在子线程调用）"""
        filename = os.path.basename(filepath)
        filesize = os.path.getsize(filepath)
        header = {
            "type": "file_request",
            "to": to,
            "filename": filename,
            "filesize": filesize,
        }
        if not self.sock:
            self.sig_error.emit("未连接服务器")
            return

        with self._send_lock:
            try:
                send_msg(self.sock, header)
                sent = 0
                with open(filepath, "rb") as f:
                    while sent < filesize:
                        chunk = f.read(BUFFER_SIZE)
                        if not chunk:
                            break
                        self.sock.sendall(chunk)
                        sent += len(chunk)
                        if progress_cb:
                            progress_cb(sent, filesize)
            except Exception as e:
                self.sig_error.emit(f"文件发送失败: {e}")

    # ---- 线程主循环 ----

    def run(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(10)
            self.sock.connect((self.host, self.port))
            self.sock.settimeout(None)
        except Exception as e:
            self.sig_error.emit(f"连接失败: {e}")
            return

        # 注册
        try:
            send_msg(self.sock, {"type": "register", "username": self.username})
        except Exception as e:
            self.sig_error.emit(f"注册失败: {e}")
            return

        # 等待 register_ok
        header = recv_header(self.sock)
        if not header:
            self.sig_error.emit("服务端无响应")
            return
        if header.get("type") == "error":
            self.sig_error.emit(header.get("msg", "注册失败"))
            return
        if header.get("type") != "register_ok":
            self.sig_error.emit(f"意外响应: {header}")
            return

        self.sig_connected.emit()

        # 主接收循环
        while self._running:
            hdr = recv_header(self.sock)
            if hdr is None:
                if self._running:
                    self.sig_disconnected.emit("与服务端断开连接")
                break

            t = hdr.get("type")
            if t == "user_list":
                self.sig_user_list.emit(hdr.get("users", []))

            elif t == "chat":
                self.sig_chat.emit(
                    hdr.get("from", "?"),
                    hdr.get("content", ""),
                    hdr.get("ts", ""),
                )

            elif t == "file_sent_ok":
                self.sig_file_sent_ok.emit(hdr.get("filename", ""), hdr.get("to", ""))

            elif t == "incoming_file":
                # 触发接收流程（emit 给 UI，UI 弹窗后再读取 socket 数据）
                self._handle_incoming(hdr)

            elif t == "file_received_ok":
                self.sig_file_received_ok.emit(hdr.get("filename", ""), hdr.get("from", ""))

            elif t == "error":
                self.sig_error.emit(hdr.get("msg", "未知错误"))

            elif t == "pong":
                pass

            else:
                log.debug(f"未处理消息: {hdr}")

    def _handle_incoming(self, hdr: dict):
        """接收来自服务端中继的文件（在网络线程内完成，进度通过信号发出）"""
        sender = hdr.get("from", "unknown")
        filename = hdr.get("filename", "file")
        filesize = hdr.get("filesize", 0)

        log.info(f"接收文件 '{filename}' ({filesize}B) 来自 '{sender}'")

        # 通知 UI（让 UI 可以显示提示）
        self.sig_incoming_file.emit(sender, filename, filesize)

        # 保存路径
        save_path = os.path.join(self._save_dir, filename)
        # 避免同名覆盖
        base, ext = os.path.splitext(filename)
        counter = 1
        while os.path.exists(save_path):
            save_path = os.path.join(self._save_dir, f"{base}_{counter}{ext}")
            counter += 1

        # 流式接收
        ok = _recv_to_file(self.sock, save_path, filesize)
        if ok:
            log.info(f"文件已保存至: {save_path}")
            self.sig_file_received_ok.emit(filename, sender)
            # 用额外信号把保存路径传给 UI
            self.sig_chat.emit(
                "系统",
                f"✅ 收到 '{sender}' 发来的文件：{filename}，已保存到 {save_path}",
                time.strftime("%H:%M:%S"),
            )
        else:
            self.sig_error.emit(f"接收文件 '{filename}' 失败")

    def stop(self):
        self._running = False
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass


# ─────────────────────────────────────────────
#  文件发送线程
# ─────────────────────────────────────────────

class FileSendWorker(QThread):
    progress = pyqtSignal(int, int)  # sent, total
    finished = pyqtSignal(bool, str)  # success, message

    def __init__(self, net: NetworkWorker, to: str, filepath: str):
        super().__init__()
        self.net = net
        self.to = to
        self.filepath = filepath

    def run(self):
        def cb(sent, total):
            self.progress.emit(sent, total)

        try:
            self.net.send_file(self.to, self.filepath, progress_cb=cb)
            self.finished.emit(True, "发送完成")
        except Exception as e:
            self.finished.emit(False, str(e))


# ─────────────────────────────────────────────
#  主窗口
# ─────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.net: Optional[NetworkWorker] = None
        self.my_username = ""
        self._setup_ui()

    # ===== UI 初始化 =====

    def _setup_ui(self):
        self.setWindowTitle("文件传输工具")
        self.resize(900, 620)
        self.setMinimumSize(700, 480)

        # 状态栏
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("未连接")

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # ── 连接面板 ──
        conn_group = QGroupBox("服务器连接")
        conn_layout = QHBoxLayout(conn_group)
        conn_layout.setSpacing(6)

        self.input_host = QLineEdit(DEFAULT_HOST)
        self.input_host.setPlaceholderText("服务器地址")
        self.input_host.setMaximumWidth(160)

        self.input_port = QLineEdit(str(DEFAULT_PORT))
        self.input_port.setPlaceholderText("端口")
        self.input_port.setMaximumWidth(70)

        self.input_username = QLineEdit()
        self.input_username.setPlaceholderText("你的用户名")
        self.input_username.setMaximumWidth(140)

        self.btn_connect = QPushButton("连接")
        self.btn_connect.setFixedWidth(70)
        self.btn_connect.clicked.connect(self._on_connect)

        self.btn_disconnect = QPushButton("断开")
        self.btn_disconnect.setFixedWidth(70)
        self.btn_disconnect.setEnabled(False)
        self.btn_disconnect.clicked.connect(self._on_disconnect)

        conn_layout.addWidget(QLabel("地址:"))
        conn_layout.addWidget(self.input_host)
        conn_layout.addWidget(QLabel("端口:"))
        conn_layout.addWidget(self.input_port)
        conn_layout.addWidget(QLabel("用户名:"))
        conn_layout.addWidget(self.input_username)
        conn_layout.addWidget(self.btn_connect)
        conn_layout.addWidget(self.btn_disconnect)
        conn_layout.addStretch()

        main_layout.addWidget(conn_group)

        # ── 主体区域（分割器）──
        splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(splitter, 1)

        # ── 左侧：在线用户 ──
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setSpacing(4)
        left_layout.setContentsMargins(0, 0, 0, 0)

        user_group = QGroupBox("在线用户")
        user_vlayout = QVBoxLayout(user_group)
        self.user_list = QListWidget()
        self.user_list.setMinimumWidth(150)
        self.user_list.itemDoubleClicked.connect(self._on_user_double_click)
        user_vlayout.addWidget(self.user_list)

        btn_refresh = QPushButton("刷新列表")
        btn_refresh.clicked.connect(self._refresh_users)
        user_vlayout.addWidget(btn_refresh)

        left_layout.addWidget(user_group)

        # 发送文件按钮
        self.btn_send_file = QPushButton("📤  发送文件给选中用户")
        self.btn_send_file.setEnabled(False)
        self.btn_send_file.clicked.connect(self._on_send_file)
        left_layout.addWidget(self.btn_send_file)

        # 接收目录选择
        recv_dir_layout = QHBoxLayout()
        self.lbl_recv_dir = QLabel("接收目录: ~/Desktop")
        self.lbl_recv_dir.setWordWrap(True)
        btn_change_dir = QPushButton("更改")
        btn_change_dir.setFixedWidth(50)
        btn_change_dir.clicked.connect(self._change_recv_dir)
        recv_dir_layout.addWidget(self.lbl_recv_dir, 1)
        recv_dir_layout.addWidget(btn_change_dir)
        left_layout.addLayout(recv_dir_layout)

        splitter.addWidget(left_panel)

        # ── 右侧：消息/日志 + 聊天输入 ──
        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setSpacing(6)
        right_layout.setContentsMargins(0, 0, 0, 0)

        log_group = QGroupBox("消息记录")
        log_vlayout = QVBoxLayout(log_group)
        self.msg_box = QTextEdit()
        self.msg_box.setReadOnly(True)
        self.msg_box.setFont(QFont("Consolas", 9))
        log_vlayout.addWidget(self.msg_box)
        right_layout.addWidget(log_group, 1)

        # 聊天输入行
        chat_layout = QHBoxLayout()
        self.chat_input = QLineEdit()
        self.chat_input.setPlaceholderText("输入消息（双击用户可私聊）后按 Enter 广播，或指定接收人...")
        self.chat_input.returnPressed.connect(self._on_send_chat)
        self.btn_chat_send = QPushButton("发送")
        self.btn_chat_send.setFixedWidth(60)
        self.btn_chat_send.setEnabled(False)
        self.btn_chat_send.clicked.connect(self._on_send_chat)
        chat_layout.addWidget(self.chat_input)
        chat_layout.addWidget(self.btn_chat_send)
        right_layout.addLayout(chat_layout)

        splitter.addWidget(right_panel)
        splitter.setSizes([220, 680])

        self._set_connected_state(False)

    # ===== 事件处理 =====

    def _on_connect(self):
        host = self.input_host.text().strip()
        port_str = self.input_port.text().strip()
        username = self.input_username.text().strip()

        if not host or not port_str or not username:
            QMessageBox.warning(self, "提示", "请填写服务器地址、端口和用户名")
            return
        try:
            port = int(port_str)
        except ValueError:
            QMessageBox.warning(self, "提示", "端口必须是数字")
            return

        self.my_username = username
        self._log(f"正在连接 {host}:{port}，用户名: {username}…")

        self.net = NetworkWorker(host, port, username)
        # 设置接收目录
        recv_dir = os.path.expanduser("~/Desktop")
        self.net.set_save_dir(recv_dir)

        # 绑定信号
        self.net.sig_connected.connect(self._on_connected)
        self.net.sig_disconnected.connect(self._on_disconnected)
        self.net.sig_error.connect(self._on_net_error)
        self.net.sig_user_list.connect(self._update_user_list)
        self.net.sig_chat.connect(self._on_chat_received)
        self.net.sig_file_sent_ok.connect(self._on_file_sent_ok)
        self.net.sig_incoming_file.connect(self._on_incoming_file)
        self.net.sig_file_received_ok.connect(self._on_file_received_ok)

        self.net.start()
        self.btn_connect.setEnabled(False)

    def _on_disconnect(self):
        if self.net:
            self.net.stop()
            self.net.wait(2000)
            self.net = None
        self._set_connected_state(False)
        self._log("已主动断开连接")
        self.status_bar.showMessage("未连接")

    def _on_connected(self):
        self._set_connected_state(True)
        self._log(f"✅ 连接成功！欢迎 {self.my_username}")
        self.status_bar.showMessage(f"已连接  |  用户名: {self.my_username}")

    def _on_disconnected(self, reason: str):
        self._set_connected_state(False)
        self._log(f"❌ 断开连接: {reason}")
        self.status_bar.showMessage("已断开")

    def _on_net_error(self, msg: str):
        self._log(f"⚠️  {msg}")
        if "注册" in msg or "连接" in msg:
            self._set_connected_state(False)
            self.btn_connect.setEnabled(True)

    def _on_send_file(self):
        items = self.user_list.selectedItems()
        if not items:
            QMessageBox.information(self, "提示", "请先在用户列表中选择接收方")
            return
        to = items[0].text()
        if to == self.my_username:
            QMessageBox.information(self, "提示", "不能给自己发送文件")
            return

        filepath, _ = QFileDialog.getOpenFileName(self, "选择要发送的文件")
        if not filepath:
            return

        filesize = os.path.getsize(filepath)
        filename = os.path.basename(filepath)
        self._log(f"开始发送文件 '{filename}' ({self._fmt_size(filesize)}) 给 '{to}'…")

        # 进度对话框
        prog = QProgressDialog(f"正在发送 {filename}…", "取消", 0, filesize, self)
        prog.setWindowTitle("文件发送")
        prog.setWindowModality(Qt.WindowModal)
        prog.setValue(0)
        prog.show()

        worker = FileSendWorker(self.net, to, filepath)

        def on_progress(sent, total):
            prog.setValue(sent)

        def on_finished(ok, msg):
            prog.close()
            if ok:
                self._log(f"✅ 文件 '{filename}' 发送给 '{to}' 成功")
            else:
                self._log(f"❌ 文件发送失败: {msg}")

        worker.progress.connect(on_progress, Qt.QueuedConnection)
        worker.finished.connect(on_finished, Qt.QueuedConnection)
        worker.start()

        # 保持 worker 引用防止被 GC
        self._current_send_worker = worker

    def _on_send_chat(self):
        content = self.chat_input.text().strip()
        if not content or not self.net:
            return
        items = self.user_list.selectedItems()
        to = items[0].text() if items else ""
        if to == self.my_username:
            to = ""

        if to:
            self.net.send({"type": "chat", "to": to, "content": content})
            self._log(f"[私聊→{to}] {content}")
        else:
            self.net.send({"type": "chat", "content": content})
            self._log(f"[广播] {content}")

        self.chat_input.clear()

    def _on_user_double_click(self, item: QListWidgetItem):
        username = item.text()
        if username != self.my_username:
            self.chat_input.setFocus()
            self._log(f"💬 私聊模式：消息将发送给 '{username}'（已选中）")

    def _refresh_users(self):
        if self.net:
            self.net.send({"type": "get_users"})

    def _change_recv_dir(self):
        d = QFileDialog.getExistingDirectory(self, "选择文件接收目录")
        if d and self.net:
            self.net.set_save_dir(d)
            self.lbl_recv_dir.setText(f"接收目录: {d}")
            self._log(f"接收目录已改为: {d}")

    # ===== 信号回调 =====

    def _update_user_list(self, users: list):
        self.user_list.clear()
        for u in users:
            item = QListWidgetItem(u)
            if u == self.my_username:
                item.setForeground(QColor("#2980b9"))
                item.setText(f"{u} (我)")
            self.user_list.addItem(item)

    def _on_chat_received(self, sender: str, content: str, ts: str):
        self._log(f"[{ts}] {sender}: {content}")

    def _on_file_sent_ok(self, filename: str, to: str):
        self._log(f"✅ 文件 '{filename}' 已成功送达 '{to}'")

    def _on_incoming_file(self, sender: str, filename: str, filesize: int):
        self._log(f"📥 正在接收来自 '{sender}' 的文件 '{filename}' ({self._fmt_size(filesize)})…")

    def _on_file_received_ok(self, filename: str, sender: str):
        self._log(f"✅ 文件 '{filename}' 接收完成（来自 '{sender}'）")

    # ===== 辅助方法 =====

    def _log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        self.msg_box.append(f"[{ts}] {msg}")
        # 滚动到底部
        self.msg_box.verticalScrollBar().setValue(
            self.msg_box.verticalScrollBar().maximum()
        )

    def _set_connected_state(self, connected: bool):
        self.btn_connect.setEnabled(not connected)
        self.btn_disconnect.setEnabled(connected)
        self.btn_send_file.setEnabled(connected)
        self.btn_chat_send.setEnabled(connected)
        self.input_host.setEnabled(not connected)
        self.input_port.setEnabled(not connected)
        self.input_username.setEnabled(not connected)

    @staticmethod
    def _fmt_size(n: int) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if n < 1024:
                return f"{n:.1f} {unit}"
            n /= 1024
        return f"{n:.1f} TB"

    def closeEvent(self, event):
        if self.net:
            self.net.stop()
            self.net.wait(1500)
        event.accept()


# ─────────────────────────────────────────────
#  入口
# ─────────────────────────────────────────────

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # 简洁深色调色板
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(245, 246, 250))
    palette.setColor(QPalette.WindowText, QColor(30, 30, 30))
    palette.setColor(QPalette.Base, QColor(255, 255, 255))
    palette.setColor(QPalette.Highlight, QColor(41, 128, 185))
    palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
    app.setPalette(palette)

    win = MainWindow()
    win.show()
    sys.exit(app.exec_())
