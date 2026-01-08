#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
File    : mavsocket_server.py
Author  : FantasyWilly
Email   : bc697522h04@gmail.com
SPDX-License-Identifier: Apache-2.0

功能總覽:
    • [client <-> WebSocket Server <-> Upstream]
    • 從 Upstream 接收 MAVlink 資訊的方式 (Serial | TCP | UDP)
    • 建立 WebSocket Server
    • 將資訊透過打包成 socket 形式 送進 client

遵循:
    • Google Python Style Guide (含區段標題)
    • PEP 8 (行寬 ≤ 88, snake_case, 2 空行區段分隔)
"""

# ------------------------------------------------------------------------------------ #
# Imports
# ------------------------------------------------------------------------------------ #
# 標準庫
import os
import sys
import time
import asyncio
import logging
import signal
import socket
import glob
import re
from typing import Optional, Set, Tuple, List

# 第三方套件
import yaml
import serial
import websockets
from websockets.server import ServerConnection


# ------------------------------------------------------------------------------------ #
# 路徑讀取
# ------------------------------------------------------------------------------------ #
def _resolve_config_path() -> str:
    # 外部環境變數讀取
    env_path = os.environ.get("MAVSOCKET_CONFIG", "").strip()
    if env_path:
        return os.path.expanduser(env_path)

    # 二進制本身存在的資料夾底下的檔案
    exe_dir = os.path.dirname(os.path.abspath(sys.executable))
    p = os.path.join(exe_dir, "mavsocket.yaml")
    if os.path.exists(p):
        return p

    # 當前工作空間下的檔案
    p = os.path.join(os.getcwd(), "mavsocket.yaml")
    if os.path.exists(p):
        return p

    # 跑 python3 原始碼
    script_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(script_dir, "mavsocket.yaml")

CONFIG_PATH = _resolve_config_path()


# ------------------------------------------------------------------------------------ #
# Config
# ------------------------------------------------------------------------------------ #
try:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
except FileNotFoundError:
    cfg = {}
    logging.warning("Config file not found: %s, using built-in defaults", CONFIG_PATH)

# 選擇連接方式 [serial / tcp / udp]
UPSTREAM_MODE = str(cfg.get("upstream_mode", "udp")).strip().lower()

# [Serial] 連接相關參數
SERIAL_PORT = cfg.get("serial_port", "/dev/ttyACM0")
SERIAL_BAUD = int(cfg.get("serial_baud", 57600))

# [TCP] 連接相關參數
TCP_HOST = cfg.get("tcp_host", "127.0.0.1")
TCP_PORT = int(cfg.get("tcp_port", 14650))
TCP_CONNECT_TIMEOUT_S = float(cfg.get("tcp_connect_timeout_s", 2.0))

# [UDP] 連接相關參數
UDP_RX_IP = cfg.get("udp_rx_ip", "0.0.0.0")
UDP_RX_PORT = int(cfg.get("udp_rx_port", 14560))
UDP_TX_IP = cfg.get("udp_tx_ip", "127.0.0.1")
UDP_TX_PORT = int(cfg.get("udp_tx_port", 14570))

# [WebSocket Server] 相關參數
WS_HOST = cfg.get("ws_host", "0.0.0.0")
WS_PORT = int(cfg.get("ws_port", 8765))
TX_QUEUE_MAX = int(cfg.get("tx_queue_max", 1000))
RX_QUEUE_MAX = int(cfg.get("rx_queue_max", 2000))

# LOG 相關參數
LOG_DIR_RAW = cfg.get("log_dir", "~/logs")
LOG_DIR = os.path.expanduser(LOG_DIR_RAW)
LOG_FILE_NAME = cfg.get("log_file", "mavsocket_server.log")

LOG_FILE = os.path.join(LOG_DIR, LOG_FILE_NAME)
LOG_LEVEL_NAME = str(cfg.get("log_level", "INFO")).upper()


# ------------------------------------------------------------------------------------ #
# 日誌寫入
# ------------------------------------------------------------------------------------ #
def _ensure_log_dir(log_path: str) -> None:
    d = os.path.dirname(os.path.abspath(log_path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

_ensure_log_dir(LOG_FILE)
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)

log = logging.getLogger()
logging.getLogger("websockets").setLevel(logging.WARNING)


# ------------------------------------------------------------------------------------ #
# 主程式
# ------------------------------------------------------------------------------------ #
class MavSocketAsync:
    def __init__(self) -> None:
        # ------------------------------------------------------------------
        # 1. 建立與 UpStream 溝通物件 / UDP 計數物件
        # ------------------------------------------------------------------
        self.ser: Optional[serial.Serial] = None
        self.tcp: Optional[socket.socket] = None
        self.udp_rx: Optional[socket.socket] = None
        self.udp_tx: Optional[socket.socket] = None
        self._udp_tx_addr: Tuple[str, int] = (UDP_TX_IP, UDP_TX_PORT)

        self._last_udp_rx_ts: Optional[float] = None
        self._udp_watchdog_task: Optional[asyncio.Task] = None
        self._udp_rx_stalled: bool = False
        self._drop_rx = 0

        self._tcp_connect_fail_logged: bool = False

        self._serial_connect_fail_logged: bool = False
        self._serial_device: Optional[str] = None

        # ------------------------------------------------------------------
        # 2. 建立 WebSocket Client 儲存池 / WebSocket Server 暫存器
        # ------------------------------------------------------------------
        self.clients: Set[ServerConnection] = set()
        self._to_upstream: asyncio.Queue[bytes] = asyncio.Queue(maxsize=TX_QUEUE_MAX)
        self._from_upstream: asyncio.Queue[bytes] = asyncio.Queue(maxsize=RX_QUEUE_MAX)

        # ------------------------------------------------------------------
        # 3. 建立 Asyncio [LOOP] & [Task] 物件
        # ------------------------------------------------------------------
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._tasks: List[asyncio.Task] = []

        # ------------------------------------------------------------------
        # 4. 建立 Asyncio [LOOP] 停止旗標
        # ------------------------------------------------------------------
        self._stop = asyncio.Event()

    # ==========================================================================
    # 讀取參數 Upstream 連接模式 & 處理主線程暫停
    # ==========================================================================
    def mode(self) -> str:
        return UPSTREAM_MODE

    def stop(self) -> None:
        self._stop.set()

    async def wait_stopped(self) -> None:
        await self._stop.wait()

    # ==========================================================================
    # 訊號處理器 (關閉程式)
    # ==========================================================================
    def install_shutdown_handlers(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except Exception:
            loop = None

        def _sig() -> None:
            self.stop()

        if loop is not None:
            try:
                loop.add_signal_handler(signal.SIGINT, _sig)
                loop.add_signal_handler(signal.SIGTERM, _sig)
            except Exception:
                pass

        try:
            signal.signal(signal.SIGINT, lambda *_: _sig())
            signal.signal(signal.SIGTERM, lambda *_: _sig())
        except Exception:
            pass

    # ==========================================================================
    # 開啟 / 關閉 Upstream 連接
    # ==========================================================================
    def open_upstream(self) -> None:
        m = self.mode()
        if m == "serial":
            self._open_serial()

        elif m == "tcp":
            try:
                self._open_tcp()
            except OSError as e:
                log.warning(
                    "[LINK] initial tcp connect failed (%s:%d): %s; "
                    "will retry later in reader/writer loops",
                    TCP_HOST,
                    TCP_PORT,
                    e,
                )

        elif m == "udp":
            self._open_udp()
        else:
            raise ValueError("UPSTREAM_MODE must be serial | udp | tcp")

    def close_upstream(self) -> None:
        self._close_serial()
        self._close_tcp()
        self._close_udp()

    # ------------------------------------------------------------------
    # Serial
    # ------------------------------------------------------------------
    def _open_serial(self) -> bool:
        if self.ser and self.ser.is_open:
            return True

        m = re.match(r"^(/dev/[A-Za-z]+)(\d+)?$", SERIAL_PORT)
        if m:
            pattern = f"{m.group(1)}*"
        else:
            pattern = "/dev/ttyACM*"

        candidates = [SERIAL_PORT] + sorted(glob.glob(pattern))
        seen = set()
        last_err: Optional[Exception] = None

        for dev in candidates:
            if dev in seen:
                continue
            seen.add(dev)

            first_log = not self._serial_connect_fail_logged
            if first_log:
                log.info("[LINK] opening serial... %s @ %d", dev, SERIAL_BAUD)

            try:
                ser = serial.Serial(
                    dev,
                    SERIAL_BAUD,
                    timeout=0.05,
                    write_timeout=0.05,
                )
            except Exception as e:
                last_err = e
                if first_log:
                    log.warning("[LINK] serial open failed (%s): %s", dev, e)
                continue

            self.ser = ser
            self._serial_device = dev
            self._serial_connect_fail_logged = False
            log.info("[LINK] serial connected: %s @ %d", dev, SERIAL_BAUD)
            return True

        if last_err is not None and not self._serial_connect_fail_logged:
            self._serial_connect_fail_logged = True
        return False

    def _close_serial(self) -> None:
        try:
            if self.ser:
                self.ser.close()
        except Exception:
            pass
        self.ser = None
        self._serial_device = None

    # ------------------------------------------------------------------  
    # TCP
    # ------------------------------------------------------------------  
    def _open_tcp(self) -> bool:
        if self.tcp is not None:
            return True

        first_log = not self._tcp_connect_fail_logged
        if first_log:
            log.info("[LINK] connecting tcp... %s:%d", TCP_HOST, TCP_PORT)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TCP_CONNECT_TIMEOUT_S)

        try:
            s.connect((TCP_HOST, TCP_PORT))
        except OSError as e:
            if first_log:
                log.warning("[LINK] tcp connect failed: %s", e)
            s.close()
            self._tcp_connect_fail_logged = True
            return False

        try:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except Exception:
            pass

        s.settimeout(0.05)
        self.tcp = s
        self._tcp_connect_fail_logged = False
        log.info("[LINK] tcp connected")
        return True

    def _close_tcp(self) -> None:
        try:
            if self.tcp:
                self.tcp.close()
        except Exception:
            pass
        self.tcp = None

    # ------------------------------------------------------------------  
    # UDP
    # ------------------------------------------------------------------  
    def _open_udp(self) -> None:
        if self.udp_rx is None:
            rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                rx.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 20)
            except Exception:
                pass
            rx.bind((UDP_RX_IP, UDP_RX_PORT))
            rx.setblocking(False)
            self.udp_rx = rx
            log.info("[WS <- UpStream] UDP (RX) %s:%d", UDP_RX_IP, UDP_RX_PORT)

        if self.udp_tx is None:
            tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                tx.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1 << 20)
            except Exception:
                pass
            self.udp_tx = tx
            log.info("[WS -> UpStream] UDP (TX) %s:%d", UDP_TX_IP, UDP_TX_PORT)

    def _close_udp(self) -> None:
        try:
            if self.udp_rx:
                self.udp_rx.close()
        except Exception:
            pass
        self.udp_rx = None

        try:
            if self.udp_tx:
                self.udp_tx.close()
        except Exception:
            pass
        self.udp_tx = None

    # ==========================================================================
    # WS 和 UpStream 資料處理 [發送 / 接收] 方法
    # ==========================================================================
    # ------------------------------------------------------------------  
    # 處理 [發送] 資料的方式入口
    # ------------------------------------------------------------------ 
    def _write_upstream_blocking(self, data: bytes) -> None:
        if not data:
            return

        m = self.mode()

        if m == "serial":
            if not self.ser or not self.ser.is_open:
                ok = self._open_serial()
                if not ok:
                    time.sleep(0.5)
                    return
            assert self.ser is not None
            try:
                self.ser.write(data)
            except Exception as e:
                if not self._serial_connect_fail_logged:
                    log.warning("[WS -> Upstream] serial write error: %s", e)
                    self._serial_connect_fail_logged = True
                self._close_serial()
                time.sleep(0.5)
            return

        if m == "tcp":
            if self.tcp is None:
                self._open_tcp()
            assert self.tcp is not None
            self.tcp.sendall(data)
            return

        if m == "udp":
            if self.udp_tx is None:
                self._open_udp()
            assert self.udp_tx is not None
            self.udp_tx.sendto(data, self._udp_tx_addr)

    # ------------------------------------------------------------------  
    # Serial
    # ------------------------------------------------------------------ 
    def _read_serial_blocking(self) -> bytes:
        if not self.ser or not self.ser.is_open:
            ok = self._open_serial()
            if not ok:
                time.sleep(0.5)
                return b""
        assert self.ser is not None

        try:
            n = int(getattr(self.ser, "in_waiting", 0) or 0)
            if n <= 0:
                return b""
            return self.ser.read(min(4096, n))
        except Exception as e:
            if not self._serial_connect_fail_logged:
                log.warning("[WS <- Upstream] serial read error: %s", e)
                self._serial_connect_fail_logged = True

            self._close_serial()
            time.sleep(0.5)
            return b""

    # ------------------------------------------------------------------  
    # tcp
    # ------------------------------------------------------------------ 
    def _read_tcp_blocking(self) -> Optional[bytes]:
        if self.tcp is None:
            ok = self._open_tcp()
            if not ok:
                time.sleep(0.5)
                return None

        assert self.tcp is not None
        try:
            return self.tcp.recv(4096)
        except socket.timeout:
            return None
        except OSError as e:
            log.warning("[WS <- Upstream] tcp read error: %s", e)
            self._close_tcp()
            time.sleep(0.5)
            return None

    # ------------------------------------------------------------------  
    # udp
    # ------------------------------------------------------------------ 
    def _udp_on_readable(self) -> None:
        if not self.udp_rx:
            return

        while True:
            try:
                data, _addr = self.udp_rx.recvfrom(65535)
            except BlockingIOError:
                break
            except Exception:
                break

            if not data:
                continue

            self._last_udp_rx_ts = time.monotonic()
            if self._udp_rx_stalled:
                log.info("[WS <- UpStream] Successful Get NEW UDP Data")
                self._udp_rx_stalled = False

            try:
                self._from_upstream.put_nowait(data)
            except asyncio.QueueFull:
                self._drop_rx += 1
                if self._drop_rx % 1000 == 0:
                    log.warning(
                        "[WS <- UpStream] RX Queue Full, dropped=%d",
                        self._drop_rx,
                    )

    async def _udp_watchdog_loop(self, interval: float = 5.0) -> None:
        await asyncio.sleep(interval)

        while not self._stop.is_set():
            try:
                now = time.monotonic()

                if self._last_udp_rx_ts is None:
                    if not self._udp_rx_stalled:
                        log.warning("[WS <- UpStream] no UDP data received yet")
                        self._udp_rx_stalled = True

                else:
                    idle = now - self._last_udp_rx_ts
                    if idle >= interval:
                        if not self._udp_rx_stalled:
                            log.warning(
                                "[WS <- UpStream] no UDP data for %.1f seconds",
                                idle,
                            )
                            self._udp_rx_stalled = True

                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[WS <- UpStream] watchdog error: %s", e)
                await asyncio.sleep(interval)

    # ==========================================================================
    # WebSocket Server 資料處理方式 [Client <-> WS <-> UpStream]
    # ========================================================================== 
    # ------------------------------------------------------------------  
    # 處理客戶端 (連線 / 關閉) & 處理客戶端回傳資料
    # ------------------------------------------------------------------  
    async def ws_handler(self, conn: ServerConnection) -> None:
        self.clients.add(conn)
        try:
            async for msg in conn:
                if isinstance(msg, (bytes, bytearray)):
                    await self._ws_binary_to_queue(bytes(msg))
        except Exception:
            pass
        finally:
            self.clients.discard(conn)

    # ------------------------------------------------------------------  
    # [Client -> WS] 回傳資料送進暫存器
    # ------------------------------------------------------------------  
    async def _ws_binary_to_queue(self, data: bytes) -> None:
        if not data:
            return
        try:
            self._to_upstream.put_nowait(data)
        except asyncio.QueueFull:
            log.warning("[Client -> WS] TX Queue Full, dropping %d bytes", len(data))
            return
        
    # ------------------------------------------------------------------  
    # [Client <- WS] 將接收資料送進每個 Client
    # ------------------------------------------------------------------   
    async def broadcaster_loop(self) -> None:
        while not self._stop.is_set():
            try:
                data = await self._from_upstream.get()
                await self._broadcast_binary(data)
            except Exception:
                await asyncio.sleep(0.001)

    async def _broadcast_binary(self, data: bytes) -> None:
        if not self.clients:
            return
        dead = []
        for c in list(self.clients):
            try:
                await c.send(data)
            except Exception:
                dead.append(c)
        for c in dead:
            self.clients.discard(c)
        
    # ------------------------------------------------------------------  
    # [WS -> Upstream] 將資料送進 Upstream 通道 (啟用多線程)
    # ------------------------------------------------------------------   
    async def upstream_writer_loop(self) -> None:
        while not self._stop.is_set():
            try:
                data = await self._to_upstream.get()
                await asyncio.to_thread(self._write_upstream_blocking, data)
            except Exception as e:
                log.warning("[WS -> Upstream] writer error: %s", e)
                self.close_upstream()
                await asyncio.sleep(0.2)

    # ------------------------------------------------------------------  
    # [WS <- Upstream] 將 Upstream 資料送進 WebSocket Server (啟用多線程)
    # ------------------------------------------------------------------   
    async def upstream_reader_loop(self) -> None:
        while not self._stop.is_set():
            try:
                m = self.mode()

                if m == "serial":
                    chunk = await asyncio.to_thread(self._read_serial_blocking)
                    if not chunk:
                        await asyncio.sleep(0.001)
                        continue
                    await self._from_upstream.put(chunk)
                    continue

                if m == "tcp":
                    chunk = await asyncio.to_thread(self._read_tcp_blocking)
                    if chunk is None:
                        await asyncio.sleep(0.001)
                        continue
                    if chunk == b"":
                        log.warning("[WS <- Upstream] tcp closed by peer")
                        self._close_tcp()
                        await asyncio.sleep(0.2)
                        continue
                    await self._from_upstream.put(chunk)
                    continue

                await asyncio.sleep(0.05)

            except Exception as e:
                log.warning("[LINK] read error: %s", e)
                self.close_upstream()
                await asyncio.sleep(0.2)

    # ==========================================================================
    # 主循環 Start / Stop IO
    # ========================================================================== 
    def start_io(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

        # 處理 [Client <- WS -> UpStream] 
        self._tasks = [
            asyncio.create_task(self.broadcaster_loop(), name="bcast"),
            asyncio.create_task(self.upstream_writer_loop(), name="writer"),
        ]

        # 處理 [WS <- UpStream] (注意: [Client -> WS] 伺服器開啟就會開始處理)
        if self.mode() == "udp":
            loop.add_reader(self.udp_rx.fileno(), self._udp_on_readable)
            self._udp_watchdog_task = asyncio.create_task(
                self._udp_watchdog_loop(5.0),
                name="udp_watchdog",
            )
        else:
            self._tasks.append(
                asyncio.create_task(self.upstream_reader_loop(), name="reader")
            )

    async def stop_io(self) -> None:
        if self._loop is not None and self.udp_rx is not None:
            try:
                self._loop.remove_reader(self.udp_rx.fileno())
            except Exception:
                pass
        if self._udp_watchdog_task:
            self._udp_watchdog_task.cancel()
            try:
                await self._udp_watchdog_task
            except Exception:
                pass
            self._udp_watchdog_task = None

        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except Exception:
                pass
        self._tasks.clear()

        self.close_upstream()


# ------------------------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------------------------ #
async def main() -> None:
    bridge = MavSocketAsync()
    bridge.install_shutdown_handlers()

    loop = asyncio.get_running_loop()
    bridge.open_upstream()

    try:
        async with websockets.serve(
            bridge.ws_handler,
            host=WS_HOST,
            port=WS_PORT,
        ):
            log.info(
                "[Server] WS://%s:%d | MODE = %s",
                WS_HOST, WS_PORT,
                bridge.mode(),
            )
            print("-" * 88)

            bridge.start_io(loop)

            # 等待 SIGINT / SIGTERM 關閉信號
            await bridge.wait_stopped()
            log.info("[Server] closing")

            await bridge.stop_io()

    except asyncio.CancelledError:
        log.info("[Server] cancelled by Ctrl+C")
        raise
    finally:
        log.info("[Server] closed")
        log.info("-" * 88)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("退出 MAVSocket Server")
