from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Iterable

import msgpack


class DataSourceType(IntEnum):
    INTERNAL = 0


class MessageType(IntEnum):
    SUBSCRIBE = 0
    DATA = 1
    SUBSCRIBE_RESPONSE = 2


class DataSourceStatus(IntEnum):
    SUCCESS = 0
    ERROR = 1


class ProtocolError(ValueError):
    pass


@dataclass(frozen=True)
class RequestedSource:
    source_type: DataSourceType
    source_id: int
    name: str
    category: str = ""
    key: str = ""


@dataclass(frozen=True)
class SourceDefinition:
    name: str
    category: str
    key: str


@dataclass(frozen=True)
class BufferedValue:
    value: float
    published_at: float


@dataclass(frozen=True)
class ActiveSubscription:
    requested_id: int
    source_token: tuple[str, str]
    subscribed_at: float


def _pack_message(parts: Iterable[object]) -> bytes:
    return msgpack.packb(list(parts), use_bin_type=True)


def _build_subscribe_response(results: list[tuple[int, DataSourceStatus]]) -> bytes:
    success = all(status == DataSourceStatus.SUCCESS for _, status in results)
    response_items = [[source_id, int(status)] for source_id, status in results]
    return _pack_message([int(MessageType.SUBSCRIBE_RESPONSE), success, response_items])


def _build_data_message(source_id: int, timestamp_seconds: float, value: float) -> bytes:
    return _pack_message([int(MessageType.DATA), source_id, timestamp_seconds, value])


def _decode_subscribe_message(message: object) -> list[RequestedSource]:
    if not isinstance(message, list):
        raise ProtocolError("Top-level msgpack object must be an array.")
    if len(message) != 2:
        raise ProtocolError("Subscribe message must have exactly 2 fields.")

    message_type = _require_int(message[0], "message type")
    if message_type != int(MessageType.SUBSCRIBE):
        raise ProtocolError(f"Unsupported message type: {message_type}")

    raw_sources = message[1]
    if not isinstance(raw_sources, list):
        raise ProtocolError("Subscribe payload must contain a list of data sources.")

    return [_decode_requested_source(raw_source) for raw_source in raw_sources]


def _decode_requested_source(raw_source: object) -> RequestedSource:
    if not isinstance(raw_source, list) or len(raw_source) != 2:
        raise ProtocolError("Each requested source must be [source_type, payload].")

    source_type = DataSourceType(_require_int(raw_source[0], "source type"))
    payload = raw_source[1]
    if not isinstance(payload, list):
        raise ProtocolError("Wrapped data source payload must be an array.")

    if source_type == DataSourceType.INTERNAL:
        if len(payload) != 4:
            raise ProtocolError("Internal data source payload must be [id, name, category, key].")
        return RequestedSource(
            source_type=source_type,
            source_id=_require_int(payload[0], "internal source id"),
            name=_require_str(payload[1], "internal source name"),
            category=_require_str(payload[2], "internal source category"),
            key=_require_str(payload[3], "internal source key"),
        )

    raise ProtocolError(f"Unsupported data source type: {source_type}")


def _require_int(value: object, label: str) -> int:
    if not isinstance(value, int):
        raise ProtocolError(f"{label} must be an integer.")
    return value


def _require_str(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise ProtocolError(f"{label} must be a string.")
    return value


class _ClientSession(threading.Thread):
    def __init__(self, server: "BoardPoseDataStreamPublisher", client_id: int, sock: socket.socket) -> None:
        super().__init__(name=f"board-pose-client-{client_id}", daemon=True)
        self._server = server
        self.client_id = client_id
        self._socket = sock
        self._socket.settimeout(1.0)
        self._peer = sock.getpeername()
        self._unpacker = msgpack.Unpacker(raw=False)
        self._send_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._subscriptions: list[ActiveSubscription] = []

    def run(self) -> None:
        print(f"BoardPoseDataStream: client {self.client_id} connected from {self._peer[0]}:{self._peer[1]}")
        try:
            while not self._stop_event.is_set() and self._server.is_running:
                try:
                    payload = self._socket.recv(4096)
                except socket.timeout:
                    continue
                except OSError:
                    break

                if not payload:
                    break

                self._unpacker.feed(payload)
                for message in self._unpacker:
                    self._handle_message(message)
        finally:
            self.close()
            self._server.remove_session(self.client_id)

    def close(self) -> None:
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._socket.close()
        except OSError:
            pass

    def snapshot_subscriptions(self) -> list[ActiveSubscription]:
        with self._state_lock:
            return list(self._subscriptions)

    def stream_buffered_values(self) -> bool:
        subscriptions = self.snapshot_subscriptions()
        if not subscriptions:
            return True

        for subscription in subscriptions:
            buffered_value = self._server.get_buffered_value(*subscription.source_token)
            if buffered_value is None:
                continue

            elapsed_seconds = max(0.0, buffered_value.published_at - subscription.subscribed_at)
            packet = _build_data_message(
                subscription.requested_id,
                elapsed_seconds,
                buffered_value.value,
            )
            if not self._send_packet(packet):
                return False

        return True

    def _handle_message(self, message: object) -> None:
        try:
            requested_sources = _decode_subscribe_message(message)
        except ProtocolError as exc:
            print(f"BoardPoseDataStream: client {self.client_id} malformed subscribe: {exc}")
            return

        response_items: list[tuple[int, DataSourceStatus]] = []
        accepted_subscriptions: list[ActiveSubscription] = []
        subscribe_time = time.time()

        for requested_source in requested_sources:
            status, source = self._validate_subscription(requested_source)
            response_items.append((requested_source.source_id, status))
            if status == DataSourceStatus.SUCCESS and source is not None:
                accepted_subscriptions.append(
                    ActiveSubscription(
                        requested_id=requested_source.source_id,
                        source_token=(source.category, source.key),
                        subscribed_at=subscribe_time,
                    )
                )

        with self._state_lock:
            self._subscriptions = accepted_subscriptions

        response_packet = _build_subscribe_response(response_items)
        self._send_packet(response_packet)

    def _validate_subscription(
        self,
        requested_source: RequestedSource,
    ) -> tuple[DataSourceStatus, SourceDefinition | None]:
        if requested_source.source_type != DataSourceType.INTERNAL:
            return DataSourceStatus.ERROR, None

        if not requested_source.category or not requested_source.key:
            return DataSourceStatus.ERROR, None

        source = self._server.get_source(requested_source.category, requested_source.key)
        if source is None:
            return DataSourceStatus.ERROR, None

        return DataSourceStatus.SUCCESS, source

    def _send_packet(self, packet: bytes) -> bool:
        if self._stop_event.is_set():
            return False
        try:
            with self._send_lock:
                self._socket.sendall(packet)
            return True
        except OSError:
            self.close()
            return False


class BoardPoseDataStreamPublisher:
    """FurioDataStream-compatible server for board pose channels."""

    _CHANNELS: list[tuple[str, str, str]] = [
        ("BOARD_POSE", "x_mm", "Board pose X (mm)"),
        ("BOARD_POSE", "y_mm", "Board pose Y (mm)"),
        ("BOARD_POSE", "z_mm", "Board pose Z (mm)"),
        ("BOARD_POSE", "roll_deg", "Board pose roll (deg)"),
        ("BOARD_POSE", "pitch_deg", "Board pose pitch (deg)"),
        ("BOARD_POSE", "yaw_deg", "Board pose yaw (deg)"),
    ]

    def __init__(self, enabled: bool, host: str, port: int, stream_hz: float = 50.0) -> None:
        self.enabled = bool(enabled)
        self.host = host
        self.port = int(port)
        self.stream_hz = max(1.0, float(stream_hz))

        self._sources: dict[tuple[str, str], SourceDefinition] = {}
        self._latest_values: dict[tuple[str, str], BufferedValue] = {}
        self._source_lock = threading.Lock()

        self._sessions: dict[int, _ClientSession] = {}
        self._sessions_lock = threading.Lock()
        self._next_client_id = 1

        self._stop_event = threading.Event()
        self._listen_socket: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._stream_thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set()

    def start(self) -> None:
        if not self.enabled:
            return

        for category, key, name in self._CHANNELS:
            self.register_source(category, key, name)

        try:
            self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._listen_socket.bind((self.host, self.port))
            self._listen_socket.listen()
            self._listen_socket.settimeout(1.0)
        except OSError as exc:
            print(f"BoardPoseDataStream: failed to bind {self.host}:{self.port}: {exc}")
            self.enabled = False
            self.stop()
            return

        self._accept_thread = threading.Thread(target=self._accept_loop, name="board-pose-accept", daemon=True)
        self._stream_thread = threading.Thread(target=self._stream_loop, name="board-pose-stream", daemon=True)

        self._accept_thread.start()
        self._stream_thread.start()

        print(f"BoardPoseDataStream: listening on {self.host}:{self.port} ({self.stream_hz:.1f} Hz)")
        print("BoardPoseDataStream: available channels:")
        for source in self.list_sources():
            print(f"  category={source.category} key={source.key} name={source.name}")

    def stop(self) -> None:
        self._stop_event.set()

        if self._listen_socket is not None:
            try:
                self._listen_socket.close()
            except OSError:
                pass
            self._listen_socket = None

        with self._sessions_lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()

        for session in sessions:
            session.close()

        if self._accept_thread is not None and self._accept_thread.is_alive():
            self._accept_thread.join(timeout=2.0)
        if self._stream_thread is not None and self._stream_thread.is_alive():
            self._stream_thread.join(timeout=2.0)

    def publish_board_pose(
        self,
        x_mm: float,
        y_mm: float,
        z_mm: float,
        roll_deg: float,
        pitch_deg: float,
        yaw_deg: float,
    ) -> None:
        if not self.enabled or self._stop_event.is_set():
            return

        published_at = time.time()
        self.publish_value("BOARD_POSE", "x_mm", float(x_mm), published_at)
        self.publish_value("BOARD_POSE", "y_mm", float(y_mm), published_at)
        self.publish_value("BOARD_POSE", "z_mm", float(z_mm), published_at)
        self.publish_value("BOARD_POSE", "roll_deg", float(roll_deg), published_at)
        self.publish_value("BOARD_POSE", "pitch_deg", float(pitch_deg), published_at)
        self.publish_value("BOARD_POSE", "yaw_deg", float(yaw_deg), published_at)

    def publish_value(
        self,
        category: str,
        key: str,
        value: float,
        timestamp: float | None = None,
    ) -> None:
        token = (category, key)
        published_at = time.time() if timestamp is None else float(timestamp)

        with self._source_lock:
            if token not in self._sources:
                return
            self._latest_values[token] = BufferedValue(float(value), published_at)

    def register_source(self, category: str, key: str, name: str) -> None:
        with self._source_lock:
            self._sources[(category, key)] = SourceDefinition(name=name, category=category, key=key)

    def get_source(self, category: str, key: str) -> SourceDefinition | None:
        with self._source_lock:
            return self._sources.get((category, key))

    def get_buffered_value(self, category: str, key: str) -> BufferedValue | None:
        with self._source_lock:
            return self._latest_values.get((category, key))

    def list_sources(self) -> list[SourceDefinition]:
        with self._source_lock:
            return list(self._sources.values())

    def remove_session(self, client_id: int) -> None:
        with self._sessions_lock:
            self._sessions.pop(client_id, None)

    def _accept_loop(self) -> None:
        if self._listen_socket is None:
            return

        while not self._stop_event.is_set():
            try:
                client_socket, _ = self._listen_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            with self._sessions_lock:
                client_id = self._next_client_id
                self._next_client_id += 1
                session = _ClientSession(self, client_id, client_socket)
                self._sessions[client_id] = session
            session.start()

    def _stream_loop(self) -> None:
        interval_seconds = 1.0 / self.stream_hz

        while not self._stop_event.is_set():
            loop_started = time.time()

            with self._sessions_lock:
                sessions = list(self._sessions.values())

            for session in sessions:
                if not session.stream_buffered_values():
                    self.remove_session(session.client_id)

            remaining = interval_seconds - (time.time() - loop_started)
            if remaining > 0.0:
                time.sleep(remaining)
