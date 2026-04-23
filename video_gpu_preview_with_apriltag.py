import argparse
import bisect
import ctypes
from collections import deque
import glob
import importlib
import json
import math
import os
import queue
import shutil
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime

import cv2
import numpy as np

try:
    mqtt = importlib.import_module("paho.mqtt.client")
    _MQTT_AVAILABLE = True
except Exception:
    mqtt = None
    _MQTT_AVAILABLE = False

try:
    pa = importlib.import_module("pyarrow")
    pq = importlib.import_module("pyarrow.parquet")
    _PARQUET_AVAILABLE = True
except Exception:
    pa = None
    pq = None
    _PARQUET_AVAILABLE = False

try:
    av = importlib.import_module("av")
    _PYAV_AVAILABLE = True
except Exception:
    av = None
    _PYAV_AVAILABLE = False

try:
    import glfw
    from OpenGL.GL import (
        GL_BGR, GL_RGB, GL_TEXTURE_2D, GL_UNSIGNED_BYTE,
        GL_TEXTURE_MIN_FILTER, GL_TEXTURE_MAG_FILTER, GL_LINEAR,
        GL_TRIANGLE_STRIP, GL_PROJECTION, GL_MODELVIEW, GL_COLOR_BUFFER_BIT,
        glGenTextures, glBindTexture, glTexParameteri, glTexImage2D, glTexSubImage2D,
        glEnable, glDisable, glClear, glBegin, glEnd,
        glTexCoord2f, glVertex2f, glViewport,
        glMatrixMode, glLoadIdentity, glOrtho,
    )
    _GL_AVAILABLE = True
except Exception:
    _GL_AVAILABLE = False


VIDEO_FILE_EXTENSIONS = (".mov", ".mp4", ".m4v", ".avi", ".mkv")


def list_video_files(video_search_dir: str) -> list[str]:
    search_root = os.path.abspath(video_search_dir)
    if not os.path.isdir(search_root):
        return []

    matches: set[str] = set()
    for ext in VIDEO_FILE_EXTENSIONS:
        matches.update(os.path.abspath(path) for path in glob.glob(os.path.join(search_root, f"*{ext}")))
        matches.update(os.path.abspath(path) for path in glob.glob(os.path.join(search_root, f"*{ext.upper()}")))
    return sorted(matches)


def resolve_video_path(video_path: str, video_search_dir: str) -> str:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    resolved_search_dir = (
        video_search_dir
        if os.path.isabs(video_search_dir)
        else os.path.join(repo_root, video_search_dir)
    )

    trimmed = str(video_path).strip()
    if trimmed:
        candidates: list[str] = []
        if os.path.isabs(trimmed):
            candidates.append(trimmed)
        else:
            candidates.append(os.path.abspath(trimmed))
            candidates.append(os.path.abspath(os.path.join(repo_root, trimmed)))
            candidates.append(os.path.abspath(os.path.join(resolved_search_dir, trimmed)))
        for candidate in candidates:
            if os.path.isfile(candidate):
                return candidate
        raise FileNotFoundError(f"Video file not found: {video_path}")

    candidates = list_video_files(resolved_search_dir)
    if not candidates:
        raise FileNotFoundError(
            f"No supported video files found in {resolved_search_dir}. "
            f"Supported extensions: {', '.join(VIDEO_FILE_EXTENSIONS)}"
        )
    return candidates[0]


def list_videos(video_search_dir: str) -> int:
    repo_root = os.path.dirname(os.path.abspath(__file__))
    resolved_search_dir = (
        video_search_dir
        if os.path.isabs(video_search_dir)
        else os.path.join(repo_root, video_search_dir)
    )
    videos = list_video_files(resolved_search_dir)
    if not videos:
        print(f"No supported video files found in {resolved_search_dir}.")
        return 1

    print(f"Videos in {resolved_search_dir}:")
    for index, path in enumerate(videos):
        print(f"[{index}] {os.path.basename(path)}")
        print(f"    {path}")
    return 0


def _decode_fourcc(fourcc_value: float) -> str:
    try:
        fourcc_int = int(round(float(fourcc_value)))
    except Exception:
        return "?"
    if fourcc_int <= 0:
        return "?"

    chars = [chr((fourcc_int >> (8 * shift)) & 0xFF) for shift in range(4)]
    text = "".join(chars).strip("\x00 ")
    return text or "?"


def format_media_timestamp(media_seconds: float) -> str:
    media_seconds = max(0.0, float(media_seconds))
    hours = int(media_seconds // 3600)
    minutes = int((media_seconds % 3600) // 60)
    seconds = int(media_seconds % 60)
    millis = int(round((media_seconds - math.floor(media_seconds)) * 1000.0))
    if millis >= 1000:
        millis -= 1000
        seconds += 1
        if seconds >= 60:
            seconds = 0
            minutes += 1
            if minutes >= 60:
                minutes = 0
                hours += 1
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}"


def open_video_capture(video_path: str) -> tuple[cv2.VideoCapture | None, str, bool]:
    backend_candidates: list[tuple[int | None, str]] = []
    if hasattr(cv2, "CAP_FFMPEG"):
        backend_candidates.append((cv2.CAP_FFMPEG, "FFMPEG"))
    if hasattr(cv2, "CAP_MSMF"):
        backend_candidates.append((cv2.CAP_MSMF, "MSMF"))
    backend_candidates.append((None, "AUTO"))

    supports_hw_hint = hasattr(cv2, "CAP_PROP_HW_ACCELERATION") and hasattr(cv2, "VIDEO_ACCELERATION_ANY")
    tried: set[str] = set()

    for backend_id, backend_name in backend_candidates:
        if backend_name in tried:
            continue
        tried.add(backend_name)

        params: list[int] = []
        if supports_hw_hint:
            params.extend([cv2.CAP_PROP_HW_ACCELERATION, cv2.VIDEO_ACCELERATION_ANY])

        if params:
            try:
                if backend_id is None:
                    capture = cv2.VideoCapture(video_path, cv2.CAP_ANY, params)
                else:
                    capture = cv2.VideoCapture(video_path, backend_id, params)
                if capture is not None and capture.isOpened():
                    try:
                        return capture, capture.getBackendName(), True
                    except Exception:
                        return capture, backend_name, True
                if capture is not None:
                    capture.release()
            except Exception:
                pass

        try:
            if backend_id is None:
                capture = cv2.VideoCapture(video_path)
            else:
                capture = cv2.VideoCapture(video_path, backend_id)
            if capture is not None and capture.isOpened():
                try:
                    return capture, capture.getBackendName(), False
                except Exception:
                    return capture, backend_name, False
            if capture is not None:
                capture.release()
        except Exception:
            pass

    return None, "unavailable", supports_hw_hint


def extract_video_meta(
    capture: cv2.VideoCapture,
    source_name: str,
    capture_backend: str,
    frame_index: int,
    frame_count: int,
    media_ts_ms: float,
) -> dict:
    xres = int(max(0, capture.get(cv2.CAP_PROP_FRAME_WIDTH)))
    yres = int(max(0, capture.get(cv2.CAP_PROP_FRAME_HEIGHT)))
    fps = float(capture.get(cv2.CAP_PROP_FPS))
    fourcc = _decode_fourcc(capture.get(cv2.CAP_PROP_FOURCC))
    return build_video_meta(
        source_name=source_name,
        capture_backend=capture_backend,
        frame_index=frame_index,
        frame_count=frame_count,
        media_ts_ms=media_ts_ms,
        xres=xres,
        yres=yres,
        fps=fps,
        fourcc=fourcc,
    )


def build_video_meta(
    source_name: str,
    capture_backend: str,
    frame_index: int,
    frame_count: int,
    media_ts_ms: float,
    xres: int,
    yres: int,
    fps: float,
    fourcc: str,
) -> dict:
    fps_str = f"{fps:.3f}" if float(fps) > 0.0 else "?"
    aspect = f"{(float(xres) / float(yres)):.4f}" if int(yres) > 0 else "?"
    media_seconds = max(0.0, float(media_ts_ms) / 1000.0)
    return {
        "source": source_name,
        "xres": int(xres),
        "yres": int(yres),
        "fps_str": fps_str,
        "fps_n": 0,
        "fps_d": 1,
        "fourcc": str(fourcc),
        "aspect": aspect,
        "frame_fmt": "FILE",
        "timecode": format_media_timestamp(media_seconds),
        "stride": int(xres) * 3 if int(xres) > 0 else 0,
        "backend": capture_backend,
        "frame_index": int(frame_index),
        "frame_count": int(frame_count),
    }


def discover_source(target_hint: str, timeout_seconds: float):
    finder = ndi.find_create_v2()
    if finder is None:
        raise RuntimeError("find_create_v2 failed")

    ndi.find_wait_for_sources(finder, int(max(0.5, timeout_seconds) * 1000))
    sources = list(ndi.find_get_current_sources(finder))
    if not sources:
        ndi.find_destroy(finder)
        raise RuntimeError("No NDI sources found")

    # Some environments can surface empty/placeholder source names.
    # Keep all sources for diagnostics and metadata matching.
    named_sources = [src for src in sources if getattr(src, "ndi_name", "").strip()]

    def source_search_text(src) -> str:
        fields = []
        for key in ("ndi_name", "url_address", "p_url_address", "ip_address", "p_ip_address"):
            value = getattr(src, key, "")
            if value:
                fields.append(str(value))
        if not fields:
            fields.append(str(src))
        return " ".join(fields).lower()

    candidate_hints = [hint.strip().lower() for hint in target_hint.split(",") if hint.strip()]
    searchable_sources = [(src, source_search_text(src)) for src in sources]
    for hint_lower in candidate_hints:
        for src, searchable_text in searchable_sources:
            if hint_lower in searchable_text:
                return src, finder

    if len(named_sources) == 1:
        return named_sources[0], finder
    if len(sources) == 1:
        return sources[0], finder

    ndi.find_destroy(finder)
    listed = "\n".join(f"  - {getattr(src, 'ndi_name', str(src))}" for src in sources)
    raise RuntimeError(
        f"Could not find an NDI source containing '{target_hint}'.\n"
        f"Discovered sources:\n{listed}\n"
        "Pass a different --source-hint or run with --list."
    )


def list_sources(timeout_seconds: float) -> int:
    finder = ndi.find_create_v2()
    if finder is None:
        print("ERROR: find_create_v2 failed")
        return 2

    try:
        ndi.find_wait_for_sources(finder, int(max(0.5, timeout_seconds) * 1000))
        sources = list(ndi.find_get_current_sources(finder))
        if not sources:
            print("No NDI sources found.")
            return 1

        for i, src in enumerate(sources):
            name = getattr(src, "ndi_name", "") or str(src)
            url = getattr(src, "url_address", "") or getattr(src, "p_url_address", "")
            if url:
                print(f"[{i}] {name} | {url}")
            else:
                print(f"[{i}] {name}")
        return 0
    finally:
        ndi.find_destroy(finder)


def fourcc_name(video_frame) -> str:
    try:
        return str(video_frame.FourCC)
    except Exception:
        return "UNKNOWN"


def frame_to_bgr(video_frame, frame_data: np.ndarray) -> np.ndarray | None:
    width = int(video_frame.xres)
    height = int(video_frame.yres)
    stride = int(video_frame.line_stride_in_bytes)
    if width <= 0 or height <= 0 or stride <= 0:
        return None

    arr = np.asarray(frame_data, dtype=np.uint8)
    fourcc = fourcc_name(video_frame)

    if arr.ndim == 3:
        if arr.shape[2] >= 4 and ("BGRA" in fourcc or "BGRX" in fourcc):
            return np.ascontiguousarray(arr[:, :, :3])
        if arr.shape[2] >= 4 and ("RGBA" in fourcc or "RGBX" in fourcc):
            return cv2.cvtColor(arr[:, :, :4], cv2.COLOR_RGBA2BGR)
        if arr.shape[2] == 2 and "UYVY" in fourcc:
            return cv2.cvtColor(arr, cv2.COLOR_YUV2BGR_UYVY)
        return None

    flat = arr.reshape(-1)

    if "NV12" in fourcc:
        needed = (height * width * 3) // 2
        if flat.size < needed:
            return None
        nv12 = flat[:needed].reshape((height * 3) // 2, width)
        return cv2.cvtColor(nv12, cv2.COLOR_YUV2BGR_NV12)

    if "I420" in fourcc or "YV12" in fourcc:
        needed = (height * width * 3) // 2
        if flat.size < needed:
            return None
        yuv = flat[:needed].reshape((height * 3) // 2, width)
        code = cv2.COLOR_YUV2BGR_I420 if "I420" in fourcc else cv2.COLOR_YUV2BGR_YV12
        return cv2.cvtColor(yuv, code)

    needed = height * stride
    if flat.size < needed:
        return None
    packed = flat[:needed].reshape(height, stride)

    if "UYVY" in fourcc:
        row = packed[:, : width * 2].reshape(height, width, 2)
        return cv2.cvtColor(row, cv2.COLOR_YUV2BGR_UYVY)

    if "BGRA" in fourcc or "BGRX" in fourcc:
        row = packed[:, : width * 4].reshape(height, width, 4)
        return np.ascontiguousarray(row[:, :, :3])

    if "RGBA" in fourcc or "RGBX" in fourcc:
        row = packed[:, : width * 4].reshape(height, width, 4)
        return cv2.cvtColor(row, cv2.COLOR_RGBA2BGR)

    row = packed[:, :width]
    return cv2.cvtColor(row, cv2.COLOR_GRAY2BGR)


def frame_to_gray(video_frame, frame_data: np.ndarray) -> np.ndarray | None:
    """Convert NDI frame to grayscale for tag detection."""
    width = int(video_frame.xres)
    height = int(video_frame.yres)
    stride = int(video_frame.line_stride_in_bytes)
    if width <= 0 or height <= 0 or stride <= 0:
        return None

    arr = np.asarray(frame_data, dtype=np.uint8)
    fourcc = fourcc_name(video_frame)

    # Try 3-channel formats first
    if arr.ndim == 3:
        if arr.shape[2] >= 4 and ("BGRA" in fourcc or "BGRX" in fourcc):
            return cv2.cvtColor(arr[:, :, :4], cv2.COLOR_BGRA2GRAY)
        if arr.shape[2] >= 4 and ("RGBA" in fourcc or "RGBX" in fourcc):
            return cv2.cvtColor(arr[:, :, :4], cv2.COLOR_RGBA2GRAY)
        if arr.shape[2] == 2 and "UYVY" in fourcc:
            return np.ascontiguousarray(arr[:, :, 1])
        return None

    flat = arr.reshape(-1)

    if "NV12" in fourcc:
        needed = (height * width * 3) // 2
        if flat.size < needed:
            return None
        nv12 = flat[:needed].reshape((height * 3) // 2, width)
        return np.ascontiguousarray(nv12[:height, :width])

    if "I420" in fourcc or "YV12" in fourcc:
        needed = (height * width * 3) // 2
        if flat.size < needed:
            return None
        yuv = flat[:needed].reshape((height * 3) // 2, width)
        return np.ascontiguousarray(yuv[:height, :width])

    needed = height * stride
    if flat.size < needed:
        return None
    packed = flat[:needed].reshape(height, stride)

    if "UYVY" in fourcc:
        row = packed[:, : width * 2]
        return np.ascontiguousarray(row[:, 1::2])

    if "BGRA" in fourcc or "BGRX" in fourcc or "RGBA" in fourcc or "RGBX" in fourcc:
        if "BGRA" in fourcc or "BGRX" in fourcc:
            row = packed[:, : width * 4].reshape(height, width, 4)
            return cv2.cvtColor(row, cv2.COLOR_BGRA2GRAY)
        else:
            row = packed[:, : width * 4].reshape(height, width, 4)
            return cv2.cvtColor(row, cv2.COLOR_RGBA2GRAY)

    return np.ascontiguousarray(packed[:, :width])


class NvidiaTelemetry:
    def __init__(self, gpu_index: int = 0, interval: float = 1.0) -> None:
        self.gpu_index = gpu_index
        self.interval = max(0.5, interval)
        self.decoder_util: float | None = None
        self.windows_decode_util: float | None = None
        self.windows_decode_scope: str = "none"
        self.gpu_util: float | None = None
        self.mem_util: float | None = None
        self._available = False
        self._nvidia_available = False
        self._windows_counter_available = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def available(self) -> bool:
        return self._available

    @property
    def nvidia_available(self) -> bool:
        return self._nvidia_available

    @property
    def windows_counter_available(self) -> bool:
        return self._windows_counter_available

    def start(self) -> None:
        if self._thread is not None:
            return
        self._nvidia_available = self._probe_nvidia()
        self._windows_counter_available = self._probe_windows_video_decode()
        self._available = self._nvidia_available or self._windows_counter_available
        if not self._available:
            return
        self._thread = threading.Thread(target=self._poll_loop, name="nvidia-smi-poller", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _probe_nvidia(self) -> bool:
        try:
            result = subprocess.run(
                ["nvidia-smi", "--help"],
                capture_output=True,
                text=True,
                check=False,
                timeout=3,
            )
        except Exception:
            return False
        return result.returncode == 0

    def _build_windows_video_decode_command(self) -> str:
        pid = os.getpid()
        return (
            "$samples = (Get-Counter '\\GPU Engine(*)\\Utilization Percentage').CounterSamples;"
            f"$match = $samples | Where-Object {{ $_.InstanceName -match 'pid_{pid}_' -and $_.InstanceName -match 'phys_{self.gpu_index}' -and $_.InstanceName -match 'engtype_VideoDecode' }};"
            "if (-not $match) {"
            f"  $match = $samples | Where-Object {{ $_.InstanceName -match 'pid_{pid}_' -and $_.InstanceName -match 'engtype_VideoDecode' }};"
            "}"
            "$scope = 'process';"
            "if (-not $match) {"
            f"  $match = $samples | Where-Object {{ $_.InstanceName -match 'phys_{self.gpu_index}' -and $_.InstanceName -match 'engtype_VideoDecode' }};"
            "  $scope = 'gpu';"
            "}"
            "$sum = ($match | Measure-Object -Property CookedValue -Sum).Sum;"
            "if ($null -eq $sum) { $sum = -1; $scope = 'none' }"
            "[Console]::Out.Write(('{0}|{1}' -f $sum, $scope))"
        )

    def _probe_windows_video_decode(self) -> bool:
        if sys.platform != "win32":
            return False
        try:
            result = subprocess.run(
                ["powershell.exe", "-NoProfile", "-Command", self._build_windows_video_decode_command()],
                capture_output=True,
                text=True,
                check=False,
                timeout=5,
            )
        except Exception:
            return False
        return result.returncode == 0

    def _poll_loop(self) -> None:
        while not self._stop_event.is_set():
            self._sample_once()
            self._stop_event.wait(self.interval)

    def _sample_once(self) -> None:
        if self._nvidia_available:
            query = "index,utilization.decoder,utilization.gpu,utilization.memory"
            try:
                result = subprocess.run(
                    [
                        "nvidia-smi",
                        f"--query-gpu={query}",
                        "--format=csv,noheader,nounits",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=3,
                )
                if result.returncode == 0:
                    for line in result.stdout.splitlines():
                        parts = [p.strip() for p in line.split(",")]
                        if len(parts) != 4:
                            continue
                        if int(parts[0]) != self.gpu_index:
                            continue
                        self.decoder_util = float(parts[1])
                        self.gpu_util = float(parts[2])
                        self.mem_util = float(parts[3])
                        break
            except Exception:
                pass

        if self._windows_counter_available:
            try:
                result = subprocess.run(
                    ["powershell.exe", "-NoProfile", "-Command", self._build_windows_video_decode_command()],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=5,
                )
                if result.returncode == 0:
                    value_text = result.stdout.strip()
                    if value_text:
                        value_parts = value_text.split("|", 2)
                        value = float(value_parts[0])
                        self.windows_decode_util = None if value < 0 else value
                        self.windows_decode_scope = value_parts[1] if len(value_parts) > 1 else "unknown"
            except Exception:
                pass


class MqttPublisher:
    """Async MQTT publisher so network I/O never blocks capture/analyze/display threads."""

    def __init__(
        self,
        enabled: bool,
        host: str,
        port: int,
        topic_prefix: str,
        client_id: str,
    ) -> None:
        self.enabled = bool(enabled)
        self.host = host
        self.port = int(port)
        self.topic_prefix = topic_prefix.rstrip("/")
        self.client_id = client_id
        self._queue: queue.Queue[tuple[str, str]] = queue.Queue(maxsize=1000)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._client = None

        if not self.enabled:
            return
        if not _MQTT_AVAILABLE:
            print("MQTT: paho-mqtt not installed; telemetry publish disabled.")
            self.enabled = False

    def start(self) -> None:
        if not self.enabled:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="mqtt-pub")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        try:
            if self._client is not None:
                self._client.loop_stop()
                self._client.disconnect()
        except Exception:
            pass

    def publish(self, topic_suffix: str, payload: dict) -> None:
        if not self.enabled:
            return
        topic = f"{self.topic_prefix}/{topic_suffix.lstrip('/')}"
        body = json.dumps(payload, separators=(",", ":"))
        try:
            self._queue.put_nowait((topic, body))
        except queue.Full:
            # Drop oldest and keep latest data flowing.
            try:
                self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait((topic, body))
            except queue.Full:
                pass

    def _run(self) -> None:
        reconnect_backoff = 0.5
        while not self._stop.is_set():
            try:
                callback_api_v2 = getattr(getattr(mqtt, "CallbackAPIVersion", None), "VERSION2", None)
                if callback_api_v2 is not None:
                    self._client = mqtt.Client(
                        callback_api_version=callback_api_v2,
                        client_id=self.client_id,
                        protocol=mqtt.MQTTv311,
                    )
                else:
                    self._client = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv311)
                self._client.connect(self.host, self.port, keepalive=30)
                self._client.loop_start()
                print(f"MQTT: publishing to {self.host}:{self.port} prefix={self.topic_prefix}")
                reconnect_backoff = 0.5
                while not self._stop.is_set():
                    try:
                        topic, body = self._queue.get(timeout=0.2)
                    except queue.Empty:
                        continue
                    try:
                        self._client.publish(topic, body, qos=0, retain=False)
                    except Exception:
                        # Requeue on transient publish failures.
                        try:
                            self._queue.put_nowait((topic, body))
                        except Exception:
                            pass
                        break
            except Exception as exc:
                print(f"MQTT: connect/publish error: {exc}")
            finally:
                try:
                    if self._client is not None:
                        self._client.loop_stop()
                        self._client.disconnect()
                except Exception:
                    pass
            self._stop.wait(reconnect_backoff)
            reconnect_backoff = min(10.0, reconnect_backoff * 2.0)


class ParquetTelemetryWriter:
    """Async telemetry writer that persists MQTT-like payloads into a Parquet stream."""

    def __init__(
        self,
        enabled: bool,
        output_dir: str,
        source_name: str,
        session_dir: str | None = None,
        flush_rows: int = 2000,
        flush_interval_s: float = 1.0,
    ) -> None:
        self.enabled = bool(enabled)
        self.output_dir = output_dir
        self.source_name = source_name
        self.session_dir = session_dir
        self.flush_rows = max(200, int(flush_rows))
        self.flush_interval_s = max(0.1, float(flush_interval_s))

        self._queue: queue.Queue[tuple[str, dict]] = queue.Queue(maxsize=20000)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._dropped = 0

        if not self.enabled:
            return
        if not _PARQUET_AVAILABLE:
            print("Parquet: pyarrow is not installed; parquet telemetry disabled.")
            self.enabled = False

    def start(self) -> None:
        if not self.enabled:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="parquet-telemetry")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def publish(self, topic_suffix: str, payload: dict) -> None:
        if not self.enabled:
            return
        try:
            self._queue.put_nowait((topic_suffix.lstrip("/"), dict(payload)))
        except queue.Full:
            self._dropped += 1
            try:
                self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait((topic_suffix.lstrip("/"), dict(payload)))
            except queue.Full:
                self._dropped += 1

    def _run(self) -> None:
        assert pa is not None
        assert pq is not None

        if self.session_dir:
            session_dir = self.session_dir
        else:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_source = "".join(ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in self.source_name)
            safe_source = safe_source.strip("_") or "source"
            session_dir = os.path.join(self.output_dir, f"{stamp}_{safe_source}")
        os.makedirs(session_dir, exist_ok=True)
        parquet_path = os.path.join(session_dir, "telemetry.parquet")
        manifest_path = os.path.join(session_dir, "manifest.json")

        writer = None
        rows: list[dict] = []
        total_rows = 0
        last_flush = time.perf_counter()

        def _as_int(value: object) -> int | None:
            try:
                return int(value)
            except Exception:
                return None

        def _flush_batch() -> None:
            nonlocal writer, rows, total_rows, last_flush
            if not rows:
                return

            columns = {
                "topic": [row["topic"] for row in rows],
                "event_type": [row["event_type"] for row in rows],
                "ts_wall_ns": [row["ts_wall_ns"] for row in rows],
                "ts_mono_ns": [row["ts_mono_ns"] for row in rows],
                "ingest_seq": [row["ingest_seq"] for row in rows],
                "frame_id": [row["frame_id"] for row in rows],
                "payload_json": [row["payload_json"] for row in rows],
            }
            table = pa.table(columns)

            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression="zstd")
            writer.write_table(table)
            total_rows += len(rows)
            rows = []
            last_flush = time.perf_counter()

        try:
            print(f"Parquet: writing telemetry -> {parquet_path}")
            while not self._stop.is_set() or not self._queue.empty():
                try:
                    topic_suffix, payload = self._queue.get(timeout=0.2)
                    rows.append(
                        {
                            "topic": str(topic_suffix),
                            "event_type": str(payload.get("type", "")),
                            "ts_wall_ns": _as_int(payload.get("ts_wall_ns")),
                            "ts_mono_ns": _as_int(payload.get("ts_mono_ns")),
                            "ingest_seq": _as_int(payload.get("ingest_seq")),
                            "frame_id": _as_int(payload.get("frame_id")),
                            "payload_json": json.dumps(payload, separators=(",", ":")),
                        }
                    )
                except queue.Empty:
                    pass

                should_flush = len(rows) >= self.flush_rows
                if rows and (time.perf_counter() - last_flush) >= self.flush_interval_s:
                    should_flush = True
                if should_flush:
                    _flush_batch()

            _flush_batch()
        except Exception as exc:
            print(f"Parquet: writer error ({exc})")
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass

            try:
                with open(manifest_path, "w", encoding="utf-8") as manifest_fp:
                    json.dump(
                        {
                            "source_name": self.source_name,
                            "parquet_path": parquet_path,
                            "row_count": int(total_rows),
                            "dropped_events": int(self._dropped),
                            "created_wall_ns": int(time.time_ns()),
                        },
                        manifest_fp,
                        indent=2,
                    )
            except Exception:
                pass


class OverlayDataWriter:
    """Persist per-frame overlay inputs so overlays can be rendered in a second pass."""

    def __init__(self, enabled: bool, session_dir: str, file_name: str = "overlay_data.jsonl") -> None:
        self.enabled = bool(enabled)
        self.session_dir = session_dir
        self.file_name = file_name
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=20000)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._dropped = 0

    def start(self) -> None:
        if not self.enabled:
            return
        self._thread = threading.Thread(target=self._run, daemon=True, name="overlay-data")
        self._thread.start()

    def stop(self) -> tuple[str | None, int]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)
        path = os.path.join(self.session_dir, self.file_name) if self.enabled else None
        return path, int(self._dropped)

    def publish(self, payload: dict) -> None:
        if not self.enabled:
            return
        try:
            self._queue.put_nowait(dict(payload))
        except queue.Full:
            self._dropped += 1
            try:
                self._queue.get_nowait()
            except queue.Empty:
                pass
            try:
                self._queue.put_nowait(dict(payload))
            except queue.Full:
                self._dropped += 1

    def _run(self) -> None:
        os.makedirs(self.session_dir, exist_ok=True)
        path = os.path.join(self.session_dir, self.file_name)
        try:
            with open(path, "w", encoding="utf-8") as fp:
                while not self._stop.is_set() or not self._queue.empty():
                    try:
                        row = self._queue.get(timeout=0.2)
                    except queue.Empty:
                        continue
                    fp.write(json.dumps(row, separators=(",", ":")) + "\n")
        except Exception as exc:
            print(f"OverlayData: writer error ({exc})")


class PyAvVideoDecoder:
    """File decoder using FFmpeg via PyAV with multithreaded codec decode."""

    def __init__(self, video_path: str, decode_threads: int = 0) -> None:
        if not _PYAV_AVAILABLE or av is None:
            raise RuntimeError("PyAV is not available")

        self.video_path = video_path
        self.decode_threads = max(0, int(decode_threads))
        self.backend_name = "PYAV"

        self._container = av.open(video_path, mode="r")
        if not self._container.streams.video:
            raise RuntimeError(f"No video stream found in {video_path}")
        self._stream = self._container.streams.video[0]
        self._codec_name = str(getattr(self._stream, "codec_context", {}).name if getattr(self._stream, "codec_context", None) is not None else "")

        codec_ctx = self._stream.codec_context
        try:
            codec_ctx.thread_type = "AUTO"
        except Exception:
            pass
        if self.decode_threads > 0:
            try:
                codec_ctx.thread_count = int(self.decode_threads)
            except Exception:
                pass

        self.width = int(self._stream.width or 0)
        self.height = int(self._stream.height or 0)
        self.frame_count = int(self._stream.frames or 0)

        self.fps = 0.0
        try:
            if self._stream.average_rate is not None:
                self.fps = float(self._stream.average_rate)
        except Exception:
            self.fps = 0.0
        if self.fps <= 0.0:
            try:
                if self._stream.base_rate is not None:
                    self.fps = float(self._stream.base_rate)
            except Exception:
                self.fps = 0.0

        self._iter = iter(self._container.decode(video=0))
        self._frame_index = 0

    @property
    def codec_name(self) -> str:
        return self._codec_name or "?"

    def read(self) -> tuple[bool, np.ndarray | None, int, float]:
        try:
            frame = next(self._iter)
        except StopIteration:
            return False, None, -1, -1.0
        except Exception:
            return False, None, -1, -1.0

        frame_index = self._frame_index
        self._frame_index += 1

        try:
            bgr = frame.to_ndarray(format="bgr24")
        except Exception:
            return False, None, -1, -1.0

        media_ts_ms = -1.0
        try:
            if frame.time is not None:
                media_ts_ms = float(frame.time) * 1000.0
            elif frame.pts is not None and self._stream.time_base is not None:
                media_ts_ms = float(frame.pts * self._stream.time_base) * 1000.0
        except Exception:
            media_ts_ms = -1.0

        if media_ts_ms < 0.0:
            media_ts_ms = (frame_index / max(1.0, self.fps)) * 1000.0

        return True, bgr, frame_index, media_ts_ms

    def release(self) -> None:
        try:
            self._container.close()
        except Exception:
            pass


class RawFrameRecorder:
    """Asynchronous raw frame recorder (video + JSONL metadata sidecar)."""

    def __init__(
        self,
        base_output_dir: str,
        target_fps: float,
        source_name: str,
        backend: str = "ffmpeg",
        ffmpeg_bin: str = "ffmpeg",
        ffmpeg_encoder: str = "h264_nvenc",
        ffmpeg_preset: str = "p5",
    ) -> None:
        self.base_output_dir = base_output_dir
        self.target_fps = max(1.0, float(target_fps))
        self.source_name = source_name
        self.backend = str(backend).strip().lower() or "auto"
        self.ffmpeg_bin = ffmpeg_bin
        self.ffmpeg_encoder = ffmpeg_encoder
        self.ffmpeg_preset = ffmpeg_preset

        self._queue: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=240)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name="raw-recorder")

        self._active = False
        self._dropped_frames = 0
        self._thread.start()

    def is_recording(self) -> bool:
        with self._lock:
            return bool(self._active)

    def dropped_frames(self) -> int:
        with self._lock:
            return int(self._dropped_frames)

    def start_recording(self, frame_width: int, frame_height: int, session_dir: str | None = None) -> bool:
        with self._lock:
            if self._active:
                return False
            self._active = True
            self._dropped_frames = 0

        if session_dir is None:
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            session_dir = os.path.join(self.base_output_dir, stamp)
        self._enqueue_control((
            "start",
            {
                "session_dir": session_dir,
                "frame_width": int(frame_width),
                "frame_height": int(frame_height),
                "target_fps": float(self.target_fps),
                "source_name": self.source_name,
                "backend": self.backend,
                "ffmpeg_bin": self.ffmpeg_bin,
                "ffmpeg_encoder": self.ffmpeg_encoder,
                "ffmpeg_preset": self.ffmpeg_preset,
            },
        ))
        print(f"Raw recorder: START requested -> {session_dir}")
        return True

    def stop_recording(self) -> bool:
        with self._lock:
            if not self._active:
                return False
            self._active = False
        self._enqueue_control(("stop", None))
        print("Raw recorder: STOP requested")
        return True

    def enqueue_frame(self, frame: np.ndarray, metadata: dict) -> None:
        with self._lock:
            if not self._active:
                return

        try:
            # Copy here so writer thread owns immutable frame data.
            self._queue.put_nowait(("frame", (frame.copy(), dict(metadata))))
        except queue.Full:
            with self._lock:
                self._dropped_frames += 1

    def shutdown(self) -> None:
        with self._lock:
            was_active = self._active
            self._active = False
        if was_active:
            self._enqueue_control(("stop", None))
        self._enqueue_control(("quit", None))
        self._stop.set()
        self._thread.join(timeout=3.0)

    def _enqueue_control(self, item: tuple[str, object]) -> None:
        while True:
            try:
                self._queue.put_nowait(item)
                return
            except queue.Full:
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    pass

    def _open_opencv_writer(
        self,
        session_dir: str,
        frame_width: int,
        frame_height: int,
        target_fps: float,
    ) -> tuple[cv2.VideoWriter | None, str]:
        mp4_path = os.path.join(session_dir, "video_raw.mp4")
        writer = cv2.VideoWriter(
            mp4_path,
            cv2.VideoWriter_fourcc(*"mp4v"),
            float(target_fps),
            (int(frame_width), int(frame_height)),
        )
        if writer.isOpened():
            return writer, mp4_path

        try:
            writer.release()
        except Exception:
            pass

        avi_path = os.path.join(session_dir, "video_raw.avi")
        writer = cv2.VideoWriter(
            avi_path,
            cv2.VideoWriter_fourcc(*"MJPG"),
            float(target_fps),
            (int(frame_width), int(frame_height)),
        )
        if writer.isOpened():
            return writer, avi_path

        try:
            writer.release()
        except Exception:
            pass
        return None, ""

    def _open_ffmpeg_writer(
        self,
        session_dir: str,
        frame_width: int,
        frame_height: int,
        target_fps: float,
        ffmpeg_bin: str,
        ffmpeg_encoder: str,
        ffmpeg_preset: str,
    ) -> tuple[subprocess.Popen | None, str]:
        ffmpeg_exe = self._resolve_ffmpeg_bin(ffmpeg_bin)
        if not ffmpeg_exe:
            print(f"Raw recorder: ffmpeg binary not found (configured='{ffmpeg_bin}')")
            return None, ""

        video_path = os.path.join(session_dir, "video_raw.mp4")
        cmd = [
            ffmpeg_exe,
            "-y",
            "-loglevel", "error",
            "-f", "rawvideo",
            "-pix_fmt", "bgr24",
            "-s:v", f"{int(frame_width)}x{int(frame_height)}",
            "-r", f"{float(target_fps):.6f}",
            "-i", "-",
            "-an",
            "-c:v", ffmpeg_encoder,
            "-preset", ffmpeg_preset,
            "-pix_fmt", "yuv420p",
            video_path,
        ]
        print(f"Raw recorder: ffmpeg executable -> {ffmpeg_exe}")

        try:
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
            time.sleep(0.15)
            if proc.poll() is not None:
                ffmpeg_err = ""
                try:
                    if proc.stderr is not None:
                        ffmpeg_err = proc.stderr.read().decode("utf-8", errors="ignore").strip()
                except Exception:
                    ffmpeg_err = ""
                try:
                    proc.kill()
                except Exception:
                    pass
                if ffmpeg_err:
                    print(f"Raw recorder: ffmpeg exited immediately ({ffmpeg_err})")
                else:
                    print("Raw recorder: ffmpeg exited immediately; check ffmpeg path/encoder settings")
                return None, ""
            return proc, video_path
        except Exception as exc:
            print(f"Raw recorder: ffmpeg launch failed ({exc})")
            return None, ""

    def _resolve_ffmpeg_bin(self, ffmpeg_bin: str) -> str | None:
        candidate = str(ffmpeg_bin).strip() or "ffmpeg"

        # Explicit path (absolute or relative) wins when present.
        if os.path.isabs(candidate) and os.path.exists(candidate):
            return candidate
        if os.path.dirname(candidate):
            resolved = os.path.abspath(candidate)
            if os.path.exists(resolved):
                return resolved

        which_hit = shutil.which(candidate)
        if which_hit:
            return which_hit

        if candidate.lower() not in {"ffmpeg", "ffmpeg.exe"}:
            return None

        local_app_data = os.environ.get("LOCALAPPDATA", "")
        package_root = os.path.join(local_app_data, "Microsoft", "WinGet", "Packages")
        if package_root and os.path.isdir(package_root):
            pattern = os.path.join(package_root, "Gyan.FFmpeg_*", "ffmpeg-*", "bin", "ffmpeg.exe")
            matches = [p for p in glob.glob(pattern) if os.path.isfile(p)]
            if matches:
                matches.sort(key=lambda p: os.path.getmtime(p), reverse=True)
                return matches[0]

        windows_apps = os.path.join(local_app_data, "Microsoft", "WindowsApps", "ffmpeg.exe")
        if os.path.isfile(windows_apps):
            return windows_apps

        return None

    def _open_writer(
        self,
        session_dir: str,
        frame_width: int,
        frame_height: int,
        target_fps: float,
        backend: str,
        ffmpeg_bin: str,
        ffmpeg_encoder: str,
        ffmpeg_preset: str,
    ) -> tuple[str, object | None, str]:
        requested = str(backend).strip().lower()
        if requested not in {"auto", "opencv", "ffmpeg"}:
            requested = "auto"

        if requested in {"auto", "ffmpeg"}:
            proc, ffmpeg_path = self._open_ffmpeg_writer(
                session_dir=session_dir,
                frame_width=frame_width,
                frame_height=frame_height,
                target_fps=target_fps,
                ffmpeg_bin=ffmpeg_bin,
                ffmpeg_encoder=ffmpeg_encoder,
                ffmpeg_preset=ffmpeg_preset,
            )
            if proc is not None:
                return "ffmpeg", proc, ffmpeg_path
            if requested == "ffmpeg":
                return "", None, ""

        writer, path = self._open_opencv_writer(
            session_dir=session_dir,
            frame_width=frame_width,
            frame_height=frame_height,
            target_fps=target_fps,
        )
        if writer is not None:
            return "opencv", writer, path

        return "", None, ""

    def _run(self) -> None:
        writer_kind = ""
        writer_obj: object | None = None
        metadata_fp = None
        manifest_path = ""
        video_path = ""
        frame_count = 0
        start_wall_ns = 0
        target_fps = self.target_fps
        frame_width = 0
        frame_height = 0
        source_name = self.source_name
        backend = self.backend
        ffmpeg_bin = self.ffmpeg_bin
        ffmpeg_encoder = self.ffmpeg_encoder
        ffmpeg_preset = self.ffmpeg_preset

        def close_session() -> None:
            nonlocal writer_kind, writer_obj, metadata_fp, frame_count, manifest_path, video_path, start_wall_ns
            session_backend = writer_kind
            session_manifest_path = manifest_path
            session_video_path = video_path
            if writer_obj is not None:
                if writer_kind == "opencv":
                    try:
                        writer_obj.release()  # type: ignore[attr-defined]
                    except Exception:
                        pass
                elif writer_kind == "ffmpeg":
                    proc = writer_obj
                    try:
                        stdin = getattr(proc, "stdin", None)
                        if stdin is not None:
                            stdin.close()
                    except Exception:
                        pass
                    try:
                        proc.wait(timeout=5.0)  # type: ignore[attr-defined]
                    except Exception:
                        try:
                            proc.kill()  # type: ignore[attr-defined]
                        except Exception:
                            pass
                writer_obj = None
                writer_kind = ""
            if metadata_fp is not None:
                try:
                    metadata_fp.close()
                except Exception:
                    pass
                metadata_fp = None
            if session_manifest_path:
                try:
                    manifest = {
                        "source_name": source_name,
                        "video_path": session_video_path,
                        "metadata_path": os.path.join(os.path.dirname(session_manifest_path), "frames.jsonl"),
                        "start_wall_ns": int(start_wall_ns),
                        "end_wall_ns": int(time.time_ns()),
                        "frame_count": int(frame_count),
                        "dropped_frames": int(self.dropped_frames()),
                        "target_fps": float(target_fps),
                        "frame_width": int(frame_width),
                        "frame_height": int(frame_height),
                        "record_backend": session_backend if session_backend else "none",
                        "requested_backend": backend,
                        "ffmpeg_bin": ffmpeg_bin,
                        "ffmpeg_encoder": ffmpeg_encoder,
                        "ffmpeg_preset": ffmpeg_preset,
                    }
                    with open(session_manifest_path, "w", encoding="utf-8") as fp:
                        json.dump(manifest, fp, indent=2)
                except Exception:
                    pass
            manifest_path = ""
            video_path = ""
            frame_count = 0
            start_wall_ns = 0

        while not self._stop.is_set() or not self._queue.empty():
            try:
                kind, payload = self._queue.get(timeout=0.2)
            except queue.Empty:
                continue

            if kind == "start":
                close_session()
                cfg = dict(payload) if isinstance(payload, dict) else {}
                session_dir = str(cfg.get("session_dir", ""))
                frame_width = int(cfg.get("frame_width", 0))
                frame_height = int(cfg.get("frame_height", 0))
                target_fps = float(cfg.get("target_fps", self.target_fps))
                source_name = str(cfg.get("source_name", self.source_name))
                backend = str(cfg.get("backend", self.backend)).strip().lower() or "auto"
                ffmpeg_bin = str(cfg.get("ffmpeg_bin", self.ffmpeg_bin)).strip() or "ffmpeg"
                ffmpeg_encoder = str(cfg.get("ffmpeg_encoder", self.ffmpeg_encoder)).strip() or "h264_nvenc"
                ffmpeg_preset = str(cfg.get("ffmpeg_preset", self.ffmpeg_preset)).strip() or "p5"
                start_wall_ns = int(time.time_ns())
                frame_count = 0

                try:
                    os.makedirs(session_dir, exist_ok=True)
                    writer_kind, writer_obj, video_path = self._open_writer(
                        session_dir=session_dir,
                        frame_width=frame_width,
                        frame_height=frame_height,
                        target_fps=target_fps,
                        backend=backend,
                        ffmpeg_bin=ffmpeg_bin,
                        ffmpeg_encoder=ffmpeg_encoder,
                        ffmpeg_preset=ffmpeg_preset,
                    )
                    if writer_obj is None:
                        print("Raw recorder: failed to open writer; recording disabled for this session")
                        with self._lock:
                            self._active = False
                        continue
                    metadata_path = os.path.join(session_dir, "frames.jsonl")
                    metadata_fp = open(metadata_path, "w", encoding="utf-8")
                    manifest_path = os.path.join(session_dir, "manifest.json")
                    print(f"Raw recorder: active ({writer_kind}) -> {video_path}")
                except Exception as exc:
                    print(f"Raw recorder: start failed ({exc})")
                    close_session()
                    with self._lock:
                        self._active = False

            elif kind == "frame":
                if writer_obj is None or metadata_fp is None:
                    continue
                try:
                    frame, metadata = payload  # type: ignore[misc]
                    if writer_kind == "opencv":
                        writer_obj.write(frame)  # type: ignore[attr-defined]
                    elif writer_kind == "ffmpeg":
                        stdin = getattr(writer_obj, "stdin", None)
                        if stdin is None:
                            raise RuntimeError("ffmpeg stdin is not available")
                        stdin.write(frame.tobytes())
                    else:
                        raise RuntimeError("no writer backend")
                    metadata_row = dict(metadata)
                    metadata_row["record_index"] = int(frame_count)
                    metadata_fp.write(json.dumps(metadata_row, separators=(",", ":")) + "\n")
                    frame_count += 1
                except Exception as exc:
                    if writer_kind == "ffmpeg":
                        ffmpeg_stopped = False
                        try:
                            ffmpeg_stopped = getattr(writer_obj, "poll", lambda: None)() is not None
                        except Exception:
                            ffmpeg_stopped = False
                        if ffmpeg_stopped:
                            print("Raw recorder: ffmpeg writer stopped unexpectedly; recording ended")
                        else:
                            print(f"Raw recorder: ffmpeg write failed ({exc}); recording ended")
                    else:
                        print(f"Raw recorder: frame write failed ({exc}); recording ended")
                    close_session()
                    with self._lock:
                        self._active = False

            elif kind == "stop":
                close_session()

            elif kind == "quit":
                break

        close_session()


def create_receiver(name: str):
    recv_desc = ndi.RecvCreateV3()
    recv_desc.color_format = ndi.RecvColorFormat.RECV_COLOR_FORMAT_FASTEST
    recv_desc.bandwidth = ndi.RecvBandwidth.RECV_BANDWIDTH_HIGHEST
    recv_desc.allow_video_fields = False

    recv = ndi.recv_create_v3(recv_desc)
    if recv is None:
        raise RuntimeError(f"recv_create_v3 failed for {name}")
    return recv


def parse_dict_names(dicts_arg: str) -> list[str]:
    """Parse comma-separated ArUco dictionary names."""
    names = [part.strip() for part in dicts_arg.split(",") if part.strip()]
    if not names:
        raise ValueError("At least one dictionary must be provided")
    return names


def aruco_dict_name_from_id(dictionary_id: int) -> str | None:
    if not hasattr(cv2, "aruco"):
        return None
    for name in dir(cv2.aruco):
        if not name.startswith("DICT_"):
            continue
        try:
            if int(getattr(cv2.aruco, name)) == int(dictionary_id):
                return name
        except Exception:
            continue
    return None


def load_board_definition(board_json_path: str) -> dict:
    if not os.path.exists(board_json_path):
        raise FileNotFoundError(f"Board JSON not found: {board_json_path}")

    with open(board_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    dictionary_id = int(data["dictionary_id"])
    coordinate_units = str(data.get("coordinate_units", "mm")).strip().lower()
    units_to_mm = {
        "mm": 1.0,
        "millimeter": 1.0,
        "millimeters": 1.0,
        "cm": 10.0,
        "centimeter": 10.0,
        "centimeters": 10.0,
        "m": 1000.0,
        "meter": 1000.0,
        "meters": 1000.0,
    }.get(coordinate_units)
    if units_to_mm is None:
        raise ValueError(
            f"Unsupported coordinate_units '{coordinate_units}' in board JSON: {board_json_path}"
        )
    ids = [int(v) for v in data["ids"]]
    obj_points = [np.asarray(points, dtype=np.float32) for points in data["obj_points"]]

    if len(ids) != len(obj_points):
        raise ValueError("Board JSON invalid: ids and obj_points length mismatch")

    dictionary = cv2.aruco.getPredefinedDictionary(dictionary_id)
    ids_np = np.asarray(ids, dtype=np.int32).reshape(-1, 1)
    board = cv2.aruco.Board(obj_points, dictionary, ids_np)

    # Optional per-id size map in JSON lets mixed-size tags be solved with correct scale.
    tag_size_mm_by_id_json = data.get("tag_size_mm_by_id", {})
    tag_size_mm_by_id: dict[int, float] = {}
    if isinstance(tag_size_mm_by_id_json, dict):
        for key, value in tag_size_mm_by_id_json.items():
            try:
                tag_size_mm_by_id[int(key)] = float(value)
            except Exception:
                continue

    tag_size_mm_default = data.get("tag_size_mm", None)
    if tag_size_mm_default is not None:
        try:
            tag_size_mm_default = float(tag_size_mm_default)
        except Exception:
            tag_size_mm_default = None

    # Fallback: infer per-id side length from board geometry when no explicit size is provided.
    inferred_tag_size_mm_by_id: dict[int, float] = {}
    for marker_id, corners in zip(ids, obj_points):
        if corners.shape[0] < 4:
            continue
        width_mm = float(np.linalg.norm(corners[1] - corners[0]))
        height_mm = float(np.linalg.norm(corners[3] - corners[0]))
        inferred_tag_size_mm_by_id[int(marker_id)] = (width_mm + height_mm) * 0.5

    effective_tag_size_mm_by_id: dict[int, float] = {}
    for marker_id in ids:
        mid = int(marker_id)
        if mid in tag_size_mm_by_id:
            effective_tag_size_mm_by_id[mid] = float(tag_size_mm_by_id[mid])
        elif tag_size_mm_default is not None:
            effective_tag_size_mm_by_id[mid] = float(tag_size_mm_default)
        elif mid in inferred_tag_size_mm_by_id:
            effective_tag_size_mm_by_id[mid] = float(inferred_tag_size_mm_by_id[mid])

    if tag_size_mm_default is None and effective_tag_size_mm_by_id:
        tag_size_mm_default = float(np.median(np.asarray(list(effective_tag_size_mm_by_id.values()), dtype=np.float64)))

    dict_name = aruco_dict_name_from_id(dictionary_id)
    return {
        "name": os.path.splitext(os.path.basename(board_json_path))[0],
        "path": board_json_path,
        "dictionary_id": dictionary_id,
        "dict_name": dict_name,
        "coordinate_units": coordinate_units,
        "units_to_mm": float(units_to_mm),
        "ids": ids,
        "ids_set": set(ids),
        "tag_size_mm": tag_size_mm_default,
        "tag_size_mm_by_id": effective_tag_size_mm_by_id,
        "board": board,
    }


def load_tag_size_map_json(tag_size_map_json_path: str) -> dict[int, float]:
    """Load an optional per-tag size override map from JSON.

    Accepted formats:
    - {"31": 295.6, "32": 163.2}
    - {"tag_size_mm_by_id": {"31": 295.6, "32": 163.2}}
    """
    if not os.path.exists(tag_size_map_json_path):
        raise FileNotFoundError(f"Tag size map JSON not found: {tag_size_map_json_path}")

    with open(tag_size_map_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, dict) and "tag_size_mm_by_id" in data and isinstance(data["tag_size_mm_by_id"], dict):
        data = data["tag_size_mm_by_id"]

    if not isinstance(data, dict):
        raise ValueError("Tag size map JSON must be an object mapping tag id to size in mm")

    result: dict[int, float] = {}
    for key, value in data.items():
        try:
            tag_id = int(key)
            size_mm = float(value)
        except Exception as exc:
            raise ValueError(f"Invalid tag size entry '{key}: {value}'") from exc
        if size_mm <= 0:
            raise ValueError(f"tag_size_mm must be > 0 for tag ID {tag_id}")
        result[tag_id] = size_mm

    return result


def signed_24bit_to_int(three_bytes: bytes) -> int:
    value = (three_bytes[0] << 16) | (three_bytes[1] << 8) | three_bytes[2]
    if value & 0x800000:
        value -= 0x1000000
    return value


def decode_freed_fields(data: bytes) -> tuple[dict[str, int | None], str]:
    decoded = {
        "camera_id": None,
        "pan": None,
        "tilt": None,
        "roll": None,
        "x": None,
        "y": None,
        "z": None,
        "zoom": None,
        "focus": None,
    }

    if len(data) >= 26 and data[0] == 0xD1:
        camera_id_index = 1
        base = 2
        mode = "D1 framed"
    elif len(data) >= 25:
        camera_id_index = 0
        base = 1
        mode = "Legacy/raw"
    else:
        return decoded, "Packet too short"

    decoded["camera_id"] = data[camera_id_index]
    decoded["pan"] = signed_24bit_to_int(data[base : base + 3])
    decoded["tilt"] = signed_24bit_to_int(data[base + 3 : base + 6])
    decoded["roll"] = signed_24bit_to_int(data[base + 6 : base + 9])
    decoded["x"] = signed_24bit_to_int(data[base + 9 : base + 12])
    decoded["y"] = signed_24bit_to_int(data[base + 12 : base + 15])
    decoded["z"] = signed_24bit_to_int(data[base + 15 : base + 18])
    decoded["zoom"] = signed_24bit_to_int(data[base + 18 : base + 21])
    decoded["focus"] = signed_24bit_to_int(data[base + 21 : base + 24])
    return decoded, mode


# Free-D scaling constants — matching capture_freed.py
_FREED_ZOOM_MAX_RAW = 16384.0
_FREED_ZOOM_MAX_X = 30.0
_FREED_FOCUS_MIN_RAW = 364.0
_FREED_FOCUS_MAX_RAW = 1641.0


def _freed_fmt_angle(raw: int | None, scale: float) -> str:
    if raw is None or scale == 0:
        return "-"
    return f"{raw / scale:.2f}"


def _freed_fmt_zoom(raw: int | None) -> str:
    if raw is None:
        return "-"
    z = max(0.0, (raw / _FREED_ZOOM_MAX_RAW) * _FREED_ZOOM_MAX_X)
    return f"{z:.2f}x"


def _freed_fmt_focus(raw: int | None) -> str:
    if raw is None:
        return "-"
    span = _FREED_FOCUS_MAX_RAW - _FREED_FOCUS_MIN_RAW
    pct = min(100.0, max(0.0, ((raw - _FREED_FOCUS_MIN_RAW) / span) * 100.0))
    return f"{pct:.1f}%"


def configure_detector_parameters(
    quad_decimate: float = 2.0,
    quad_sigma: float = 0.0,
    adaptive_thresh_win_size_min: int = 3,
    adaptive_thresh_win_size_max: int = 23,
    corner_refinement_method: int | None = None,
    min_marker_perimeter_rate: float = 0.03,
    error_correction_rate: float = 0.6,
    april_tag_min_white_black_diff: int = 5,
) -> cv2.aruco.DetectorParameters:
    """Configure ArUco DetectorParameters with tuning knobs for robustness.
    
    Parameters:
        quad_decimate (float): Decimation factor for edge detection. Lower values (1.0-1.5) detect
            smaller/distant tags; higher values (3.0+) improve speed. Default 2.0.
        
        quad_sigma (float): Gaussian blur sigma applied before edge detection. Higher values
            (0.5-1.5) improve soft-focus tolerance. Default 0.0 (no blur).
        
        adaptive_thresh_win_size_min (int): Minimum window size for adaptive thresholding (must be odd).
            Controls sensitivity to local lighting. Default 3.
        
        adaptive_thresh_win_size_max (int): Maximum window size for adaptive thresholding (must be odd).
            Larger values tolerate bigger lighting gradients. Default 23.
        
        corner_refinement_method (int): Subpixel corner refinement method.
            None/INT_MAX: No refinement (fastest).
            CORNER_REFINE_SUBPIX: Sub-pixel refinement (moderate speed).
            CORNER_REFINE_CONTOUR: Contour-based refinement.
            CORNER_REFINE_APRILTAG: AprilTag-specific refinement (best quality, slower).
        
        min_marker_perimeter_rate (float): Minimum perimeter as fraction of image diagonal.
            Lower values (0.01-0.02) catch smaller tags; higher (0.05+) reject noise.
            Default 0.03.
        
        error_correction_rate (float): Hamming distance acceptance threshold (0.0-1.0).
            Higher values tolerate more bit errors. Default 0.6.
        
        april_tag_min_white_black_diff (int): Minimum difference in pixel value to detect an edge.
            Higher values (10-20) reduce noise sensitivity. Default 5.
    
    Returns:
        Configured DetectorParameters object.
    """
    params = cv2.aruco.DetectorParameters()
    params.aprilTagQuadDecimate = quad_decimate
    params.aprilTagQuadSigma = quad_sigma
    params.adaptiveThreshWinSizeMin = adaptive_thresh_win_size_min
    params.adaptiveThreshWinSizeMax = adaptive_thresh_win_size_max
    params.minMarkerPerimeterRate = min_marker_perimeter_rate
    params.errorCorrectionRate = error_correction_rate
    params.aprilTagMinWhiteBlackDiff = april_tag_min_white_black_diff
    
    if corner_refinement_method is not None:
        params.cornerRefinementMethod = corner_refinement_method
    
    return params


def build_aruco_detectors(
    dict_names: list[str],
    detector_params: cv2.aruco.DetectorParameters | None = None,
) -> list[tuple[str, object, object | None]]:
    """Build ArUco detectors for the provided dictionary names.
    
    Args:
        dict_names: List of ArUco dictionary names (e.g., ['DICT_APRILTAG_36h11']).
        detector_params: Optional pre-configured DetectorParameters. If None, uses defaults.
    
    Returns:
        List of (dict_name, aruco_dict, aruco_detector) tuples.
    """
    if not hasattr(cv2, "aruco"):
        raise RuntimeError("cv2.aruco not available in this OpenCV build")
    
    if detector_params is None:
        detector_params = cv2.aruco.DetectorParameters()
    
    detector_entries: list[tuple[str, object, object | None]] = []
    for dict_name in dict_names:
        dict_id = getattr(cv2.aruco, dict_name, None)
        if dict_id is None:
            raise ValueError(f"Unknown ArUco dictionary: {dict_name}")
        aruco_dict = cv2.aruco.getPredefinedDictionary(dict_id)
        aruco_detector = (
            cv2.aruco.ArucoDetector(aruco_dict, detector_params)
            if hasattr(cv2.aruco, "ArucoDetector")
            else None
        )
        detector_entries.append((dict_name, aruco_dict, aruco_detector))
    
    return detector_entries


# Detection result type: list of (dict_name, corners, ids) per dictionary
_DetectionList = list[tuple[str, list, object]]


def rotation_matrix_to_quaternion(rot_mtx: np.ndarray) -> tuple[float, float, float, float]:
    """Convert a 3x3 rotation matrix to a unit quaternion in (w, x, y, z) order."""
    trace = float(rot_mtx[0, 0] + rot_mtx[1, 1] + rot_mtx[2, 2])

    if trace > 0.0:
        s = float(np.sqrt(trace + 1.0) * 2.0)
        qw = 0.25 * s
        qx = float((rot_mtx[2, 1] - rot_mtx[1, 2]) / s)
        qy = float((rot_mtx[0, 2] - rot_mtx[2, 0]) / s)
        qz = float((rot_mtx[1, 0] - rot_mtx[0, 1]) / s)
    elif rot_mtx[0, 0] > rot_mtx[1, 1] and rot_mtx[0, 0] > rot_mtx[2, 2]:
        s = float(np.sqrt(1.0 + rot_mtx[0, 0] - rot_mtx[1, 1] - rot_mtx[2, 2]) * 2.0)
        qw = float((rot_mtx[2, 1] - rot_mtx[1, 2]) / s)
        qx = 0.25 * s
        qy = float((rot_mtx[0, 1] + rot_mtx[1, 0]) / s)
        qz = float((rot_mtx[0, 2] + rot_mtx[2, 0]) / s)
    elif rot_mtx[1, 1] > rot_mtx[2, 2]:
        s = float(np.sqrt(1.0 + rot_mtx[1, 1] - rot_mtx[0, 0] - rot_mtx[2, 2]) * 2.0)
        qw = float((rot_mtx[0, 2] - rot_mtx[2, 0]) / s)
        qx = float((rot_mtx[0, 1] + rot_mtx[1, 0]) / s)
        qy = 0.25 * s
        qz = float((rot_mtx[1, 2] + rot_mtx[2, 1]) / s)
    else:
        s = float(np.sqrt(1.0 + rot_mtx[2, 2] - rot_mtx[0, 0] - rot_mtx[1, 1]) * 2.0)
        qw = float((rot_mtx[1, 0] - rot_mtx[0, 1]) / s)
        qx = float((rot_mtx[0, 2] + rot_mtx[2, 0]) / s)
        qy = float((rot_mtx[1, 2] + rot_mtx[2, 1]) / s)
        qz = 0.25 * s

    quat = np.asarray([qw, qx, qy, qz], dtype=np.float64)
    norm = float(np.linalg.norm(quat))
    if norm <= 0.0:
        return 1.0, 0.0, 0.0, 0.0
    quat /= norm
    return float(quat[0]), float(quat[1]), float(quat[2]), float(quat[3])


def rvec_to_quaternion(rvec: np.ndarray) -> tuple[float, float, float, float]:
    """Convert a Rodrigues rotation vector to a unit quaternion in (w, x, y, z) order."""
    rot_mtx, _ = cv2.Rodrigues(rvec)
    return rotation_matrix_to_quaternion(rot_mtx)


def rvec_to_euler_deg(rvec: np.ndarray) -> tuple[float, float, float]:
    """Convert Rodrigues rotation vector to roll/pitch/yaw Euler angles in degrees."""
    rot_mtx, _ = cv2.Rodrigues(rvec)
    sy = float(np.sqrt(rot_mtx[0, 0] * rot_mtx[0, 0] + rot_mtx[1, 0] * rot_mtx[1, 0]))
    singular = sy < 1e-6

    if not singular:
        roll = float(np.arctan2(rot_mtx[2, 1], rot_mtx[2, 2]))
        pitch = float(np.arctan2(-rot_mtx[2, 0], sy))
        yaw = float(np.arctan2(rot_mtx[1, 0], rot_mtx[0, 0]))
    else:
        roll = float(np.arctan2(-rot_mtx[1, 2], rot_mtx[1, 1]))
        pitch = float(np.arctan2(-rot_mtx[2, 0], sy))
        yaw = 0.0

    return float(np.degrees(roll)), float(np.degrees(pitch)), float(np.degrees(yaw))


def _circular_mean_deg(values_deg: list[float]) -> float:
    if not values_deg:
        return 0.0
    angles = np.deg2rad(np.asarray(values_deg, dtype=np.float64))
    sin_m = float(np.mean(np.sin(angles)))
    cos_m = float(np.mean(np.cos(angles)))
    return float(np.degrees(np.arctan2(sin_m, cos_m)))


def euler_deg_to_quaternion(
    roll_deg: float,
    pitch_deg: float,
    yaw_deg: float,
) -> tuple[float, float, float, float]:
    """Convert roll/pitch/yaw Euler angles in degrees to a unit quaternion in (w, x, y, z) order."""
    roll = float(np.deg2rad(roll_deg)) * 0.5
    pitch = float(np.deg2rad(pitch_deg)) * 0.5
    yaw = float(np.deg2rad(yaw_deg)) * 0.5

    cr = float(np.cos(roll))
    sr = float(np.sin(roll))
    cp = float(np.cos(pitch))
    sp = float(np.sin(pitch))
    cy = float(np.cos(yaw))
    sy = float(np.sin(yaw))

    quat = np.asarray([
        cr * cp * cy + sr * sp * sy,
        sr * cp * cy - cr * sp * sy,
        cr * sp * cy + sr * cp * sy,
        cr * cp * sy - sr * sp * cy,
    ], dtype=np.float64)
    norm = float(np.linalg.norm(quat))
    if norm <= 0.0:
        return 1.0, 0.0, 0.0, 0.0
    quat /= norm
    return float(quat[0]), float(quat[1]), float(quat[2]), float(quat[3])


def estimate_board_pose(
    tag_poses: list[tuple[int, float, float, float, float, float, float]],
) -> tuple[float, float, float, float, float, float, int] | None:
    """Estimate a single board pose from multiple tag poses in camera space."""
    if not tag_poses:
        return None

    xs = [float(p[1]) for p in tag_poses]
    ys = [float(p[2]) for p in tag_poses]
    zs = [float(p[3]) for p in tag_poses]
    rolls = [float(p[4]) for p in tag_poses]
    pitches = [float(p[5]) for p in tag_poses]
    yaws = [float(p[6]) for p in tag_poses]

    # Median position is more robust than mean when one tag jitters.
    board_x = float(np.median(np.asarray(xs, dtype=np.float64)))
    board_y = float(np.median(np.asarray(ys, dtype=np.float64)))
    board_z = float(np.median(np.asarray(zs, dtype=np.float64)))

    board_roll = _circular_mean_deg(rolls)
    board_pitch = _circular_mean_deg(pitches)
    board_yaw = _circular_mean_deg(yaws)

    return (board_x, board_y, board_z, board_roll, board_pitch, board_yaw, len(tag_poses))


def estimate_board_pose_from_tag_subset(
    tag_poses: list[tuple[int, float, float, float, float, float, float]],
    board_ids: set[int],
    board_expected_count: int,
) -> tuple[float, float, float, float, float, float, int, int] | None:
    """Fallback board pose using median of solved tag poses for board member IDs."""
    subset = [pose for pose in tag_poses if int(pose[0]) in board_ids]
    aggregate = estimate_board_pose(subset)
    if aggregate is None:
        return None
    board_x, board_y, board_z, board_roll, board_pitch, board_yaw, board_tag_count = aggregate
    return (
        board_x,
        board_y,
        board_z,
        board_roll,
        board_pitch,
        board_yaw,
        board_tag_count,
        int(board_expected_count),
    )


def estimate_board_pose_from_detections(
    detections: _DetectionList,
    board_definition: dict,
    camera_matrix: np.ndarray,
) -> tuple[float, float, float, float, float, float, int, int] | None:
    matched_corners: list[np.ndarray] = []
    matched_ids: list[int] = []

    board_dict_name = board_definition.get("dict_name")
    board_dict_name_upper = board_dict_name.upper() if board_dict_name else None
    board_ids = board_definition["ids_set"]

    for dict_name, corners, ids in detections:
        if board_dict_name_upper and dict_name.upper() != board_dict_name_upper:
            continue
        if not corners or ids is None:
            continue

        for corner, marker_id in zip(corners, ids.flatten()):
            mid = int(marker_id)
            if mid in board_ids:
                matched_corners.append(np.asarray(corner, dtype=np.float32))
                matched_ids.append(mid)

    if not matched_ids:
        return None

    ids_np = np.asarray(matched_ids, dtype=np.int32).reshape(-1, 1)
    dist_coeffs = np.zeros(5, dtype=np.float32)

    # OpenCV builds vary: some expose cv2.aruco.estimatePoseBoard, others do not.
    # Fall back to Board.matchImagePoints + solvePnP when estimatePoseBoard is unavailable.
    try:
        if hasattr(cv2.aruco, "estimatePoseBoard"):
            retval, rvec, tvec = cv2.aruco.estimatePoseBoard(
                matched_corners,
                ids_np,
                board_definition["board"],
                camera_matrix,
                dist_coeffs,
                None,
                None,
            )
            if retval is None or float(retval) <= 0 or rvec is None or tvec is None:
                return None
        else:
            obj_points, img_points = board_definition["board"].matchImagePoints(matched_corners, ids_np)
            if obj_points is None or img_points is None or len(obj_points) < 4:
                return None
            ok, rvec, tvec = cv2.solvePnP(
                obj_points,
                img_points,
                camera_matrix,
                dist_coeffs,
                useExtrinsicGuess=False,
                flags=cv2.SOLVEPNP_ITERATIVE,
            )
            if not ok or rvec is None or tvec is None:
                return None
    except Exception:
        return None

    units_to_mm = float(board_definition.get("units_to_mm", 1.0))
    roll_deg, pitch_deg, yaw_deg = rvec_to_euler_deg(rvec)
    return (
        float(tvec[0, 0]) * units_to_mm,
        float(tvec[1, 0]) * units_to_mm,
        float(tvec[2, 0]) * units_to_mm,
        roll_deg,
        pitch_deg,
        yaw_deg,
        len(set(matched_ids)),
        len(board_definition["ids"]),
    )


def count_board_tag_matches(detections: _DetectionList, board_definition: dict) -> int:
    """Count unique board member tags present in current detections."""
    board_dict_name = board_definition.get("dict_name")
    board_dict_name_upper = board_dict_name.upper() if board_dict_name else None
    board_ids = board_definition["ids_set"]
    matched_ids: set[int] = set()

    for dict_name, corners, ids in detections:
        if board_dict_name_upper and dict_name.upper() != board_dict_name_upper:
            continue
        if not corners or ids is None:
            continue

        for marker_id in ids.flatten():
            mid = int(marker_id)
            if mid in board_ids:
                matched_ids.add(mid)

    return len(matched_ids)


def fit_text_to_width(
    text: str,
    max_width_px: int,
    font: int,
    scale: float,
    thickness: int,
) -> str:
    """Trim text with ellipsis so rendered width stays within max_width_px."""
    if max_width_px <= 0:
        return ""

    text_w, _ = cv2.getTextSize(text, font, scale, thickness)[0]
    if text_w <= max_width_px:
        return text

    ellipsis = "..."
    ellipsis_w, _ = cv2.getTextSize(ellipsis, font, scale, thickness)[0]
    if ellipsis_w > max_width_px:
        return ""

    trimmed = text
    while trimmed:
        trimmed = trimmed[:-1]
        candidate = trimmed + ellipsis
        candidate_w, _ = cv2.getTextSize(candidate, font, scale, thickness)[0]
        if candidate_w <= max_width_px:
            return candidate

    return ellipsis
def refine_detected_markers_with_board(
    image: np.ndarray,
    detector: object,
    corners: list,
    ids: np.ndarray | None,
    board: object,
    camera_matrix: np.ndarray,
    dist_coeffs: np.ndarray,
) -> tuple[list, np.ndarray | None]:
    """Refine marker detections using board geometry constraints.
    
    This refinement pass uses the known board geometry to recover markers
    that were missed or degraded in the initial detection phase. Useful
    for soft-focus, distance, or partial occlusion scenarios.
    
    Args:
        image: Grayscale image frame.
        detector: ArucoDetector object with refineDetectedMarkers capability.
        corners: List of corner arrays from initial detection.
        ids: Array of marker IDs from initial detection.
        board: Board object defining the expected marker configuration.
        camera_matrix: Camera intrinsics matrix.
        dist_coeffs: Distortion coefficients.
    
    Returns:
        (refined_corners, refined_ids) tuple.
    """
    try:
        if not hasattr(detector, "refineDetectedMarkers"):
            return corners, ids
        
        # Get rejected candidates from the detector's internal state if available
        # Otherwise pass empty list
        rejected = []
        
        # Create RefineParameters with defaults
        refine_params = cv2.aruco.RefineParameters()
        
        # Call refinement
        refined_corners, refined_ids, refined_rejected = detector.refineDetectedMarkers(
            image,
            board,
            corners if corners else [],
            ids if ids is not None else np.array([], dtype=np.int32),
            rejected,
            camera_matrix,
            dist_coeffs,
            refine_params,
        )
        
        return refined_corners, refined_ids
    except Exception:
        # On any error, return original detections
        return corners, ids


def detect_tags(
    gray: np.ndarray,
    detector_entries: list[tuple[str, object, object | None]],
    camera_matrix: np.ndarray,
    tag_size_mm: float = 148.6,
    board_definitions: list[dict] | None = None,
    enable_board_refinement: bool = False,
    tag_size_mm_by_id: dict[int, float] | None = None,
) -> tuple[int, list[tuple[int, float, float, float, float, float, float]], _DetectionList, dict[int, dict], dict[int, tuple[float, float, float, float]]]:
    """Run detection and pose estimation on a grayscale frame.
    
    Args:
        gray: Grayscale image.
        detector_entries: List of (dict_name, aruco_dict, aruco_detector) tuples.
        camera_matrix: Camera intrinsics.
        tag_size_mm: Physical tag size in millimeters.
        board_definitions: Optional list of board definition dicts for refinement.
        enable_board_refinement: If True and board_definitions provided, use board geometry
            to refine detections and recover markers missed by initial detection.
    
    Returns:
        (detection_count, poses, detections, tag_image_metrics, tag_quaternions) tuple.
        detections carries corners/ids for display drawing by the rendering thread.
    """
    detection_count = 0
    poses: list[tuple[int, float, float, float, float, float, float]] = []
    detections: _DetectionList = []
    tag_image_metrics: dict[int, dict] = {}
    tag_quaternions: dict[int, tuple[float, float, float, float]] = {}

    object_points_cache: dict[float, np.ndarray] = {}

    def get_object_points_for_size(tag_size_mm_local: float) -> np.ndarray:
        key = float(tag_size_mm_local)
        cached = object_points_cache.get(key)
        if cached is not None:
            return cached
        tag_size_m_local = key / 1000.0
        pts = np.array([
            [-tag_size_m_local / 2, -tag_size_m_local / 2, 0],
            [ tag_size_m_local / 2, -tag_size_m_local / 2, 0],
            [ tag_size_m_local / 2,  tag_size_m_local / 2, 0],
            [-tag_size_m_local / 2,  tag_size_m_local / 2, 0],
        ], dtype=np.float32)
        object_points_cache[key] = pts
        return pts

    default_object_points = get_object_points_for_size(tag_size_mm)
    dist_coeffs = np.zeros(5, dtype=np.float32)

    for dict_name, aruco_dict, aruco_detector in detector_entries:
        try:
            if aruco_detector is not None:
                corners, ids, _ = aruco_detector.detectMarkers(gray)
            else:
                corners, ids, _ = cv2.aruco.detectMarkers(gray, aruco_dict)

            # Apply board refinement if enabled and board is available for this detector
            if enable_board_refinement and board_definitions and aruco_detector is not None:
                for board_def in board_definitions:
                    board_dict_name = board_def.get("dict_name", "").upper()
                    if board_dict_name == dict_name.upper():
                        corners, ids = refine_detected_markers_with_board(
                            gray,
                            aruco_detector,
                            corners if corners else [],
                            ids,
                            board_def["board"],
                            camera_matrix,
                            dist_coeffs,
                        )
                        break

            if ids is None or corners is None or len(ids) == 0:
                detections.append((dict_name, [], None))
                continue

            detection_count += len(ids)
            detections.append((dict_name, corners, ids))

            for corner, tag_id in zip(corners, ids.flatten()):
                try:
                    tid = int(tag_id)
                    local_tag_size_mm = (
                        float(tag_size_mm_by_id.get(tid, tag_size_mm))
                        if tag_size_mm_by_id is not None
                        else float(tag_size_mm)
                    )
                    object_points = (
                        get_object_points_for_size(local_tag_size_mm)
                        if local_tag_size_mm > 0
                        else default_object_points
                    )
                    image_points = corner.reshape(-1, 2).astype(np.float32)
                    success, rvec, tvec = cv2.solvePnP(
                        object_points, image_points, camera_matrix, dist_coeffs,
                        useExtrinsicGuess=False, flags=cv2.SOLVEPNP_ITERATIVE,
                    )
                    if success and tvec is not None and rvec is not None:
                        roll_deg, pitch_deg, yaw_deg = rvec_to_euler_deg(rvec)
                        tag_quaternions[tid] = rvec_to_quaternion(rvec)
                        poses.append((
                            tid,
                            float(tvec[0, 0]) * 1000,
                            float(tvec[1, 0]) * 1000,
                            float(tvec[2, 0]) * 1000,
                            roll_deg,
                            pitch_deg,
                            yaw_deg,
                        ))
                        # Image-plane metrics
                        cx = float(image_points[:, 0].mean())
                        cy = float(image_points[:, 1].mean())
                        bbox_w = float(image_points[:, 0].max() - image_points[:, 0].min())
                        bbox_h = float(image_points[:, 1].max() - image_points[:, 1].min())
                        # Reprojection error is the pixel residual between:
                        # 1) detected tag corners in the image, and
                        # 2) corners reprojected from the solved 6DoF pose.
                        # Lower values mean the pose fits the observed image corners better.
                        # This is an image-fit quality metric, not direct world/robot ground-truth error.
                        projected, _ = cv2.projectPoints(
                            object_points, rvec, tvec, camera_matrix, dist_coeffs
                        )
                        proj_pts = projected.reshape(-1, 2)
                        corner_errors = np.linalg.norm(proj_pts - image_points, axis=1)
                        tag_image_metrics[tid] = {
                            # Image coordinates are in pixel units from top-left of the frame.
                            # They are not pre-centered on the image midpoint.
                            "cx_px": round(cx, 2),
                            "cy_px": round(cy, 2),
                            "bbox_w_px": round(bbox_w, 2),
                            "bbox_h_px": round(bbox_h, 2),
                            # Mean and worst corner residual (pixels) for this tag solve.
                            "reproj_err_mean_px": round(float(corner_errors.mean()), 4),
                            "reproj_err_max_px": round(float(corner_errors.max()), 4),
                        }
                except Exception:
                    pass
        except Exception:
            pass

    return detection_count, poses, detections, tag_image_metrics, tag_quaternions


def scale_detections_for_display(
    detections: _DetectionList,
    coord_scale: float,
) -> _DetectionList:
    if coord_scale == 1.0:
        return detections

    scaled_detections: _DetectionList = []
    for dict_name, corners, ids in detections:
        if not corners or ids is None:
            scaled_detections.append((dict_name, corners, ids))
            continue

        scaled_corners = [corner.astype(np.float32) * coord_scale for corner in corners]
        scaled_detections.append((dict_name, scaled_corners, ids))

    return scaled_detections


def serialize_detections(detections: _DetectionList) -> list[dict]:
    serialized: list[dict] = []
    for dict_name, corners, ids in detections:
        ids_list = [int(v) for v in ids.flatten()] if ids is not None else []
        corners_list = [np.asarray(corner, dtype=np.float32).reshape(-1, 2).tolist() for corner in corners]
        serialized.append(
            {
                "dict_name": str(dict_name),
                "ids": ids_list,
                "corners": corners_list,
            }
        )
    return serialized


def deserialize_detections(serialized: list[dict]) -> _DetectionList:
    detections: _DetectionList = []
    for entry in serialized:
        dict_name = str(entry.get("dict_name", ""))
        ids_list = entry.get("ids", [])
        corners_list = entry.get("corners", [])
        ids_np = np.asarray(ids_list, dtype=np.int32).reshape(-1, 1) if ids_list else None
        corners = [np.asarray(corner, dtype=np.float32).reshape(1, 4, 2) for corner in corners_list]
        detections.append((dict_name, corners, ids_np))
    return detections


def draw_tag_detections(
    image: np.ndarray,
    detections: _DetectionList,
    poses: list[tuple[int, float, float, float, float, float, float]],
) -> None:
    """Draw tag borders, IDs, and pose text onto a BGR image (in-place)."""
    colors = {
        "DICT_APRILTAG_36h11": (0, 255, 0),
        "DICT_4X4_50": (255, 0, 0),
        "DICT_7X7_100": (0, 0, 255),
    }
    height, width = image.shape[:2]

    for dict_name, corners, ids in detections:
        if not corners or ids is None:
            continue
        color = colors.get(dict_name, (255, 255, 0))
        for corner, tag_id in zip(corners, ids.flatten()):
            pts = corner.reshape(-1, 2).astype(int)
            cv2.polylines(image, [pts], True, color, 2)
            cx = int(np.mean(pts[:, 0]))
            cy = int(np.mean(pts[:, 1]))
            cv2.circle(image, (cx, cy), 3, color, -1)
            tl = pts[0]
            label = f"ID:{tag_id}"
            cv2.putText(image, label, (tl[0] + 4, tl[1] - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 0, 0), 2, cv2.LINE_AA)
            cv2.putText(image, label, (tl[0] + 4, tl[1] - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (255, 255, 255), 1, cv2.LINE_AA)

    if poses:
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.35
        font_thickness = 1
        text_pad = 8
        line_h = 12
        max_text_width = max(1, width - (2 * text_pad))
        pose_y = height - (len(poses) * 12) - 8

        # Keep a persistent max width so the right-side pose block does not jitter
        # horizontally when numeric values change width frame-to-frame.
        if not hasattr(draw_tag_detections, "_pose_max_width"):
            draw_tag_detections._pose_max_width = {}
        pose_max_width = draw_tag_detections._pose_max_width
        stable_max_w = int(pose_max_width.get(width, 0))

        # Build all fitted strings first, then find the widest so all lines share
        # the same stable left anchor (left-justified within the right column).
        fitted_texts = []
        max_line_w = 0
        for tag_id, x_mm, y_mm, z_mm, roll_deg, pitch_deg, yaw_deg in poses:
            pose_text = (
                f"ID{tag_id}: XYZ(mm)=({x_mm:.2f},{y_mm:.2f},{z_mm:.2f}) "
                f"RPY(deg)=({roll_deg:.2f},{pitch_deg:.2f},{yaw_deg:.2f})"
            )
            fitted = fit_text_to_width(pose_text, max_text_width, font, font_scale, font_thickness)
            fitted_texts.append(fitted)
            if fitted:
                tw, _ = cv2.getTextSize(fitted, font, font_scale, font_thickness)[0]
                if tw > max_line_w:
                    max_line_w = tw

        if max_line_w > stable_max_w:
            stable_max_w = max_line_w
            pose_max_width[width] = stable_max_w

        stable_max_w = min(stable_max_w, max_text_width)

        # Fixed left anchor based on widest line — all lines start here.
        text_x = max(text_pad, width - stable_max_w - text_pad)
        for fitted_text in fitted_texts:
            if not fitted_text:
                continue
            cv2.putText(image, fitted_text, (text_x, pose_y), font, font_scale, (0, 0, 0), 2, cv2.LINE_AA)
            cv2.putText(image, fitted_text, (text_x, pose_y), font, font_scale, (0, 255, 255), font_thickness, cv2.LINE_AA)
            pose_y += line_h


def build_tag_overlay(
    image_shape: tuple[int, int],
    detections: _DetectionList,
    poses: list[tuple[int, float, float, float, float, float, float]],
) -> tuple[np.ndarray | None, np.ndarray | None, tuple[int, int, int, int] | None]:
    overlay = np.zeros((image_shape[0], image_shape[1], 3), dtype=np.uint8)
    draw_tag_detections(overlay, detections, poses)

    mask = overlay.any(axis=2).astype(np.uint8) * 255
    coords = cv2.findNonZero(mask)
    if coords is None:
        return None, None, None

    x, y, width, height = cv2.boundingRect(coords)
    return (
        overlay[y:y + height, x:x + width].copy(),
        mask[y:y + height, x:x + width].copy(),
        (x, y, width, height),
    )


def render_overlay_video_pass(session_dir: str, overlay_jsonl_path: str) -> str | None:
    """Render an overlay video from clean recording + saved overlay metadata."""
    manifest_path = os.path.join(session_dir, "manifest.json")
    if not os.path.isfile(manifest_path) or not os.path.isfile(overlay_jsonl_path):
        return None

    try:
        with open(manifest_path, "r", encoding="utf-8") as fp:
            manifest = json.load(fp)
    except Exception:
        return None

    clean_video_path = str(manifest.get("video_path", ""))
    frames_metadata_path = str(manifest.get("metadata_path", ""))
    if not clean_video_path or not os.path.isfile(clean_video_path):
        return None
    if not frames_metadata_path or not os.path.isfile(frames_metadata_path):
        return None

    recorded_frame_ids: list[int] = []
    try:
        with open(frames_metadata_path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                recorded_frame_ids.append(int(row.get("frame_id", len(recorded_frame_ids))))
    except Exception:
        return None

    overlay_by_frame_id: dict[int, tuple[_DetectionList, list[tuple[int, float, float, float, float, float, float]]]] = {}
    try:
        with open(overlay_jsonl_path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                frame_id = int(row.get("frame_id", -1))
                if frame_id < 0:
                    continue
                detections = deserialize_detections(row.get("detections", []))
                poses = [
                    (
                        int(p[0]),
                        float(p[1]),
                        float(p[2]),
                        float(p[3]),
                        float(p[4]),
                        float(p[5]),
                        float(p[6]),
                    )
                    for p in row.get("tag_poses", [])
                    if isinstance(p, (list, tuple)) and len(p) == 7
                ]
                overlay_by_frame_id[frame_id] = (detections, poses)
    except Exception:
        return None

    cap = cv2.VideoCapture(clean_video_path)
    if cap is None or not cap.isOpened():
        return None

    fps = float(cap.get(cv2.CAP_PROP_FPS))
    if fps <= 0.0:
        fps = 30.0
    width = int(max(1, cap.get(cv2.CAP_PROP_FRAME_WIDTH)))
    height = int(max(1, cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))

    output_path = os.path.join(session_dir, "video_overlay.mp4")
    writer = cv2.VideoWriter(
        output_path,
        cv2.VideoWriter_fourcc(*"mp4v"),
        float(fps),
        (width, height),
    )
    if not writer.isOpened():
        cap.release()
        try:
            writer.release()
        except Exception:
            pass
        return None

    frame_index = 0
    written_frames = 0
    try:
        while True:
            ok, frame = cap.read()
            if not ok or frame is None:
                break

            frame_id = recorded_frame_ids[frame_index] if frame_index < len(recorded_frame_ids) else frame_index
            overlay_payload = overlay_by_frame_id.get(int(frame_id))
            if overlay_payload is not None:
                detections, poses = overlay_payload
                draw_tag_detections(frame, detections, poses)

            writer.write(frame)
            frame_index += 1
            written_frames += 1
    finally:
        cap.release()
        writer.release()

    try:
        with open(os.path.join(session_dir, "overlay_manifest.json"), "w", encoding="utf-8") as fp:
            json.dump(
                {
                    "input_video": clean_video_path,
                    "overlay_jsonl": overlay_jsonl_path,
                    "output_video": output_path,
                    "frames_written": int(written_frames),
                    "created_wall_ns": int(time.time_ns()),
                },
                fp,
                indent=2,
            )
    except Exception:
        pass

    return output_path


def get_frame_timestamp(video_frame) -> str:
    """Extract timestamp from NDI frame or use current time if not available."""
    try:
        # Try to get NDI timecode if available
        if hasattr(video_frame, 'timecode') and video_frame.timecode is not None:
            # Timecode format varies; format as best we can
            tc = int(video_frame.timecode)
            frames = tc & 0xFF
            secs = (tc >> 8) & 0xFF
            mins = (tc >> 16) & 0xFF
            hours = (tc >> 24) & 0xFF
            return f"{hours:02d}:{mins:02d}:{secs:02d}.{frames:02d}"
    except Exception:
        pass
    
    # Fallback: use current time in readable format
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def extract_ndi_meta(video_frame, source_name: str) -> dict:
    """Extract metadata from a live NDI video_frame into a plain dict (call while frame is held)."""
    xres = int(video_frame.xres)
    yres = int(video_frame.yres)
    try:
        fps_n = int(video_frame.frame_rate_N)
        fps_d = int(video_frame.frame_rate_D)
        fps_str = f"{fps_n / fps_d:.3f}" if fps_d else "?"
    except Exception:
        fps_n, fps_d, fps_str = 0, 1, "?"
    fourcc = fourcc_name(video_frame)
    try:
        aspect = f"{float(video_frame.picture_aspect_ratio):.4f}"
    except Exception:
        aspect = "?"
    try:
        fmt_raw = str(video_frame.frame_format_type)
        frame_fmt = fmt_raw.split(".")[-1].replace("FRAME_FORMAT_TYPE_", "").capitalize()
    except Exception:
        frame_fmt = "?"
    try:
        tc = int(video_frame.timecode)
        # NDI timecodes are in 100-nanosecond units since epoch
        tc_sec = tc / 10_000_000.0
        hours = int(tc_sec // 3600)
        mins = int((tc_sec % 3600) // 60)
        secs = int(tc_sec % 60)
        ms = int((tc_sec % 1) * 1000)
        timecode = f"{hours:02d}:{mins:02d}:{secs:02d}.{ms:03d}"
    except Exception:
        timecode = "?"
    try:
        stride = int(video_frame.line_stride_in_bytes)
    except Exception:
        stride = 0
    return {
        "source": source_name,
        "xres": xres,
        "yres": yres,
        "fps_str": fps_str,
        "fps_n": fps_n,
        "fps_d": fps_d,
        "fourcc": fourcc,
        "aspect": aspect,
        "frame_fmt": frame_fmt,
        "timecode": timecode,
        "stride": stride,
    }


def draw_ndi_info_overlay(image: np.ndarray, ndi_meta: dict, recv_fps: float) -> None:
    """Draw video source metadata in the bottom-left corner (in-place)."""
    if not ndi_meta:
        return
    h, w = image.shape[:2]
    declared_fps = ndi_meta.get("fps_str", "?")
    lines = [
        f"FILE: {ndi_meta.get('source', '?')}",
        f"Res: {ndi_meta.get('xres', '?')}x{ndi_meta.get('yres', '?')}  Stride: {ndi_meta.get('stride', '?')}",
        f"Source FPS: {declared_fps}  Preview FPS: {recv_fps:.1f}",
        f"Backend: {ndi_meta.get('backend', '?')}  Codec: {ndi_meta.get('fourcc', '?')}  Field: {ndi_meta.get('field_order', '?')}  Deint: {ndi_meta.get('deinterlace', 'off')}",
        f"Fmt: {ndi_meta.get('frame_fmt', '?')}  Pos: {ndi_meta.get('timecode', '?')}",
    ]
    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = 0.35
    line_h = 13
    x = 8
    y_base = h - 8 - (len(lines) - 1) * line_h
    for i, line in enumerate(lines):
        y = y_base + i * line_h
        cv2.putText(image, line, (x, y), font, scale, (0, 0, 0), 2, cv2.LINE_AA)
        cv2.putText(image, line, (x, y), font, scale, (0, 200, 255), 1, cv2.LINE_AA)


def probe_video_stream_info(video_path: str) -> dict[str, str]:
    ffprobe_exe = shutil.which("ffprobe")
    if not ffprobe_exe:
        return {}

    try:
        completed = subprocess.run(
            [
                ffprobe_exe,
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=field_order,codec_name",
                "-of",
                "json",
                video_path,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5.0,
        )
    except Exception:
        return {}

    if completed.returncode != 0 or not completed.stdout.strip():
        return {}

    try:
        payload = json.loads(completed.stdout)
        streams = payload.get("streams") or []
        if not streams:
            return {}
        stream_info = streams[0] if isinstance(streams[0], dict) else {}
        return {
            "field_order": str(stream_info.get("field_order") or "").strip().lower(),
            "codec_name": str(stream_info.get("codec_name") or "").strip().lower(),
        }
    except Exception:
        return {}


def video_field_order_is_interlaced(field_order: str) -> bool:
    return str(field_order).strip().lower() in {"tt", "bb", "tb", "bt"}


def resolve_video_deinterlace_mode(requested_mode: str, field_order: str) -> str:
    mode = str(requested_mode).strip().lower() or "auto"
    if mode == "auto":
        return "blend" if video_field_order_is_interlaced(field_order) else "off"
    if mode in {"off", "blend"}:
        return mode
    return "off"


def deinterlace_frame_blend(frame: np.ndarray) -> np.ndarray:
    if frame.ndim < 2 or frame.shape[0] < 2:
        return frame.copy()

    result = frame.copy()
    if frame.shape[0] == 2:
        top = frame[0].astype(np.uint16)
        bottom = frame[1].astype(np.uint16)
        result[0] = ((top + bottom + 1) // 2).astype(frame.dtype)
        result[1] = result[0]
        return result

    upper = frame[:-2].astype(np.uint16)
    center = frame[1:-1].astype(np.uint16)
    lower = frame[2:].astype(np.uint16)
    result[1:-1] = ((upper + (center * 2) + lower + 2) // 4).astype(frame.dtype)
    result[0] = ((frame[0].astype(np.uint16) + frame[1].astype(np.uint16) + 1) // 2).astype(frame.dtype)
    result[-1] = ((frame[-2].astype(np.uint16) + frame[-1].astype(np.uint16) + 1) // 2).astype(frame.dtype)
    return result


def run_video_preview(
    video_path: str,
    no_display: bool,
    telemetry_interval: float,
    gpu_index: int,
    dict_names: list[str],
    show_timestamp: bool,
    focal_length: float = 1000.0,
    tag_size_mm: float = 148.6,
    analysis_workers: int = 0,
    display_fps: float = 30.0,
    display_scale: float = 0.5,
    display_prep_oversample: float = 4.0 / 3.0,
    display_delay_frames: int = 2,
    sync_timeout_ms: float = 33.0,
    freed_angle_scale: float = 32768.0,
    freed_listen_ip: str = "0.0.0.0",
    freed_port: int = 10244,
    mqtt_enable: bool = False,
    mqtt_host: str = "127.0.0.1",
    mqtt_port: int = 1883,
    mqtt_topic_prefix: str = "video/telemetry",
    parquet_enable: bool = True,
    parquet_output_dir: str = "recordings/telemetry",
    board_pose_stream_enable: bool = False,
    board_pose_stream_host: str = "0.0.0.0",
    board_pose_stream_port: int = 9102,
    board_pose_stream_hz: float = 50.0,
    raw_record_output_dir: str = "recordings",
    raw_record_backend: str = "ffmpeg",
    raw_record_ffmpeg_bin: str = "ffmpeg",
    raw_record_ffmpeg_encoder: str = "h264_nvenc",
    raw_record_ffmpeg_preset: str = "p5",
    raw_record_scale: float = 0.5,
    telemetry_record_start_enabled: bool = True,
    board_json_paths: list[str] | None = None,
    tag_size_map_json_path: str | None = None,
    april_tag_quad_decimate: float = 2.0,
    april_tag_quad_sigma: float = 0.0,
    adaptive_thresh_win_size_min: int = 3,
    adaptive_thresh_win_size_max: int = 23,
    corner_refinement_method: int | None = None,
    min_marker_perimeter_rate: float = 0.03,
    error_correction_rate: float = 0.6,
    april_tag_min_white_black_diff: int = 5,
    enable_board_refinement: bool = False,
    video_realtime: bool = True,
    video_loop: bool = False,
    video_deinterlace: str = "auto",
    interlaced_fast_profile: bool = False,
    video_decode_backend: str = "auto",
    video_decode_threads: int = 0,
    raw_record_start_enabled: bool = True,
    overlay_data_enable: bool = True,
) -> int:
    del freed_angle_scale, freed_listen_ip, freed_port

    source_name = os.path.basename(video_path)
    decode_backend_request = str(video_decode_backend).strip().lower() or "auto"
    decode_threads = max(0, int(video_decode_threads))
    if decode_backend_request not in {"auto", "opencv", "pyav"}:
        decode_backend_request = "auto"

    capture = None
    capture_backend = "unavailable"
    capture_hw_hint = False
    use_pyav = False
    pyav_codec_name = ""

    if decode_backend_request in {"auto", "opencv"}:
        capture, capture_backend, capture_hw_hint = open_video_capture(video_path)

    source_fps = 0.0
    source_frame_count = 0
    source_width = 0
    source_height = 0

    if decode_backend_request in {"auto", "pyav"} and _PYAV_AVAILABLE:
        try:
            pyav_probe = PyAvVideoDecoder(video_path, decode_threads=decode_threads)
            source_fps = float(pyav_probe.fps)
            source_frame_count = int(pyav_probe.frame_count)
            source_width = int(pyav_probe.width)
            source_height = int(pyav_probe.height)
            pyav_codec_name = pyav_probe.codec_name
            pyav_probe.release()
            use_pyav = True
            capture_backend = f"PYAV/{pyav_codec_name or '?'}"
            capture_hw_hint = True
            if capture is not None:
                capture.release()
                capture = None
        except Exception as exc:
            if decode_backend_request == "pyav":
                print(f"ERROR: Failed to open video with PyAV: {exc}")
                return 2
            print(f"Video decode: PyAV unavailable for this input ({exc}); falling back to OpenCV.")

    if not use_pyav:
        if capture is None or not capture.isOpened():
            print(f"ERROR: Failed to open video file: {video_path}")
            return 2
        source_fps = float(capture.get(cv2.CAP_PROP_FPS))
        source_frame_count = int(max(0, capture.get(cv2.CAP_PROP_FRAME_COUNT)))
        source_width = int(max(0, capture.get(cv2.CAP_PROP_FRAME_WIDTH)))
        source_height = int(max(0, capture.get(cv2.CAP_PROP_FRAME_HEIGHT)))

    if source_fps <= 0.0:
        source_fps = max(1.0, float(display_fps))
    effective_display_fps = min(max(1.0, float(display_fps)), source_fps)

    safe_source = "".join(ch if (ch.isalnum() or ch in ("-", "_")) else "_" for ch in os.path.splitext(source_name)[0])
    safe_source = safe_source.strip("_") or "source"
    session_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_dir = os.path.join(raw_record_output_dir, f"{session_stamp}_{safe_source}")
    stream_info = probe_video_stream_info(video_path)
    field_order = str(stream_info.get("field_order") or "unknown")
    stream_codec_name = str(stream_info.get("codec_name") or "")
    effective_deinterlace_mode = resolve_video_deinterlace_mode(video_deinterlace, field_order)
    interlaced_source = video_field_order_is_interlaced(field_order)

    effective_quad_decimate = float(april_tag_quad_decimate)
    effective_raw_record_scale = min(1.0, max(0.05, float(raw_record_scale)))
    effective_corner_refinement_method = corner_refinement_method
    if interlaced_fast_profile and (interlaced_source or effective_deinterlace_mode != "off"):
        effective_quad_decimate = max(effective_quad_decimate, 2.5)
        if effective_corner_refinement_method is None or int(effective_corner_refinement_method) == 3:
            effective_corner_refinement_method = 0

    print(f"Video input: {video_path}")
    print(
        f"Video decode: backend={capture_backend} hw_hint={'requested' if capture_hw_hint else 'not-requested'} "
        f"fps={source_fps:.3f} frames={source_frame_count if source_frame_count > 0 else '?'} size={source_width}x{source_height}"
    )
    print(f"Display timing: requested_fps={display_fps:.3f} effective_fps={effective_display_fps:.3f}")
    print(
        f"Video scan: field_order={field_order} interlaced={'yes' if interlaced_source else 'no'} "
        f"deinterlace={effective_deinterlace_mode}{f' codec={stream_codec_name}' if stream_codec_name else ''}"
    )
    if interlaced_fast_profile and (interlaced_source or effective_deinterlace_mode != "off"):
        print(
            "Interlaced fast detector profile: "
            f"quad_decimate={effective_quad_decimate:.2f} corner_refinement="
            f"{effective_corner_refinement_method if effective_corner_refinement_method is not None else 'default'}"
        )

    telemetry = NvidiaTelemetry(gpu_index=gpu_index, interval=telemetry_interval)
    telemetry.start()

    mqtt_pub = MqttPublisher(
        enabled=mqtt_enable,
        host=mqtt_host,
        port=mqtt_port,
        topic_prefix=mqtt_topic_prefix,
        client_id=f"video-preview-{os.getpid()}",
    )
    mqtt_pub.start()

    parquet_pub = ParquetTelemetryWriter(
        enabled=parquet_enable,
        output_dir=parquet_output_dir,
        source_name=source_name,
        session_dir=session_dir,
    )
    parquet_pub.start()

    overlay_writer = OverlayDataWriter(
        enabled=overlay_data_enable,
        session_dir=session_dir,
    )
    overlay_writer.start()

    def _publish_telemetry(topic_suffix: str, payload: dict) -> None:
        parquet_pub.publish(topic_suffix, payload)
        mqtt_pub.publish(topic_suffix, payload)

    board_pose_stream = None
    if board_pose_stream_enable:
        try:
            board_pose_module = importlib.import_module("board_pose_datastream")
            board_pose_stream_cls = getattr(board_pose_module, "BoardPoseDataStreamPublisher")
            board_pose_stream = board_pose_stream_cls(
                enabled=True,
                host=board_pose_stream_host,
                port=int(board_pose_stream_port),
                stream_hz=float(board_pose_stream_hz),
            )
            board_pose_stream.start()
        except Exception as exc:
            print(f"BoardPoseDataStream: unavailable; stream disabled ({exc})")
            board_pose_stream = None

    raw_recorder = RawFrameRecorder(
        base_output_dir=raw_record_output_dir,
        target_fps=effective_display_fps,
        source_name=source_name,
        backend=raw_record_backend,
        ffmpeg_bin=raw_record_ffmpeg_bin,
        ffmpeg_encoder=raw_record_ffmpeg_encoder,
        ffmpeg_preset=raw_record_ffmpeg_preset,
    )

    _telemetry_record_enabled = [bool(telemetry_record_start_enabled)]
    _raw_record_toggle_request = threading.Event()
    _raw_toggle_lock = threading.Lock()
    _raw_toggle_last_request_s = [0.0]
    _raw_toggle_min_interval_s = 0.35
    _raw_record_autostart_done = [False]

    def _toggle_telemetry_recording() -> bool:
        _telemetry_record_enabled[0] = not _telemetry_record_enabled[0]
        state = _telemetry_record_enabled[0]
        print(f"Telemetry recording: {'ENABLED' if state else 'DISABLED'}")
        return state

    def _is_telemetry_recording_enabled() -> bool:
        return bool(_telemetry_record_enabled[0])

    def _request_raw_record_toggle() -> None:
        now_s = time.perf_counter()
        with _raw_toggle_lock:
            if (now_s - _raw_toggle_last_request_s[0]) < _raw_toggle_min_interval_s:
                return
            _raw_toggle_last_request_s[0] = now_s
        _raw_record_toggle_request.set()

    def _build_detector_entries_local() -> list[tuple[str, object, object | None]]:
        detector_params = configure_detector_parameters(
            quad_decimate=effective_quad_decimate,
            quad_sigma=april_tag_quad_sigma,
            adaptive_thresh_win_size_min=adaptive_thresh_win_size_min,
            adaptive_thresh_win_size_max=adaptive_thresh_win_size_max,
            corner_refinement_method=effective_corner_refinement_method,
            min_marker_perimeter_rate=min_marker_perimeter_rate,
            error_correction_rate=error_correction_rate,
            april_tag_min_white_black_diff=april_tag_min_white_black_diff,
        )
        return build_aruco_detectors(dict_names, detector_params=detector_params)

    try:
        detector_entries = _build_detector_entries_local()
        print(f"ArUco dictionaries: {', '.join(name for name, _, _ in detector_entries)}")
    except (RuntimeError, ValueError) as exc:
        if capture is not None:
            capture.release()
        raw_recorder.shutdown()
        overlay_writer.stop()
        parquet_pub.stop()
        mqtt_pub.stop()
        telemetry.stop()
        if board_pose_stream is not None:
            board_pose_stream.stop()
        print(f"ERROR: {exc}")
        return 2

    board_definitions: list[dict] = []
    for board_json in (board_json_paths or []):
        try:
            board_definition = load_board_definition(board_json)
            board_definitions.append(board_definition)
        except Exception as exc:
            if capture is not None:
                capture.release()
            raw_recorder.shutdown()
            overlay_writer.stop()
            parquet_pub.stop()
            mqtt_pub.stop()
            telemetry.stop()
            if board_pose_stream is not None:
                board_pose_stream.stop()
            print(f"ERROR: Failed to load board definition '{board_json}': {exc}")
            return 2

    effective_tag_size_mm_by_id: dict[int, float] = {}
    if board_definitions:
        print("Board pose: enabled for boards:")
        for board_definition in board_definitions:
            board_dict_name = board_definition.get("dict_name") or f"DICT_ID_{board_definition['dictionary_id']}"
            board_tag_size = board_definition.get("tag_size_mm")
            tag_size_str = (
                f", tag_size_mm={float(board_tag_size):.3f}"
                if board_tag_size is not None
                else ""
            )
            print(
                f"  - {board_definition['name']} (dict={board_dict_name}, tags={len(board_definition['ids'])}{tag_size_str})"
            )
            for tag_id, size_mm in board_definition.get("tag_size_mm_by_id", {}).items():
                tid = int(tag_id)
                if tid in effective_tag_size_mm_by_id and abs(float(effective_tag_size_mm_by_id[tid]) - float(size_mm)) > 1e-6:
                    print(
                        f"Warning: conflicting tag_size_mm for tag ID {tid} across boards "
                        f"({effective_tag_size_mm_by_id[tid]:.3f} vs {float(size_mm):.3f}); keeping first value."
                    )
                    continue
                effective_tag_size_mm_by_id[tid] = float(size_mm)

    if tag_size_map_json_path:
        try:
            override_tag_sizes = load_tag_size_map_json(tag_size_map_json_path)
        except Exception as exc:
            if capture is not None:
                capture.release()
            raw_recorder.shutdown()
            overlay_writer.stop()
            parquet_pub.stop()
            mqtt_pub.stop()
            telemetry.stop()
            if board_pose_stream is not None:
                board_pose_stream.stop()
            print(f"ERROR: Failed to load tag size map '{tag_size_map_json_path}': {exc}")
            return 2
        effective_tag_size_mm_by_id.update(override_tag_sizes)

    title = f"Video GPU Preview with AprilTags - {source_name}"
    _quit = threading.Event()
    _capture_done = threading.Event()
    _use_gl = False
    _glfw_win = None
    _gl_tex_id = None
    _gl_tex_size = (0, 0)

    def _ensure_display_ready() -> None:
        nonlocal _use_gl, _glfw_win, _gl_tex_id, _gl_tex_size

        if no_display:
            return
        if _glfw_win is not None or (_use_gl is False and _gl_tex_id == "opencv"):
            return

        if _GL_AVAILABLE:
            try:
                if not glfw.init():
                    raise RuntimeError("glfw.init() failed")
                glfw.window_hint(glfw.CONTEXT_VERSION_MAJOR, 2)
                glfw.window_hint(glfw.CONTEXT_VERSION_MINOR, 1)
                glfw.window_hint(glfw.DOUBLEBUFFER, 1)
                _glfw_win = glfw.create_window(max(320, source_width), max(240, source_height), title, None, None)
                if _glfw_win is None:
                    raise RuntimeError("glfw.create_window failed")
                glfw.make_context_current(_glfw_win)
                glfw.swap_interval(0)

                def _on_glfw_key(win, key, sc, action, mods):
                    if action != glfw.PRESS:
                        return
                    if key in (glfw.KEY_ESCAPE, glfw.KEY_Q):
                        _quit.set()
                        return
                    if key == glfw.KEY_T:
                        _toggle_telemetry_recording()
                    if key == glfw.KEY_R:
                        _request_raw_record_toggle()

                glfw.set_key_callback(_glfw_win, _on_glfw_key)
                _gl_tex_id = glGenTextures(1)
                glBindTexture(GL_TEXTURE_2D, _gl_tex_id)
                glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR)
                glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR)
                glMatrixMode(GL_PROJECTION)
                glLoadIdentity()
                glOrtho(-1, 1, -1, 1, -1, 1)
                glMatrixMode(GL_MODELVIEW)
                glLoadIdentity()
                _use_gl = True
                print("Display: GLFW + OpenGL (direct GPU texture upload, GL_BGR).")
                return
            except Exception as exc:
                print(f"Display: GLFW/OpenGL unavailable ({exc}); falling back to OpenCV window.")
                _use_gl = False
                if _glfw_win is not None:
                    glfw.terminate()
                    _glfw_win = None

        cv2.namedWindow(title, cv2.WINDOW_NORMAL)
        _gl_tex_id = "opencv"
        print("Display: OpenCV standard window.")

    def _reopen_capture() -> tuple[cv2.VideoCapture | None, str, bool]:
        reopened_capture, reopened_backend, reopened_hw_hint = open_video_capture(video_path)
        if reopened_capture is None or not reopened_capture.isOpened():
            return None, "", False
        print(
            f"Video loop: restarted {source_name} using backend={reopened_backend} "
            f"hw_hint={'requested' if reopened_hw_hint else 'not-requested'}"
        )
        return reopened_capture, reopened_backend, reopened_hw_hint

    if telemetry.available:
        sources = []
        if telemetry.nvidia_available:
            sources.append("nvidia-smi")
        if telemetry.windows_counter_available:
            sources.append("windows-gpu-engine")
        print(f"NVIDIA telemetry: enabled ({', '.join(sources)}).")
    else:
        print("NVIDIA telemetry: unavailable (no nvidia-smi or Windows GPU engine counter).")

    cpu_count = max(1, os.cpu_count() or 1)
    worker_count = analysis_workers if analysis_workers > 0 else min(cpu_count, 8)
    worker_count = max(1, worker_count)
    print(f"Analysis workers: {worker_count} (cpu_count={cpu_count})")
    if not no_display:
        print(
            f"Display sync: fps={effective_display_fps:.1f} scale={display_scale:.2f} "
            f"prep_oversample={max(1.0, display_prep_oversample):.2f} delay={max(0, display_delay_frames)}f timeout={max(0.0, sync_timeout_ms):.0f}ms "
            f"record_scale={effective_raw_record_scale:.2f}"
        )
        _ensure_display_ready()

    frame_queue: queue.Queue[tuple[int, np.ndarray, dict, str, int, int] | None] = queue.Queue(maxsize=max(8, worker_count * 3))
    _result_lock = threading.Lock()
    _completed_ids: list[int] = []
    _results_by_id: dict[int, dict] = {}
    _display_lock = threading.Lock()
    _latest_display_packet: list[tuple[int, np.ndarray, np.ndarray, dict, str, int, int] | None] = [None]
    _last_captured_frame_id = [-1]
    _stats_lock = threading.Lock()
    _stats = {
        "capture_fps": 0.0,
        "analyze_fps": 0.0,
        "analyze_frames": 0,
        "analyze_tick": time.perf_counter(),
        "output_fps": 0.0,
        "output_frames": 0,
        "output_tick": time.perf_counter(),
        "last_log": 0.0,
    }

    def _update_output_fps() -> float:
        now_local = time.perf_counter()
        with _stats_lock:
            _stats["output_frames"] += 1
            elapsed = now_local - _stats["output_tick"]
            if elapsed >= 1.0:
                _stats["output_fps"] = _stats["output_frames"] / elapsed
                _stats["output_frames"] = 0
                _stats["output_tick"] = now_local
            return float(_stats["output_fps"])

    def _publish_replay_tag_and_board_telemetry(
        frame_id_local: int,
        replay_wall_ns: int,
        replay_mono_ns: int,
        frame_ts: str,
        camera_matrix_local: np.ndarray,
        result: dict,
    ) -> None:
        tag_poses = result["tag_poses"]
        detections = result["detections"]
        tag_image_metrics = result["tag_image_metrics"]
        tag_quaternions = result["tag_quaternions"]

        if not _is_telemetry_recording_enabled() or not tag_poses:
            return

        for tid, x, y, z, roll, pitch, yaw in tag_poses:
            tag_size_used_mm = float(effective_tag_size_mm_by_id.get(int(tid), tag_size_mm))
            img_m = tag_image_metrics.get(int(tid), {})
            quat_w, quat_x, quat_y, quat_z = tag_quaternions.get(int(tid), euler_deg_to_quaternion(roll, pitch, yaw))
            _publish_telemetry(
                "tag_pose",
                {
                    "type": "tag_pose",
                    "source": source_name,
                    "ts_wall_ns": int(replay_wall_ns),
                    "ts_mono_ns": int(replay_mono_ns),
                    "source_ts": str(frame_ts),
                    "ingest_seq": int(frame_id_local),
                    "frame_id": int(frame_id_local),
                    "tag_id": int(tid),
                    "tag_size_mm": round(tag_size_used_mm, 3),
                    "x_mm": round(float(x), 3),
                    "y_mm": round(float(y), 3),
                    "z_mm": round(float(z), 3),
                    "roll_deg": round(float(roll), 3),
                    "pitch_deg": round(float(pitch), 3),
                    "yaw_deg": round(float(yaw), 3),
                    "quat_w": round(float(quat_w), 6),
                    "quat_x": round(float(quat_x), 6),
                    "quat_y": round(float(quat_y), 6),
                    "quat_z": round(float(quat_z), 6),
                    **img_m,
                },
            )

        if not board_definitions:
            return

        for board_definition in board_definitions:
            matched_tag_count = count_board_tag_matches(detections, board_definition)
            tag_subset_pose = estimate_board_pose_from_tag_subset(
                tag_poses=tag_poses,
                board_ids=board_definition["ids_set"],
                board_expected_count=len(board_definition["ids"]),
            )
            board_method = "aruco_board"
            board_pose = estimate_board_pose_from_detections(
                detections=detections,
                board_definition=board_definition,
                camera_matrix=camera_matrix_local,
            )
            if board_pose is None:
                board_pose = tag_subset_pose
                board_method = "median_tags"
            if board_pose is None:
                continue

            board_x, board_y, board_z, board_roll, board_pitch, board_yaw, board_tag_count, board_tag_expected = board_pose
            quat_w, quat_x, quat_y, quat_z = euler_deg_to_quaternion(board_roll, board_pitch, board_yaw)
            _publish_telemetry(
                "board_pose",
                {
                    "type": "board_pose",
                    "board_name": str(board_definition["name"]),
                    "method": board_method,
                    "aruco_solve_ok": int(board_method == "aruco_board"),
                    "source": source_name,
                    "ts_wall_ns": int(replay_wall_ns),
                    "ts_mono_ns": int(replay_mono_ns),
                    "source_ts": str(frame_ts),
                    "ingest_seq": int(frame_id_local),
                    "frame_id": int(frame_id_local),
                    "matched_tag_count": int(matched_tag_count),
                    "tag_count": int(board_tag_count),
                    "tag_expected": int(board_tag_expected),
                    "x_mm": round(float(board_x), 3),
                    "y_mm": round(float(board_y), 3),
                    "z_mm": round(float(board_z), 3),
                    "roll_deg": round(float(board_roll), 3),
                    "pitch_deg": round(float(board_pitch), 3),
                    "yaw_deg": round(float(board_yaw), 3),
                    "quat_w": round(float(quat_w), 6),
                    "quat_x": round(float(quat_x), 6),
                    "quat_y": round(float(quat_y), 6),
                    "quat_z": round(float(quat_z), 6),
                },
            )
            if board_pose_stream is not None:
                board_pose_stream.publish_board_pose(
                    x_mm=float(board_x),
                    y_mm=float(board_y),
                    z_mm=float(board_z),
                    roll_deg=float(board_roll),
                    pitch_deg=float(board_pitch),
                    yaw_deg=float(board_yaw),
                )

    def analyze_loop(worker_index: int) -> None:
        del worker_index
        camera_matrix_local = None
        detector_entries_local = _build_detector_entries_local()

        while not _quit.is_set():
            try:
                packet = frame_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if packet is None:
                break

            frame_id_local, analysis_bgr, ndi_meta, frame_ts, frame_recv_wall_ns, frame_recv_mono_ns = packet
            gray = cv2.cvtColor(analysis_bgr, cv2.COLOR_BGR2GRAY)

            if camera_matrix_local is None:
                h_gray, w_gray = gray.shape[:2]
                camera_matrix_local = np.array(
                    [[focal_length, 0, w_gray / 2.0], [0, focal_length, h_gray / 2.0], [0, 0, 1]],
                    dtype=np.float32,
                )

            tag_count, tag_poses, detections, tag_image_metrics, tag_quaternions = detect_tags(
                gray,
                detector_entries_local,
                camera_matrix_local,
                tag_size_mm,
                board_definitions=board_definitions,
                enable_board_refinement=enable_board_refinement,
                tag_size_mm_by_id=effective_tag_size_mm_by_id,
            )

            if overlay_data_enable and tag_count > 0:
                overlay_writer.publish(
                    {
                        "frame_id": int(frame_id_local),
                        "ts_wall_ns": int(frame_recv_wall_ns),
                        "ts_mono_ns": int(frame_recv_mono_ns),
                        "tag_count": int(tag_count),
                        "tag_poses": [
                            [
                                int(p[0]),
                                float(p[1]),
                                float(p[2]),
                                float(p[3]),
                                float(p[4]),
                                float(p[5]),
                                float(p[6]),
                            ]
                            for p in tag_poses
                        ],
                        "detections": serialize_detections(detections),
                    }
                )

            with _result_lock:
                _results_by_id[frame_id_local] = {
                    "tag_count": int(tag_count),
                    "tag_poses": tag_poses,
                    "detections": detections,
                    "ndi_meta": dict(ndi_meta),
                    "frame_ts": str(frame_ts),
                    "frame_recv_wall_ns": int(frame_recv_wall_ns),
                    "frame_recv_mono_ns": int(frame_recv_mono_ns),
                    "tag_image_metrics": tag_image_metrics,
                    "tag_quaternions": tag_quaternions,
                }
                bisect.insort(_completed_ids, frame_id_local)
                if len(_results_by_id) > 300:
                    cutoff = frame_id_local - 240
                    stale = [fid for fid in _results_by_id if fid < cutoff]
                    for fid in stale:
                        del _results_by_id[fid]
                    prune_idx = bisect.bisect_left(_completed_ids, cutoff)
                    del _completed_ids[:prune_idx]

            now_local = time.perf_counter()
            with _stats_lock:
                _stats["analyze_frames"] += 1
                elapsed = now_local - _stats["analyze_tick"]
                if elapsed >= 1.0:
                    _stats["analyze_fps"] = _stats["analyze_frames"] / elapsed
                    _stats["analyze_frames"] = 0
                    _stats["analyze_tick"] = now_local
                analyze_fps = float(_stats["analyze_fps"])
                capture_fps = float(_stats["capture_fps"])
                output_fps = float(_stats["output_fps"])
                should_log = (now_local - _stats["last_log"]) >= 1.0
                if should_log:
                    _stats["last_log"] = now_local

            _publish_replay_tag_and_board_telemetry(
                frame_id_local=frame_id_local,
                replay_wall_ns=frame_recv_wall_ns,
                replay_mono_ns=frame_recv_mono_ns,
                frame_ts=frame_ts,
                camera_matrix_local=camera_matrix_local,
                result=_results_by_id[frame_id_local],
            )

            if should_log and _is_telemetry_recording_enabled():
                _publish_telemetry(
                    "stats",
                    {
                        "type": "stats",
                        "source": source_name,
                        "ts_wall_ns": int(frame_recv_wall_ns),
                        "ts_mono_ns": int(frame_recv_mono_ns),
                        "source_ts": str(frame_ts),
                        "ingest_seq": int(frame_id_local),
                        "worker_count": int(worker_count),
                        "queue_depth": int(frame_queue.qsize()),
                        "capture_fps": round(capture_fps, 3),
                        "analyze_fps": round(analyze_fps, 3),
                        "output_fps": round(output_fps, 3),
                        "tag_count": int(tag_count),
                        "telemetry_available": int(bool(telemetry.available)),
                        "decoder_util": float(telemetry.decoder_util) if telemetry.decoder_util is not None else -1.0,
                        "windows_decode_util": float(telemetry.windows_decode_util) if telemetry.windows_decode_util is not None else -1.0,
                        "gpu_util": float(telemetry.gpu_util) if telemetry.gpu_util is not None else -1.0,
                        "mem_util": float(telemetry.mem_util) if telemetry.mem_util is not None else -1.0,
                    },
                )

    def capture_loop() -> None:
        capture_local = capture
        capture_backend_local = capture_backend
        capture_fourcc = _decode_fourcc(capture_local.get(cv2.CAP_PROP_FOURCC)) if capture_local is not None else (stream_codec_name or pyav_codec_name or "?")
        frame_id_local = 0
        frame_counter = 0
        tick = time.perf_counter()

        def _process_source_frame(
            source_bgr_local: np.ndarray,
            frame_index_local: int,
            media_ts_ms_local: float,
            backend_local: str,
            fourcc_local: str,
        ) -> bool:
            nonlocal frame_id_local, frame_counter, tick

            analysis_bgr = source_bgr_local
            if effective_deinterlace_mode == "blend":
                analysis_bgr = deinterlace_frame_blend(source_bgr_local)

            if _raw_record_toggle_request.is_set():
                _raw_record_toggle_request.clear()
                if raw_recorder.is_recording():
                    raw_recorder.stop_recording()
                else:
                    record_width = max(1, int(round(source_bgr_local.shape[1] * effective_raw_record_scale)))
                    record_height = max(1, int(round(source_bgr_local.shape[0] * effective_raw_record_scale)))
                    raw_recorder.start_recording(
                        frame_width=record_width,
                        frame_height=record_height,
                        session_dir=session_dir,
                    )

            if raw_record_start_enabled and not _raw_record_autostart_done[0]:
                _raw_record_autostart_done[0] = True
                record_width = max(1, int(round(source_bgr_local.shape[1] * effective_raw_record_scale)))
                record_height = max(1, int(round(source_bgr_local.shape[0] * effective_raw_record_scale)))
                raw_recorder.start_recording(
                    frame_width=record_width,
                    frame_height=record_height,
                    session_dir=session_dir,
                )

            frame_recv_wall_ns = int(time.time_ns())
            frame_recv_mono_ns = int(time.perf_counter_ns())
            ndi_meta = build_video_meta(
                source_name=source_name,
                capture_backend=backend_local,
                frame_index=frame_index_local,
                frame_count=source_frame_count,
                media_ts_ms=media_ts_ms_local,
                xres=int(source_bgr_local.shape[1]),
                yres=int(source_bgr_local.shape[0]),
                fps=float(source_fps),
                fourcc=fourcc_local,
            )
            ndi_meta["field_order"] = field_order
            ndi_meta["deinterlace"] = effective_deinterlace_mode
            if stream_codec_name:
                ndi_meta["codec_name"] = stream_codec_name
            frame_ts = ndi_meta["timecode"]

            while not _quit.is_set():
                try:
                    frame_queue.put((frame_id_local, analysis_bgr, ndi_meta, frame_ts, frame_recv_wall_ns, frame_recv_mono_ns), timeout=0.1)
                    break
                except queue.Full:
                    continue

            if _quit.is_set():
                return False

            if raw_recorder.is_recording():
                record_frame = source_bgr_local
                if effective_raw_record_scale != 1.0:
                    record_width = max(1, int(round(source_bgr_local.shape[1] * effective_raw_record_scale)))
                    record_height = max(1, int(round(source_bgr_local.shape[0] * effective_raw_record_scale)))
                    record_frame = cv2.resize(source_bgr_local, (record_width, record_height), interpolation=cv2.INTER_AREA)
                raw_recorder.enqueue_frame(
                    record_frame,
                    {
                        "frame_id": int(frame_id_local),
                        "ts_wall_ns": int(frame_recv_wall_ns),
                        "ts_mono_ns": int(frame_recv_mono_ns),
                        "source_ts": str(frame_ts),
                        "source_name": source_name,
                        "field_order": field_order,
                        "analysis_deinterlace": effective_deinterlace_mode,
                        "tag_count": -1,
                        "record_scale": float(effective_raw_record_scale),
                        "recorded_wall_ns": int(time.time_ns()),
                    },
                )

            if not no_display:
                display_bgr = analysis_bgr
                if display_scale != 1.0:
                    resized_width = max(1, int(round(display_bgr.shape[1] * display_scale)))
                    resized_height = max(1, int(round(display_bgr.shape[0] * display_scale)))
                    interpolation = cv2.INTER_AREA if display_scale < 1.0 else cv2.INTER_LINEAR
                    display_bgr = cv2.resize(display_bgr, (resized_width, resized_height), interpolation=interpolation)
                else:
                    display_bgr = display_bgr.copy()
                with _display_lock:
                    _latest_display_packet[0] = (
                        frame_id_local,
                        display_bgr,
                        source_bgr_local.copy(),
                        dict(ndi_meta),
                        frame_ts,
                        frame_recv_wall_ns,
                        frame_recv_mono_ns,
                    )

            _last_captured_frame_id[0] = frame_id_local
            frame_counter += 1
            now_local = time.perf_counter()
            elapsed = now_local - tick
            if elapsed >= 1.0:
                with _stats_lock:
                    _stats["capture_fps"] = frame_counter / elapsed
                frame_counter = 0
                tick = now_local
            frame_id_local += 1
            return True

        try:
            if use_pyav:
                while not _quit.is_set():
                    decoder = None
                    try:
                        decoder = PyAvVideoDecoder(video_path, decode_threads=decode_threads)
                        decode_fourcc = decoder.codec_name or stream_codec_name or pyav_codec_name or "?"
                        decode_backend = f"PYAV/{decode_fourcc}"
                        while not _quit.is_set():
                            ok, source_bgr, frame_index, media_ts_ms = decoder.read()
                            if not ok or source_bgr is None:
                                break
                            if not _process_source_frame(source_bgr, int(frame_index), float(media_ts_ms), decode_backend, decode_fourcc):
                                break
                    finally:
                        if decoder is not None:
                            decoder.release()

                    if _quit.is_set() or not video_loop:
                        break
                    print(f"Video loop: restarted {source_name} using backend=PYAV")
            else:
                while not _quit.is_set():
                    ok, source_bgr = capture_local.read()
                    if not ok or source_bgr is None:
                        if video_loop:
                            try:
                                capture_local.release()
                            except Exception:
                                pass
                            reopened_capture, capture_backend_local, _ = _reopen_capture()
                            if reopened_capture is None:
                                break
                            capture_local = reopened_capture
                            capture_fourcc = _decode_fourcc(capture_local.get(cv2.CAP_PROP_FOURCC))
                            continue
                        break

                    frame_index = int(max(0.0, capture_local.get(cv2.CAP_PROP_POS_FRAMES) - 1.0))
                    media_ts_ms = float(capture_local.get(cv2.CAP_PROP_POS_MSEC))
                    if media_ts_ms < 0.0:
                        media_ts_ms = (frame_index / max(1.0, source_fps)) * 1000.0

                    if not _process_source_frame(source_bgr, frame_index, media_ts_ms, capture_backend_local, capture_fourcc):
                        break
        finally:
            if capture_local is not None:
                try:
                    capture_local.release()
                except Exception:
                    pass
            _capture_done.set()
            for _ in range(worker_count):
                while True:
                    try:
                        frame_queue.put_nowait(None)
                        break
                    except queue.Full:
                        try:
                            frame_queue.get_nowait()
                        except queue.Empty:
                            break

    capture_thread = threading.Thread(target=capture_loop, daemon=True, name="video-capture")
    analyze_threads = [
        threading.Thread(target=analyze_loop, args=(i,), daemon=True, name=f"video-analyze-{i}")
        for i in range(worker_count)
    ]

    capture_thread.start()
    for thread in analyze_threads:
        thread.start()

    try:
        next_display = time.perf_counter()

        while not _quit.is_set():
            if no_display:
                if _capture_done.is_set() and not any(thread.is_alive() for thread in analyze_threads):
                    break
                time.sleep(0.05)
                continue

            if _use_gl:
                glfw.poll_events()
                if glfw.window_should_close(_glfw_win):
                    _quit.set()
                    break
            else:
                key = cv2.pollKey() & 0xFF
                if key in (27, ord("q")):
                    _quit.set()
                    break
                if key in (ord("t"), ord("T")):
                    _toggle_telemetry_recording()
                if key in (ord("r"), ord("R")):
                    _request_raw_record_toggle()

            now_local = time.perf_counter()
            if video_realtime:
                if now_local + 0.0002 < next_display:
                    time.sleep(max(0.0, next_display - now_local - 0.0002))
                    continue
                if now_local - next_display > (1.0 / max(1.0, effective_display_fps)) * 3.0:
                    next_display = now_local
                else:
                    next_display += 1.0 / max(1.0, effective_display_fps)

            with _display_lock:
                display_packet = _latest_display_packet[0]

            if display_packet is None:
                if _capture_done.is_set() and not any(thread.is_alive() for thread in analyze_threads):
                    break
                time.sleep(0.005)
                continue

            frame_id_local, display_bgr, _raw_bgr, ndi_meta, frame_ts, frame_recv_wall_ns, frame_recv_mono_ns = display_packet

            with _result_lock:
                result = _results_by_id.get(frame_id_local)
                used_result_id = frame_id_local
                if result is None:
                    idx = bisect.bisect_right(_completed_ids, frame_id_local) - 1
                    if idx >= 0:
                        used_result_id = _completed_ids[idx]
                        result = _results_by_id.get(used_result_id)

            overlay_age_frames = -1 if result is None else frame_id_local - used_result_id
            overlay_age_ms = -1.0 if result is None else max(0.0, (frame_recv_mono_ns - int(result["frame_recv_mono_ns"])) / 1_000_000.0)
            tag_count = 0 if result is None else int(result["tag_count"])

            image = display_bgr.copy()
            if result is not None:
                display_detections = scale_detections_for_display(result["detections"], display_scale)
                tag_overlay_roi, tag_overlay_mask, tag_overlay_rect = build_tag_overlay(
                    (image.shape[0], image.shape[1]),
                    display_detections,
                    result["tag_poses"],
                )
                if tag_overlay_rect is not None and tag_overlay_roi is not None and tag_overlay_mask is not None:
                    x, y, width_roi, height_roi = tag_overlay_rect
                    cv2.copyTo(tag_overlay_roi, tag_overlay_mask, image[y:y + height_roi, x:x + width_roi])

            cx_r, cy_r = image.shape[1] // 2, image.shape[0] // 2
            cv2.line(image, (cx_r - 20, cy_r), (cx_r + 20, cy_r), (0, 255, 0), 1)
            cv2.line(image, (cx_r, cy_r - 20), (cx_r, cy_r + 20), (0, 255, 0), 1)
            cv2.circle(image, (cx_r, cy_r), 2, (0, 255, 0), -1)

            current_output_fps = _update_output_fps()
            with _stats_lock:
                capture_fps = float(_stats["capture_fps"])
                analyze_fps = float(_stats["analyze_fps"])

            hud_lines = [
                (f"CapFPS: {capture_fps:.1f} | AnaFPS: {analyze_fps:.1f} | OutFPS: {current_output_fps:.1f} | Tags: {tag_count}", (0, 255, 0)),
                (f"Telemetry Rec: {'ON' if _is_telemetry_recording_enabled() else 'OFF'} (press T)", (0, 255, 255)),
                (f"Raw Rec: {'ON' if raw_recorder.is_recording() else 'OFF'} (press R)", (0, 255, 255)),
            ]
            if overlay_age_frames >= 0:
                hud_lines.append((f"SyncAge: {overlay_age_frames}f ({overlay_age_ms:.1f}ms)", (0, 255, 255)))
            if show_timestamp:
                hud_lines.insert(0, (f"Local: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}", (0, 255, 0)))
            if telemetry.available:
                hud_lines.append((
                    f"GPU: {telemetry.gpu_util if telemetry.gpu_util is not None else -1:.0f}% "
                    f"DecNv: {telemetry.decoder_util if telemetry.decoder_util is not None else -1:.0f}% "
                    f"DecWin: {telemetry.windows_decode_util if telemetry.windows_decode_util is not None else -1:.0f}%({telemetry.windows_decode_scope})",
                    (0, 255, 0),
                ))

            y = 18
            for text, color in hud_lines:
                cv2.putText(image, text, (8, y), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (0, 0, 0), 2, cv2.LINE_AA)
                cv2.putText(image, text, (8, y), cv2.FONT_HERSHEY_SIMPLEX, 0.45, color, 1, cv2.LINE_AA)
                y += 16

            draw_ndi_info_overlay(image, ndi_meta, current_output_fps if current_output_fps > 0.0 else effective_display_fps)

            if _use_gl:
                img_c = np.ascontiguousarray(image)
                h_f, w_f = img_c.shape[:2]
                if _gl_tex_size == (0, 0):
                    glfw.set_window_size(_glfw_win, w_f, h_f)
                glBindTexture(GL_TEXTURE_2D, _gl_tex_id)
                if _gl_tex_size != (w_f, h_f):
                    glTexImage2D(GL_TEXTURE_2D, 0, GL_RGB, w_f, h_f, 0, GL_BGR, GL_UNSIGNED_BYTE, img_c)
                    _gl_tex_size = (w_f, h_f)
                else:
                    glTexSubImage2D(GL_TEXTURE_2D, 0, 0, 0, w_f, h_f, GL_BGR, GL_UNSIGNED_BYTE, img_c)
                vw, vh = glfw.get_framebuffer_size(_glfw_win)
                glViewport(0, 0, vw, vh)
                glClear(GL_COLOR_BUFFER_BIT)
                glEnable(GL_TEXTURE_2D)
                glBegin(GL_TRIANGLE_STRIP)
                glTexCoord2f(0.0, 0.0); glVertex2f(-1.0,  1.0)
                glTexCoord2f(1.0, 0.0); glVertex2f( 1.0,  1.0)
                glTexCoord2f(0.0, 1.0); glVertex2f(-1.0, -1.0)
                glTexCoord2f(1.0, 1.0); glVertex2f( 1.0, -1.0)
                glEnd()
                glDisable(GL_TEXTURE_2D)
                glfw.swap_buffers(_glfw_win)
            else:
                cv2.imshow(title, image)

            if _capture_done.is_set() and not any(thread.is_alive() for thread in analyze_threads) and frame_id_local >= _last_captured_frame_id[0]:
                break
    except KeyboardInterrupt:
        _quit.set()
    finally:
        _quit.set()
        capture_thread.join(timeout=2.0)
        for thread in analyze_threads:
            thread.join(timeout=2.0)
        raw_recorder.shutdown()
        overlay_jsonl_path, overlay_dropped = overlay_writer.stop()
        if overlay_jsonl_path and os.path.isfile(overlay_jsonl_path):
            overlay_video_path = render_overlay_video_pass(session_dir=session_dir, overlay_jsonl_path=overlay_jsonl_path)
            if overlay_video_path:
                print(f"Overlay pass: wrote {overlay_video_path}")
            else:
                print("Overlay pass: skipped (missing inputs or render failure)")
            if overlay_dropped > 0:
                print(f"OverlayData: dropped {overlay_dropped} rows due to queue pressure")
        if board_pose_stream is not None:
            board_pose_stream.stop()
        parquet_pub.stop()
        mqtt_pub.stop()
        telemetry.stop()
        if not no_display:
            if _use_gl and _glfw_win is not None:
                glfw.terminate()
            else:
                cv2.destroyAllWindows()

    return 0


def run_preview(
    source,
    no_display: bool,
    telemetry_interval: float,
    gpu_index: int,
    dict_names: list[str],
    show_timestamp: bool,
    focal_length: float = 1000.0,
    tag_size_mm: float = 148.6,
    analysis_workers: int = 0,
    display_fps: float = 30.0,
    display_scale: float = 0.5,
    display_prep_oversample: float = 4.0 / 3.0,
    display_delay_frames: int = 2,
    sync_timeout_ms: float = 33.0,
    freed_angle_scale: float = 32768.0,
    freed_listen_ip: str = "0.0.0.0",
    freed_port: int = 10244,
    mqtt_enable: bool = False,
    mqtt_host: str = "127.0.0.1",
    mqtt_port: int = 1883,
    mqtt_topic_prefix: str = "ndi/telemetry",
    board_pose_stream_enable: bool = False,
    board_pose_stream_host: str = "0.0.0.0",
    board_pose_stream_port: int = 9102,
    board_pose_stream_hz: float = 50.0,
    raw_record_output_dir: str = "recordings",
    raw_record_backend: str = "ffmpeg",
    raw_record_ffmpeg_bin: str = "ffmpeg",
    raw_record_ffmpeg_encoder: str = "h264_nvenc",
    raw_record_ffmpeg_preset: str = "p5",
    telemetry_record_start_enabled: bool = True,
    board_json_paths: list[str] | None = None,
    tag_size_map_json_path: str | None = None,
    # Detector tuning parameters
    april_tag_quad_decimate: float = 2.0,
    april_tag_quad_sigma: float = 0.0,
    adaptive_thresh_win_size_min: int = 3,
    adaptive_thresh_win_size_max: int = 23,
    corner_refinement_method: int | None = None,
    min_marker_perimeter_rate: float = 0.03,
    error_correction_rate: float = 0.6,
    april_tag_min_white_black_diff: int = 5,
    enable_board_refinement: bool = False,
) -> int:
    source_name = source.ndi_name
    recv = create_receiver("python-hx3-gpu-preview-apriltag")
    ndi.recv_connect(recv, source)

    telemetry = NvidiaTelemetry(gpu_index=gpu_index, interval=telemetry_interval)
    telemetry.start()

    mqtt_pub = MqttPublisher(
        enabled=mqtt_enable,
        host=mqtt_host,
        port=mqtt_port,
        topic_prefix=mqtt_topic_prefix,
        client_id=f"ndi-preview-{os.getpid()}",
    )
    mqtt_pub.start()

    board_pose_stream = None
    if board_pose_stream_enable:
        try:
            board_pose_module = importlib.import_module("board_pose_datastream")
            board_pose_stream_cls = getattr(board_pose_module, "BoardPoseDataStreamPublisher")
            board_pose_stream = board_pose_stream_cls(
                enabled=True,
                host=board_pose_stream_host,
                port=int(board_pose_stream_port),
                stream_hz=float(board_pose_stream_hz),
            )
            board_pose_stream.start()
        except Exception as exc:
            print(f"BoardPoseDataStream: unavailable; stream disabled ({exc})")
            board_pose_stream = None

    raw_recorder = RawFrameRecorder(
        base_output_dir=raw_record_output_dir,
        target_fps=display_fps,
        source_name=source_name,
        backend=raw_record_backend,
        ffmpeg_bin=raw_record_ffmpeg_bin,
        ffmpeg_encoder=raw_record_ffmpeg_encoder,
        ffmpeg_preset=raw_record_ffmpeg_preset,
    )
    _raw_record_toggle_request = threading.Event()
    _raw_toggle_lock = threading.Lock()
    _raw_toggle_min_interval_s = 0.35
    _raw_toggle_last_request_s = [0.0]

    _telemetry_record_lock = threading.Lock()
    _telemetry_record_enabled = [bool(telemetry_record_start_enabled)]

    def _toggle_telemetry_recording() -> bool:
        with _telemetry_record_lock:
            _telemetry_record_enabled[0] = not _telemetry_record_enabled[0]
            state = _telemetry_record_enabled[0]
        print(f"Telemetry recording: {'ENABLED' if state else 'DISABLED'}")
        return state

    def _is_telemetry_recording_enabled() -> bool:
        with _telemetry_record_lock:
            return bool(_telemetry_record_enabled[0])

    def _request_raw_record_toggle() -> None:
        now_s = time.perf_counter()
        with _raw_toggle_lock:
            if (now_s - _raw_toggle_last_request_s[0]) < _raw_toggle_min_interval_s:
                return
            _raw_toggle_last_request_s[0] = now_s
        _raw_record_toggle_request.set()

    if telemetry.available:
        telemetry_sources = []
        if telemetry.nvidia_available:
            telemetry_sources.append("nvidia-smi")
        if telemetry.windows_counter_available:
            telemetry_sources.append("windows-gpu-engine")
        print(f"NVIDIA telemetry: enabled ({', '.join(telemetry_sources)}).")
    else:
        print("NVIDIA telemetry: unavailable (no nvidia-smi or Windows GPU engine counter).")

    try:
        detector_entries = build_aruco_detectors(
            dict_names,
            detector_params=configure_detector_parameters(
                quad_decimate=april_tag_quad_decimate,
                quad_sigma=april_tag_quad_sigma,
                adaptive_thresh_win_size_min=adaptive_thresh_win_size_min,
                adaptive_thresh_win_size_max=adaptive_thresh_win_size_max,
                corner_refinement_method=corner_refinement_method,
                min_marker_perimeter_rate=min_marker_perimeter_rate,
                error_correction_rate=error_correction_rate,
                april_tag_min_white_black_diff=april_tag_min_white_black_diff,
            ),
        )
        print(f"ArUco dictionaries: {', '.join(name for name, _, _ in detector_entries)}")
    except (RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 2

    board_definitions: list[dict] = []
    for board_json in (board_json_paths or []):
        try:
            board_definition = load_board_definition(board_json)
            board_definitions.append(board_definition)
        except Exception as exc:
            print(f"ERROR: Failed to load board definition '{board_json}': {exc}")
            return 2

    effective_tag_size_mm_by_id: dict[int, float] = {}
    if board_definitions:
        print("Board pose: enabled for boards:")
        for board_definition in board_definitions:
            board_dict_name = board_definition.get("dict_name") or f"DICT_ID_{board_definition['dictionary_id']}"
            board_tag_size = board_definition.get("tag_size_mm")
            tag_size_str = (
                f", tag_size_mm={float(board_tag_size):.3f}"
                if board_tag_size is not None
                else ""
            )
            print(
                f"  - {board_definition['name']} (dict={board_dict_name}, tags={len(board_definition['ids'])}{tag_size_str})"
            )
            for tag_id, size_mm in board_definition.get("tag_size_mm_by_id", {}).items():
                tid = int(tag_id)
                if tid in effective_tag_size_mm_by_id and abs(float(effective_tag_size_mm_by_id[tid]) - float(size_mm)) > 1e-6:
                    print(
                        f"Warning: conflicting tag_size_mm for tag ID {tid} across boards "
                        f"({effective_tag_size_mm_by_id[tid]:.3f} vs {float(size_mm):.3f}); keeping first value."
                    )
                    continue
                effective_tag_size_mm_by_id[tid] = float(size_mm)

    if tag_size_map_json_path:
        try:
            override_tag_sizes = load_tag_size_map_json(tag_size_map_json_path)
        except Exception as exc:
            print(f"ERROR: Failed to load tag size map '{tag_size_map_json_path}': {exc}")
            return 2
        # Explicit CLI map has highest precedence over board-derived sizes.
        effective_tag_size_mm_by_id.update(override_tag_sizes)
        print(
            f"Tag size map: loaded {len(override_tag_sizes)} entries from {tag_size_map_json_path} "
            f"(overrides board/CLI fallback sizes)"
        )

    title = f"NDI HX3 GPU Preview with AprilTags - {source_name}"
    _use_gl = False
    _glfw_win = None
    _gl_tex_id = None
    _gl_tex_size = (0, 0)  # (w, h) of currently allocated texture
    if not no_display:
        if _GL_AVAILABLE:
            try:
                if not glfw.init():
                    raise RuntimeError("glfw.init() failed")
                glfw.window_hint(glfw.CONTEXT_VERSION_MAJOR, 2)
                glfw.window_hint(glfw.CONTEXT_VERSION_MINOR, 1)
                glfw.window_hint(glfw.DOUBLEBUFFER, 1)
                _glfw_win = glfw.create_window(1280, 720, title, None, None)
                if _glfw_win is None:
                    raise RuntimeError("glfw.create_window failed")
                glfw.make_context_current(_glfw_win)
                glfw.swap_interval(0)  # disable vsync; we pace ourselves
                def _on_glfw_key(win, key, sc, action, mods):
                    if action != glfw.PRESS:
                        return
                    if key in (glfw.KEY_ESCAPE, glfw.KEY_Q):
                        _quit.set()
                        return
                    if key == glfw.KEY_T:
                        _toggle_telemetry_recording()
                    if key == glfw.KEY_R:
                        _request_raw_record_toggle()
                glfw.set_key_callback(_glfw_win, _on_glfw_key)

                _gl_tex_id = glGenTextures(1)
                glBindTexture(GL_TEXTURE_2D, _gl_tex_id)
                glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR)
                glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR)
                glMatrixMode(GL_PROJECTION)
                glLoadIdentity()
                glOrtho(-1, 1, -1, 1, -1, 1)
                glMatrixMode(GL_MODELVIEW)
                glLoadIdentity()
                _use_gl = True
                print("Display: GLFW + OpenGL (direct GPU texture upload, GL_BGR).")
            except Exception as _gl_exc:
                print(f"Display: GLFW/OpenGL unavailable ({_gl_exc}); falling back to OpenCV window.")
                _use_gl = False
                if _glfw_win is not None:
                    glfw.terminate()
                    _glfw_win = None
        if not _use_gl:
            cv2.namedWindow(title, cv2.WINDOW_NORMAL)
            print("Display: OpenCV standard window.")

    _quit = threading.Event()

    # Analysis queue carries only grayscale + metadata (no color copy in the hot path).
    frame_queue: queue.Queue[tuple[int, np.ndarray, dict, str, int, int]] = queue.Queue(maxsize=0)

    # Display queue stores color frames tagged by frame_id and enqueue time.
    # This enables smooth output with bounded sync waiting.
    _bgr_lock = threading.Lock()
    _display_queue: deque[tuple[int, np.ndarray, float, int]] = deque()
    _max_display_queue = 240

    # Analysis results by frame_id so display can draw only matching overlays.
    _result_lock = threading.Lock()
    _results_by_id: dict[int, dict] = {}
    # Sorted list of completed frame_ids — enables O(log n) floor lookup in display loop.
    _completed_ids: list[int] = []

    _stats_lock = threading.Lock()
    _stats = {
        "capture_fps": 0.0,
        "analyze_fps": 0.0,
        "output_fps": 0.0,
        "analyze_frames": 0,
        "analyze_tick": time.perf_counter(),
        "output_frames": 0,
        "output_tick": time.perf_counter(),
        "last_log": 0.0,
    }

    _freed_lock = threading.Lock()
    _freed_state = {
        "packet_total": 0,
        "packet_valid": 0,
        "last_perf": 0.0,
        "last": None,
        "last_mode": "-",
        "last_addr": "-",
    }

    cpu_count = max(1, os.cpu_count() or 1)
    worker_count = analysis_workers if analysis_workers > 0 else min(cpu_count, 8)
    worker_count = max(1, worker_count)
    prep_fps = max(display_fps, display_fps * max(1.0, display_prep_oversample))
    print(f"Analysis workers: {worker_count} (cpu_count={cpu_count})")
    print(
        f"Display sync: fps={display_fps:.1f} prep_fps={prep_fps:.1f} scale={display_scale:.2f} delay={max(0, display_delay_frames)}f timeout={max(0.0, sync_timeout_ms):.0f}ms"
    )
    # Bound display queue to prevent unbounded growth; doesn't block rendering.
    _target_display_buffer = max(2, display_delay_frames + int(math.ceil(max(1.0, display_prep_oversample))))

    def recv_loop() -> None:
        frame_id = 0
        frame_counter = 0
        tick = time.perf_counter()
        bgr_tick = 0.0
        prep_interval = 1.0 / max(1.0, prep_fps)

        while not _quit.is_set():
            frame_type, video_frame, _, _ = ndi.recv_capture_v2(recv, 50)

            if frame_type != ndi.FrameType.FRAME_TYPE_VIDEO:
                continue

            try:
                # Grayscale for analysis — this is the primary path and carries every frame.
                gray = frame_to_gray(video_frame, video_frame.data)
                if gray is None:
                    continue
                ndi_meta = extract_ndi_meta(video_frame, source_name)
                frame_ts = get_frame_timestamp(video_frame)
                frame_recv_wall_ns = time.time_ns()
                frame_recv_mono_ns = time.perf_counter_ns()
                # BGR is only needed for display. Convert at an oversampled display-prep
                # cadence so the presenter can choose a closer frame/result match.
                now = time.perf_counter()
                bgr = None
                if not no_display and (now - bgr_tick) >= prep_interval:
                    bgr = frame_to_bgr(video_frame, video_frame.data)
                    if bgr is not None:
                        if display_scale != 1.0:
                            resized_width = max(1, int(round(bgr.shape[1] * display_scale)))
                            resized_height = max(1, int(round(bgr.shape[0] * display_scale)))
                            interpolation = cv2.INTER_AREA if display_scale < 1.0 else cv2.INTER_LINEAR
                            bgr = cv2.resize(bgr, (resized_width, resized_height), interpolation=interpolation)
                        bgr_tick = now
            finally:
                ndi.recv_free_video_v2(recv, video_frame)

            current_frame_id = frame_id

            # Every frame goes to analysis.
            frame_queue.put((current_frame_id, gray, ndi_meta, frame_ts, frame_recv_wall_ns, frame_recv_mono_ns))
            frame_id += 1

            # Queue color frames for smoother display pacing and sync matching.
            if bgr is not None:
                with _bgr_lock:
                    _display_queue.append((current_frame_id, bgr, time.perf_counter(), frame_recv_mono_ns))
                    while len(_display_queue) > _target_display_buffer:
                        _display_queue.popleft()
                    while len(_display_queue) > _max_display_queue:
                        _display_queue.popleft()

            frame_counter += 1
            now = time.perf_counter()
            elapsed = now - tick
            if elapsed >= 1.0:
                with _stats_lock:
                    _stats["capture_fps"] = frame_counter / elapsed
                frame_counter = 0
                tick = now

    def analyze_loop(worker_index: int) -> None:
        """Grayscale-only detection + pose. Zero drawing happens here."""
        camera_matrix = None
        detector_entries_local = build_aruco_detectors(dict_names)

        while not _quit.is_set():
            try:
                frame_id, gray, ndi_meta, frame_ts, frame_recv_wall_ns, frame_recv_mono_ns = frame_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if camera_matrix is None:
                h, w = gray.shape[:2]
                camera_matrix = np.array(
                    [[focal_length, 0, w / 2.0], [0, focal_length, h / 2.0], [0, 0, 1]],
                    dtype=np.float32,
                )

            tag_count, tag_poses, detections, tag_image_metrics, tag_quaternions = detect_tags(
                gray,
                detector_entries_local,
                camera_matrix,
                tag_size_mm,
                board_definitions=board_definitions,
                enable_board_refinement=enable_board_refinement,
                tag_size_mm_by_id=effective_tag_size_mm_by_id,
            )

            now = time.perf_counter()
            with _stats_lock:
                _stats["analyze_frames"] += 1
                elapsed = now - _stats["analyze_tick"]
                if elapsed >= 1.0:
                    _stats["analyze_fps"] = _stats["analyze_frames"] / elapsed
                    _stats["analyze_frames"] = 0
                    _stats["analyze_tick"] = now
                analyze_fps = _stats["analyze_fps"]
                capture_fps = _stats["capture_fps"]
                should_log = (now - _stats["last_log"]) >= 1.0
                if should_log:
                    _stats["last_log"] = now

            with _result_lock:
                _results_by_id[frame_id] = {
                    "tag_count": tag_count,
                    "tag_poses": tag_poses,
                    "detections": detections,
                    "ndi_meta": ndi_meta,
                    "frame_ts": frame_ts,
                    "frame_recv_wall_ns": int(frame_recv_wall_ns),
                    "frame_recv_mono_ns": int(frame_recv_mono_ns),
                    "tag_image_metrics": tag_image_metrics,
                }
                bisect.insort(_completed_ids, frame_id)
                # Keep both structures bounded together.
                if len(_results_by_id) > 300:
                    cutoff = frame_id - 240
                    stale = [fid for fid in _results_by_id if fid < cutoff]
                    for fid in stale:
                        del _results_by_id[fid]
                    prune_idx = bisect.bisect_left(_completed_ids, cutoff)
                    del _completed_ids[:prune_idx]

            # Tag telemetry at analysis rate (highest available rate for this pipeline).
            if _is_telemetry_recording_enabled() and tag_poses:
                for tid, x, y, z, roll, pitch, yaw in tag_poses:
                    tag_size_used_mm = float(effective_tag_size_mm_by_id.get(int(tid), tag_size_mm))
                    img_m = tag_image_metrics.get(int(tid), {})
                    quat_w, quat_x, quat_y, quat_z = tag_quaternions.get(int(tid), euler_deg_to_quaternion(roll, pitch, yaw))
                    payload: dict = {
                        "type": "tag_pose",
                        "source": source_name,
                        "ts_wall_ns": int(frame_recv_wall_ns),
                        "ts_mono_ns": int(frame_recv_mono_ns),
                        "source_ts": str(ndi_meta.get("timecode", frame_ts)),
                        "ingest_seq": int(frame_id),
                        "frame_id": int(frame_id),
                        "tag_id": int(tid),
                        "tag_size_mm": round(tag_size_used_mm, 3),
                        "x_mm": round(float(x), 3),
                        "y_mm": round(float(y), 3),
                        "z_mm": round(float(z), 3),
                        "roll_deg": round(float(roll), 3),
                        "pitch_deg": round(float(pitch), 3),
                        "yaw_deg": round(float(yaw), 3),
                        "quat_w": round(float(quat_w), 6),
                        "quat_x": round(float(quat_x), 6),
                        "quat_y": round(float(quat_y), 6),
                        "quat_z": round(float(quat_z), 6),
                        **img_m,
                    }
                    mqtt_pub.publish("tag_pose", payload)

                if board_definitions:
                    for board_definition in board_definitions:
                        matched_tag_count = count_board_tag_matches(detections, board_definition)
                        tag_subset_pose = estimate_board_pose_from_tag_subset(
                            tag_poses=tag_poses,
                            board_ids=board_definition["ids_set"],
                            board_expected_count=len(board_definition["ids"]),
                        )
                        board_method = "aruco_board"
                        board_pose = estimate_board_pose_from_detections(
                            detections=detections,
                            board_definition=board_definition,
                            camera_matrix=camera_matrix,
                        )
                        if board_pose is not None:
                            board_x, board_y, board_z, board_roll, board_pitch, board_yaw, board_tag_count, board_tag_expected = board_pose
                            # Guard against numerically valid but physically implausible board solves.
                            # If tag-subset pose is available and differs wildly in depth, prefer stable fallback.
                            if tag_subset_pose is not None:
                                _, _, subset_z, _, _, _, _, _ = tag_subset_pose
                                if abs(subset_z) > 1e-6:
                                    z_ratio = abs(float(board_z) / float(subset_z))
                                    # Board solve should stay close to tag-subset depth.
                                    if z_ratio > 3.0 or z_ratio < (1.0 / 3.0):
                                        board_pose = None
                            # Absolute sanity bound in millimeters for this tracking setup.
                            if board_pose is not None and max(abs(float(board_x)), abs(float(board_y)), abs(float(board_z))) > 30000.0:
                                board_pose = None
                        if board_pose is None:
                            board_pose = tag_subset_pose
                            board_method = "median_tags"
                        if board_pose is None:
                            if should_log and matched_tag_count > 0:
                                print(
                                    f"board[{board_definition['name']}]: matched={matched_tag_count}/"
                                    f"{len(board_definition['ids'])} but aruco solve failed and fallback had no poses"
                                )
                            continue

                        board_x, board_y, board_z, board_roll, board_pitch, board_yaw, board_tag_count, board_tag_expected = board_pose
                        if should_log and board_method != "aruco_board":
                            print(
                                f"board[{board_definition['name']}]: using fallback={board_method} "
                                f"matched={matched_tag_count}/{board_tag_expected}"
                            )
                        quat_w, quat_x, quat_y, quat_z = euler_deg_to_quaternion(
                            board_roll,
                            board_pitch,
                            board_yaw,
                        )
                        mqtt_pub.publish(
                            "board_pose",
                            {
                                "type": "board_pose",
                                "board_name": str(board_definition["name"]),
                                "method": board_method,
                                "aruco_solve_ok": int(board_method == "aruco_board"),
                                "source": source_name,
                                "ts_wall_ns": int(frame_recv_wall_ns),
                                "ts_mono_ns": int(frame_recv_mono_ns),
                                "source_ts": str(ndi_meta.get("timecode", frame_ts)),
                                "ingest_seq": int(frame_id),
                                "frame_id": int(frame_id),
                                "matched_tag_count": int(matched_tag_count),
                                "tag_count": int(board_tag_count),
                                "tag_expected": int(board_tag_expected),
                                "x_mm": round(float(board_x), 3),
                                "y_mm": round(float(board_y), 3),
                                "z_mm": round(float(board_z), 3),
                                "roll_deg": round(float(board_roll), 3),
                                "pitch_deg": round(float(board_pitch), 3),
                                "yaw_deg": round(float(board_yaw), 3),
                                "quat_w": round(float(quat_w), 6),
                                "quat_x": round(float(quat_x), 6),
                                "quat_y": round(float(quat_y), 6),
                                "quat_z": round(float(quat_z), 6),
                            },
                        )
                        if board_pose_stream is not None:
                            board_pose_stream.publish_board_pose(
                                x_mm=float(board_x),
                                y_mm=float(board_y),
                                z_mm=float(board_z),
                                roll_deg=float(board_roll),
                                pitch_deg=float(board_pitch),
                                yaw_deg=float(board_yaw),
                            )
                else:
                    fallback_pose = estimate_board_pose(tag_poses)
                    if fallback_pose is not None:
                        board_x, board_y, board_z, board_roll, board_pitch, board_yaw, board_tag_count = fallback_pose
                        quat_w, quat_x, quat_y, quat_z = euler_deg_to_quaternion(
                            board_roll,
                            board_pitch,
                            board_yaw,
                        )
                        mqtt_pub.publish(
                            "board_pose",
                            {
                                "type": "board_pose",
                                "board_name": "tag_set",
                                "method": "median_tags",
                                "source": source_name,
                                "ts_wall_ns": int(frame_recv_wall_ns),
                                "ts_mono_ns": int(frame_recv_mono_ns),
                                "source_ts": str(ndi_meta.get("timecode", frame_ts)),
                                "ingest_seq": int(frame_id),
                                "frame_id": int(frame_id),
                                "tag_count": int(board_tag_count),
                                "tag_expected": int(board_tag_count),
                                "x_mm": round(float(board_x), 3),
                                "y_mm": round(float(board_y), 3),
                                "z_mm": round(float(board_z), 3),
                                "roll_deg": round(float(board_roll), 3),
                                "pitch_deg": round(float(board_pitch), 3),
                                "yaw_deg": round(float(board_yaw), 3),
                                "quat_w": round(float(quat_w), 6),
                                "quat_x": round(float(quat_x), 6),
                                "quat_y": round(float(quat_y), 6),
                                "quat_z": round(float(quat_z), 6),
                            },
                        )
                        if board_pose_stream is not None:
                            board_pose_stream.publish_board_pose(
                                x_mm=float(board_x),
                                y_mm=float(board_y),
                                z_mm=float(board_z),
                                roll_deg=float(board_roll),
                                pitch_deg=float(board_pitch),
                                yaw_deg=float(board_yaw),
                            )

            if should_log:
                pose_str = (
                    " | ".join(
                        f"ID{tid}:XYZ=({x:.2f},{y:.2f},{z:.2f})mm RPY=({roll:.2f},{pitch:.2f},{yaw:.2f})deg"
                        for tid, x, y, z, roll, pitch, yaw in tag_poses
                    )
                    if tag_poses
                    else "no tags"
                )
                queue_depth = frame_queue.qsize()
                if _is_telemetry_recording_enabled():
                    mqtt_pub.publish(
                        "stats",
                        {
                            "type": "stats",
                            "source": source_name,
                            "ts_wall_ns": int(frame_recv_wall_ns),
                            "ts_mono_ns": int(frame_recv_mono_ns),
                            "source_ts": str(ndi_meta.get("timecode", frame_ts)),
                            "ingest_seq": int(frame_id),
                            "worker_count": worker_count,
                            "capture_fps": round(capture_fps, 3),
                            "analyze_fps": round(analyze_fps, 3),
                            "queue_depth": int(queue_depth),
                            "tag_count": int(tag_count),
                            "telemetry_available": int(bool(telemetry.available)),
                            "decoder_util": float(telemetry.decoder_util) if telemetry.decoder_util is not None else -1.0,
                            "windows_decode_util": float(telemetry.windows_decode_util) if telemetry.windows_decode_util is not None else -1.0,
                            "gpu_util": float(telemetry.gpu_util) if telemetry.gpu_util is not None else -1.0,
                            "mem_util": float(telemetry.mem_util) if telemetry.mem_util is not None else -1.0,
                        },
                    )
                if telemetry.available:
                    print(
                        f"telemetry: cap_fps={capture_fps:.1f} ana_fps={analyze_fps:.1f} q={queue_depth} w={worker_count} "
                        f"tags={tag_count} poses=[{pose_str}] fourcc={ndi_meta.get('fourcc','?')} "
                        f"decoder={telemetry.decoder_util if telemetry.decoder_util is not None else -1:.0f}% "
                        f"decoder_win={telemetry.windows_decode_util if telemetry.windows_decode_util is not None else -1:.0f}%({telemetry.windows_decode_scope}) "
                        f"gpu={telemetry.gpu_util if telemetry.gpu_util is not None else -1:.0f}% "
                        f"mem={telemetry.mem_util if telemetry.mem_util is not None else -1:.0f}%"
                    )
                else:
                    print(
                        f"telemetry: cap_fps={capture_fps:.1f} ana_fps={analyze_fps:.1f} q={queue_depth} w={worker_count} "
                        f"tags={tag_count} poses=[{pose_str}] fourcc={ndi_meta.get('fourcc','?')}"
                    )

    def freed_udp_listener() -> None:
        """Receive Free-D UDP packets at full network speed and store the latest decoded state."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((freed_listen_ip, freed_port))
        except OSError as exc:
            print(f"freeD: bind {freed_listen_ip}:{freed_port} failed: {exc}")
            return
        sock.settimeout(0.5)
        print(f"freeD: listening on {freed_listen_ip}:{freed_port}")
        freed_ingest_seq = 0
        while not _quit.is_set():
            try:
                data, addr = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except OSError:
                break
            freed_recv_wall_ns = time.time_ns()
            freed_recv_mono_ns = time.perf_counter_ns()
            freed_ingest_seq += 1
            decoded, mode = decode_freed_fields(data)
            with _freed_lock:
                _freed_state["packet_total"] += 1
                if decoded["camera_id"] is not None:
                    _freed_state["packet_valid"] += 1
                    _freed_state["last"] = decoded
                    _freed_state["last_mode"] = mode
                    _freed_state["last_addr"] = f"{addr[0]}:{addr[1]}"
                    _freed_state["last_perf"] = time.perf_counter()

            if decoded["camera_id"] is not None:
                if _is_telemetry_recording_enabled():
                    mqtt_pub.publish(
                        "freed",
                        {
                            "type": "freed",
                            "source": source_name,
                            "ts_wall_ns": int(freed_recv_wall_ns),
                            "ts_mono_ns": int(freed_recv_mono_ns),
                            "source_ts": "",
                            "ingest_seq": int(freed_ingest_seq),
                            "stream": f"{addr[0]}:{addr[1]}",
                            "mode": mode,
                            "camera_id": int(decoded["camera_id"]) if decoded["camera_id"] is not None else -1,
                            "pan_raw": int(decoded["pan"]) if decoded["pan"] is not None else 0,
                            "tilt_raw": int(decoded["tilt"]) if decoded["tilt"] is not None else 0,
                            "roll_raw": int(decoded["roll"]) if decoded["roll"] is not None else 0,
                            "x_raw": int(decoded["x"]) if decoded["x"] is not None else 0,
                            "y_raw": int(decoded["y"]) if decoded["y"] is not None else 0,
                            "z_raw": int(decoded["z"]) if decoded["z"] is not None else 0,
                            "zoom_raw": int(decoded["zoom"]) if decoded["zoom"] is not None else 0,
                            "focus_raw": int(decoded["focus"]) if decoded["focus"] is not None else 0,
                            "pan_deg": float(decoded["pan"]) / freed_angle_scale if decoded["pan"] is not None and freed_angle_scale != 0 else 0.0,
                            "tilt_deg": float(decoded["tilt"]) / freed_angle_scale if decoded["tilt"] is not None and freed_angle_scale != 0 else 0.0,
                            "roll_deg": float(decoded["roll"]) / freed_angle_scale if decoded["roll"] is not None and freed_angle_scale != 0 else 0.0,
                        },
                    )
        sock.close()

    _prepared_display_lock = threading.Lock()
    _prepared_display_queue: deque[tuple[int, np.ndarray, np.ndarray, float, int, float, bool, int, dict, str, int, int]] = deque()
    _max_prepared_display_queue = 8

    # Main thread: owns all HighGUI calls (Windows requirement).
    # Capped at 30fps — final HUD composition and presentation happen here.
    _DISPLAY_INTERVAL = 1.0 / max(1.0, display_fps)
    _DISPLAY_DELAY_FRAMES = max(0, display_delay_frames)
    _SYNC_TIMEOUT_SEC = max(0.0, sync_timeout_ms) / 1000.0

    def display_prep_loop() -> None:
        overlay_cache: dict[tuple[int, int, int], tuple[np.ndarray | None, np.ndarray | None, tuple[int, int, int, int] | None]] = {}

        while not _quit.is_set():
            with _bgr_lock:
                if _display_queue:
                    bgr_packet = _display_queue[-1]
                    _display_queue.clear()
                else:
                    bgr_packet = None

            if bgr_packet is None:
                time.sleep(0.001)
                continue

            bgr_frame_id, bgr, queued_at, bgr_frame_recv_mono_ns = bgr_packet

            with _result_lock:
                result = _results_by_id.get(bgr_frame_id)
                used_result_id = bgr_frame_id
                if result is None:
                    idx = bisect.bisect_right(_completed_ids, bgr_frame_id) - 1
                    if idx >= 0:
                        used_result_id = _completed_ids[idx]
                        result = _results_by_id.get(used_result_id)

            timed_out = (time.perf_counter() - queued_at) >= _SYNC_TIMEOUT_SEC

            if result is None:
                tag_count = 0
                ndi_meta = {}
                frame_ts = ""
                overlay_age_frames = -1
                overlay_age_ms = -1.0
                tag_overlay_roi = None
                tag_overlay_mask = None
                tag_overlay_rect = None
            else:
                tag_count = result["tag_count"]
                ndi_meta = dict(result["ndi_meta"])
                frame_ts = result["frame_ts"]
                overlay_age_frames = bgr_frame_id - used_result_id
                overlay_age_ms = max(0.0, (bgr_frame_recv_mono_ns - int(result["frame_recv_mono_ns"])) / 1_000_000.0)
                cache_key = (used_result_id, bgr.shape[1], bgr.shape[0])
                cached_overlay = overlay_cache.get(cache_key)
                if cached_overlay is None:
                    display_detections = scale_detections_for_display(result["detections"], display_scale)
                    cached_overlay = build_tag_overlay(
                        (bgr.shape[0], bgr.shape[1]),
                        display_detections,
                        result["tag_poses"],
                    )
                    overlay_cache[cache_key] = cached_overlay
                    if len(overlay_cache) > 32:
                        oldest_key = next(iter(overlay_cache))
                        del overlay_cache[oldest_key]
                tag_overlay_roi, tag_overlay_mask, tag_overlay_rect = cached_overlay

            raw_image = bgr
            image = bgr.copy()
            height, width = image.shape[:2]

            cx_r, cy_r = width // 2, height // 2
            cv2.line(image, (cx_r - 20, cy_r), (cx_r + 20, cy_r), (0, 255, 0), 1)
            cv2.line(image, (cx_r, cy_r - 20), (cx_r, cy_r + 20), (0, 255, 0), 1)
            cv2.circle(image, (cx_r, cy_r), 2, (0, 255, 0), -1)

            if tag_overlay_rect is not None and tag_overlay_roi is not None and tag_overlay_mask is not None:
                x, y, width_roi, height_roi = tag_overlay_rect
                cv2.copyTo(tag_overlay_roi, tag_overlay_mask, image[y:y + height_roi, x:x + width_roi])

            with _prepared_display_lock:
                _prepared_display_queue.append((
                    bgr_frame_id,
                    image,
                    raw_image,
                    queued_at,
                    overlay_age_frames,
                    overlay_age_ms,
                    timed_out,
                    tag_count,
                    ndi_meta,
                    frame_ts,
                    int(result["frame_recv_wall_ns"]) if result is not None else 0,
                    int(result["frame_recv_mono_ns"]) if result is not None else int(bgr_frame_recv_mono_ns),
                ))
                while len(_prepared_display_queue) > _target_display_buffer:
                    _prepared_display_queue.popleft()
                while len(_prepared_display_queue) > _max_prepared_display_queue:
                    _prepared_display_queue.popleft()

    def select_prepared_packet(
        packets: list[tuple[int, np.ndarray, np.ndarray, float, int, float, bool, int, dict, str, int, int]],
    ) -> tuple[int, np.ndarray, np.ndarray, float, int, float, bool, int, dict, str, int, int] | None:
        if not packets:
            return None

        exact_packets = [packet for packet in packets if packet[4] == 0]
        if exact_packets:
            return max(exact_packets, key=lambda packet: packet[0])

        return min(packets, key=lambda packet: (packet[3], -packet[0]))

    recv_thread = threading.Thread(target=recv_loop, daemon=True, name="ndi-recv")
    analyze_threads = [
        threading.Thread(target=analyze_loop, args=(i,), daemon=True, name=f"ndi-analyze-{i}")
        for i in range(worker_count)
    ]
    display_prep_thread = threading.Thread(target=display_prep_loop, daemon=True, name="ndi-display-prep")
    freed_thread = threading.Thread(target=freed_udp_listener, daemon=True, name="freed-udp")
    recv_thread.start()
    for t in analyze_threads:
        t.start()
    display_prep_thread.start()
    freed_thread.start()

    # NDI info overlay cache: keep only the non-zero ROI and blit it back each frame.
    # This avoids both repeated putText calls and a full-frame boolean scan on every frame.
    _ndi_ovl_roi: list[np.ndarray | None] = [None]
    _ndi_ovl_mask: list[np.ndarray | None] = [None]
    _ndi_ovl_rect: list[tuple[int, int, int, int] | None] = [None]
    _ndi_ovl_key: list[tuple] = [()]

    # Top-left HUD text cache: refresh lines at a lower cadence, then ROI-blit each frame.
    _hud_ovl_roi: list[np.ndarray | None] = [None]
    _hud_ovl_mask: list[np.ndarray | None] = [None]
    _hud_ovl_rect: list[tuple[int, int, int, int] | None] = [None]
    _hud_ovl_key: list[tuple] = [()]
    _hud_lines: list[tuple[str, tuple[int, int, int]]] = []
    _hud_next_update = 0.0
    _HUD_UPDATE_INTERVAL = 0.1
    _HUD_X = 8
    _HUD_Y = 8
    _HUD_LINE_H = 14
    _HUD_FONT_SCALE = 0.4
    _HUD_THICKNESS = 1
    _HUD_PAD = 3

    # Top-right freeD overlay cache.
    _freed_ovl_roi: list[np.ndarray | None] = [None]
    _freed_ovl_mask: list[np.ndarray | None] = [None]
    _freed_ovl_rect: list[tuple[int, int, int, int] | None] = [None]
    _freed_ovl_key: list[tuple] = [()]
    _freed_max_box_w: list[int] = [0]  # only ever grows — keeps left edge stable
    _freed_lines: list[tuple[str, tuple[int, int, int]]] = []
    _freed_next_update = 0.0
    _FREED_UPDATE_INTERVAL = 0.1
    _FREED_PAD = 8
    _FREED_LINE_H = 14
    _FREED_FONT_SCALE = 0.4
    _FREED_THICKNESS = 1
    _FREED_BOX_PAD = 3

    # Per-stage timing for display loop profiling.
    _stage_times: dict[str, float] = {}
    _stage_frame_count = 0

    # Raise Windows multimedia timer resolution to 1ms for the lifetime of the loop.
    # Without this, time.sleep() and waitKey have ~15ms granularity on stock Windows.
    _timer_set = False
    if sys.platform == 'win32':
        try:
            ctypes.windll.winmm.timeBeginPeriod(1)
            _timer_set = True
            print("Display: Windows timer resolution set to 1ms.")
        except Exception:
            pass

    try:
        next_display = time.perf_counter()
        last_prepared_packet = None
        last_recorded_frame_id: int | None = None
        while not _quit.is_set():
            if no_display:
                if not recv_thread.is_alive() or any(not t.is_alive() for t in analyze_threads) or not display_prep_thread.is_alive():
                    break
                time.sleep(0.05)
                continue

            # Event handling — non-blocking for both backends.
            if _use_gl:
                glfw.poll_events()
                if glfw.window_should_close(_glfw_win):
                    break
                # Key quit handled via GLFW set_key_callback registered below.
            else:
                key = cv2.pollKey() & 0xFF
                if key in (27, ord("q")):
                    break
                if key in (ord("t"), ord("T")):
                    _toggle_telemetry_recording()
                if key in (ord("r"), ord("R")):
                    _request_raw_record_toggle()

            now = time.perf_counter()
            if now + 0.0002 < next_display:
                # Sleep with minimal headroom to reduce jitter while keeping schedule accuracy.
                time.sleep(max(0, next_display - now - 0.0002))
                continue
            if now - next_display > (_DISPLAY_INTERVAL * 3.0):
                # If we are far behind (pause, resize, context stall), resync to current time.
                next_display = now
            else:
                next_display += _DISPLAY_INTERVAL

            with _prepared_display_lock:
                if _prepared_display_queue:
                    prepared_packet = select_prepared_packet(list(_prepared_display_queue))
                    if prepared_packet is not None:
                        selected_frame_id = prepared_packet[0]
                        while _prepared_display_queue and _prepared_display_queue[0][0] <= selected_frame_id:
                            _prepared_display_queue.popleft()
                    last_prepared_packet = prepared_packet
                else:
                    prepared_packet = last_prepared_packet
            if prepared_packet is None:
                continue
            frame_id, image, raw_image, queued_at, overlay_age_frames, overlay_age_ms, timed_out, tag_count, ndi_meta, frame_ts, frame_recv_wall_ns, frame_recv_mono_ns = prepared_packet

            if _raw_record_toggle_request.is_set():
                _raw_record_toggle_request.clear()
                if raw_recorder.is_recording():
                    raw_recorder.stop_recording()
                    last_recorded_frame_id = None
                else:
                    h_raw, w_raw = raw_image.shape[:2]
                    raw_recorder.start_recording(frame_width=w_raw, frame_height=h_raw)
                    last_recorded_frame_id = None

            if raw_recorder.is_recording() and frame_id != last_recorded_frame_id:
                raw_recorder.enqueue_frame(
                    raw_image,
                    {
                        "frame_id": int(frame_id),
                        "ts_wall_ns": int(frame_recv_wall_ns),
                        "ts_mono_ns": int(frame_recv_mono_ns),
                        "source_ts": str(ndi_meta.get("timecode", frame_ts)),
                        "source_name": str(ndi_meta.get("source", source_name)),
                        "overlay_age_frames": int(overlay_age_frames),
                        "overlay_age_ms": float(overlay_age_ms),
                        "timed_out": int(bool(timed_out)),
                        "tag_count": int(tag_count),
                        "recorded_wall_ns": int(time.time_ns()),
                    },
                )
                last_recorded_frame_id = int(frame_id)

            with _stats_lock:
                analyze_fps = _stats["analyze_fps"]
                capture_fps = _stats["capture_fps"]
                output_fps = _stats["output_fps"]

            height, width = image.shape[:2]

            # Top-left overlay (cached text layer).
            t_stage = time.perf_counter()
            if now >= _hud_next_update or not _hud_lines:
                lines: list[tuple[str, tuple[int, int, int]]] = []
                lines.append((f"Workers: {worker_count}", (0, 255, 0)))

                if show_timestamp:
                    ts_local_text = f"Local: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
                    lines.append((ts_local_text, (0, 255, 0)))

                stats_text = (
                    f"CapFPS: {capture_fps:.1f} | AnaFPS: {analyze_fps:.1f} | "
                    f"OutFPS: {output_fps:.1f} | Tags: {tag_count}"
                )
                lines.append((stats_text, (0, 255, 0)))

                rec_state = "ON" if _is_telemetry_recording_enabled() else "OFF"
                rec_color = (0, 255, 255) if rec_state == "ON" else (0, 120, 255)
                lines.append((f"Telemetry Rec: {rec_state} (press T)", rec_color))

                raw_rec_state = "ON" if raw_recorder.is_recording() else "OFF"
                raw_rec_color = (0, 255, 255) if raw_rec_state == "ON" else (0, 120, 255)
                lines.append((f"Raw Rec: {raw_rec_state} (press R)", raw_rec_color))

                dropped = raw_recorder.dropped_frames()
                if dropped > 0:
                    lines.append((f"Raw Rec Drops: {dropped}", (0, 165, 255)))

                if telemetry.available:
                    gpu_text = (
                        f"GPU: {telemetry.gpu_util if telemetry.gpu_util is not None else -1:.0f}% "
                        f"DecNv: {telemetry.decoder_util if telemetry.decoder_util is not None else -1:.0f}% "
                        f"DecWin: {telemetry.windows_decode_util if telemetry.windows_decode_util is not None else -1:.0f}%({telemetry.windows_decode_scope}) "
                    )
                    lines.append((gpu_text, (0, 255, 0)))

                if overlay_age_frames >= 0:
                    sync_text = f"SyncAge: {overlay_age_frames}f ({overlay_age_ms:.1f}ms)"
                else:
                    sync_text = f"SyncAge: pending{'*' if timed_out else ''}"
                lines.append((sync_text, (0, 255, 255)))

                _hud_lines = lines
                _hud_next_update = now + _HUD_UPDATE_INTERVAL

            hud_key = (image.shape, tuple(_hud_lines))
            if hud_key != _hud_ovl_key[0]:
                max_w = 0
                for text, _ in _hud_lines:
                    (tw, _), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, _HUD_FONT_SCALE, _HUD_THICKNESS)
                    if tw > max_w:
                        max_w = tw

                hud_w = max(1, max_w + (_HUD_PAD * 2) + 2)
                hud_h = max(1, (len(_hud_lines) * _HUD_LINE_H) + (_HUD_PAD * 2))
                hud = np.zeros((hud_h, hud_w, 3), dtype=np.uint8)

                y = _HUD_PAD + _HUD_LINE_H - 3
                for text, color in _hud_lines:
                    cv2.putText(hud, text, (_HUD_PAD, y), cv2.FONT_HERSHEY_SIMPLEX, _HUD_FONT_SCALE, (0, 0, 0), _HUD_THICKNESS + 1, cv2.LINE_AA)
                    cv2.putText(hud, text, (_HUD_PAD, y), cv2.FONT_HERSHEY_SIMPLEX, _HUD_FONT_SCALE, color, _HUD_THICKNESS, cv2.LINE_AA)
                    y += _HUD_LINE_H

                mask = hud.any(axis=2).astype(np.uint8) * 255
                if mask.any():
                    h_hud, w_hud = hud.shape[:2]
                    _hud_ovl_roi[0] = hud
                    _hud_ovl_mask[0] = mask
                    _hud_ovl_rect[0] = (_HUD_X, _HUD_Y, w_hud, h_hud)
                else:
                    _hud_ovl_roi[0] = None
                    _hud_ovl_mask[0] = None
                    _hud_ovl_rect[0] = None
                _hud_ovl_key[0] = hud_key

            if _hud_ovl_rect[0] is not None and _hud_ovl_roi[0] is not None and _hud_ovl_mask[0] is not None:
                x, y0, w_roi, h_roi = _hud_ovl_rect[0]
                h_img, w_img = image.shape[:2]
                x1 = max(0, x)
                y1 = max(0, y0)
                x2 = min(w_img, x + w_roi)
                y2 = min(h_img, y0 + h_roi)
                if x2 > x1 and y2 > y1:
                    rx1 = x1 - x
                    ry1 = y1 - y0
                    rx2 = rx1 + (x2 - x1)
                    ry2 = ry1 + (y2 - y1)
                    cv2.copyTo(
                        _hud_ovl_roi[0][ry1:ry2, rx1:rx2],
                        _hud_ovl_mask[0][ry1:ry2, rx1:rx2],
                        image[y1:y2, x1:x2],
                    )

            _stage_times["text_draw"] = (_stage_times.get("text_draw", 0.0) * 0.9 +
                                          (time.perf_counter() - t_stage) * 0.1)

            # Top-right freeD overlay (capture is full speed in recv_loop; this is display-rate rendering).
            t_stage = time.perf_counter()
            if now >= _freed_next_update or not _freed_lines:
                with _freed_lock:
                    freed_total = int(_freed_state["packet_total"])
                    freed_valid = int(_freed_state["packet_valid"])
                    freed_last_perf = float(_freed_state["last_perf"])
                    freed_last = dict(_freed_state["last"]) if isinstance(_freed_state["last"], dict) else None
                    freed_last_mode = str(_freed_state.get("last_mode", "-"))
                    freed_last_addr = str(_freed_state.get("last_addr", "-"))

                lines: list[tuple[str, tuple[int, int, int]]] = []
                age_ms = (now - freed_last_perf) * 1000.0 if freed_last_perf > 0 else -1.0
                header_color = (0, 255, 255) if freed_valid > 0 else (0, 170, 255)
                lines.append((f"freeD: valid={freed_valid} total={freed_total}", header_color))

                if freed_last is not None:
                    lines.append((
                        f"src={freed_last_addr}  [{freed_last_mode}]  age={age_ms:.0f}ms",
                        (0, 255, 255),
                    ))
                    lines.append((
                        f"P: {_freed_fmt_angle(freed_last.get('pan'), freed_angle_scale)}"
                        f"  T: {_freed_fmt_angle(freed_last.get('tilt'), freed_angle_scale)}"
                        f"  R: {_freed_fmt_angle(freed_last.get('roll'), freed_angle_scale)} deg",
                        (0, 255, 255),
                    ))
                    lines.append((
                        f"X: {freed_last.get('x', '-')}  "
                        f"Y: {freed_last.get('y', '-')}  "
                        f"Z: {freed_last.get('z', '-')}",
                        (0, 255, 255),
                    ))
                    lines.append((
                        f"Zoom: {_freed_fmt_zoom(freed_last.get('zoom'))}  "
                        f"Focus: {_freed_fmt_focus(freed_last.get('focus'))}",
                        (0, 255, 255),
                    ))
                else:
                    lines.append(("Waiting for freeD UDP data...", (0, 170, 255)))

                _freed_lines = lines
                _freed_next_update = now + _FREED_UPDATE_INTERVAL

            freed_key = (image.shape, tuple(_freed_lines))
            if freed_key != _freed_ovl_key[0]:
                max_w = 0
                for text, _ in _freed_lines:
                    (tw, _), _ = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, _FREED_FONT_SCALE, _FREED_THICKNESS)
                    if tw > max_w:
                        max_w = tw

                box_w = max(1, max_w + (_FREED_BOX_PAD * 2) + 2)
                # Ratchet: only grow the box width, never shrink it.
                # This keeps the left edge of the box fixed once widest content is seen.
                if box_w > _freed_max_box_w[0]:
                    _freed_max_box_w[0] = box_w
                box_w = _freed_max_box_w[0]
                box_h = max(1, (len(_freed_lines) * _FREED_LINE_H) + (_FREED_BOX_PAD * 2))
                box = np.zeros((box_h, box_w, 3), dtype=np.uint8)

                y = _FREED_BOX_PAD + _FREED_LINE_H - 3
                for text, color in _freed_lines:
                    cv2.putText(box, text, (_FREED_BOX_PAD, y), cv2.FONT_HERSHEY_SIMPLEX, _FREED_FONT_SCALE, (0, 0, 0), _FREED_THICKNESS + 1, cv2.LINE_AA)
                    cv2.putText(box, text, (_FREED_BOX_PAD, y), cv2.FONT_HERSHEY_SIMPLEX, _FREED_FONT_SCALE, color, _FREED_THICKNESS, cv2.LINE_AA)
                    y += _FREED_LINE_H

                mask = box.any(axis=2).astype(np.uint8) * 255
                if mask.any():
                    x = max(0, width - box_w - _FREED_PAD)
                    y0 = _FREED_PAD
                    _freed_ovl_roi[0] = box
                    _freed_ovl_mask[0] = mask
                    _freed_ovl_rect[0] = (x, y0, box_w, box_h)
                else:
                    _freed_ovl_roi[0] = None
                    _freed_ovl_mask[0] = None
                    _freed_ovl_rect[0] = None
                _freed_ovl_key[0] = freed_key

            if _freed_ovl_rect[0] is not None and _freed_ovl_roi[0] is not None and _freed_ovl_mask[0] is not None:
                x, y0, w_roi, h_roi = _freed_ovl_rect[0]
                h_img, w_img = image.shape[:2]
                x1 = max(0, x)
                y1 = max(0, y0)
                x2 = min(w_img, x + w_roi)
                y2 = min(h_img, y0 + h_roi)
                if x2 > x1 and y2 > y1:
                    rx1 = x1 - x
                    ry1 = y1 - y0
                    rx2 = rx1 + (x2 - x1)
                    ry2 = ry1 + (y2 - y1)
                    cv2.copyTo(
                        _freed_ovl_roi[0][ry1:ry2, rx1:rx2],
                        _freed_ovl_mask[0][ry1:ry2, rx1:rx2],
                        image[y1:y2, x1:x2],
                    )

            _stage_times["freed_draw"] = (_stage_times.get("freed_draw", 0.0) * 0.9 +
                                            (time.perf_counter() - t_stage) * 0.1)

            # Composite cached NDI info overlay using a small ROI blit.
            t_stage = time.perf_counter()
            # Cache key: only static metadata (not timecode, which changes every frame).
            # Round capture_fps to nearest 5 to avoid cache invalidation on fps fluctuations.
            ndi_ovl_key = (
                image.shape,
                round(capture_fps / 5) * 5,
                ndi_meta.get('source', '?'),
                ndi_meta.get('xres', '?'),
                ndi_meta.get('yres', '?'),
                ndi_meta.get('fps_str', '?'),
                ndi_meta.get('fourcc', '?'),
                ndi_meta.get('aspect', '?'),
                ndi_meta.get('frame_fmt', '?'),
                ndi_meta.get('stride', '?'),
            )
            if ndi_ovl_key != _ndi_ovl_key[0]:
                ovl = np.zeros_like(image)
                draw_ndi_info_overlay(ovl, ndi_meta, capture_fps)
                mask = ovl.any(axis=2).astype(np.uint8) * 255
                coords = cv2.findNonZero(mask)
                if coords is None:
                    _ndi_ovl_roi[0] = None
                    _ndi_ovl_mask[0] = None
                    _ndi_ovl_rect[0] = None
                else:
                    x, y, w_roi, h_roi = cv2.boundingRect(coords)
                    _ndi_ovl_roi[0] = ovl[y:y + h_roi, x:x + w_roi].copy()
                    _ndi_ovl_mask[0] = mask[y:y + h_roi, x:x + w_roi].copy()
                    _ndi_ovl_rect[0] = (x, y, w_roi, h_roi)
                _ndi_ovl_key[0] = ndi_ovl_key
            if _ndi_ovl_rect[0] is not None and _ndi_ovl_roi[0] is not None and _ndi_ovl_mask[0] is not None:
                x, y, w_roi, h_roi = _ndi_ovl_rect[0]
                cv2.copyTo(_ndi_ovl_roi[0], _ndi_ovl_mask[0], image[y:y + h_roi, x:x + w_roi])
            _stage_times["ndi_ovl"] = (_stage_times.get("ndi_ovl", 0.0) * 0.9 +
                                        (time.perf_counter() - t_stage) * 0.1)

            if _use_gl:
                t_stage = time.perf_counter()
                h_f, w_f = image.shape[:2]
                # Resize GLFW window to match frame on first frame.
                if _gl_tex_size == (0, 0):
                    glfw.set_window_size(_glfw_win, w_f, h_f)
                # Upload BGR frame directly — GL_BGR tells the GPU to swap channels;
                # no CPU-side color conversion needed.
                img_c = np.ascontiguousarray(image)
                glBindTexture(GL_TEXTURE_2D, _gl_tex_id)
                if _gl_tex_size != (w_f, h_f):
                    glTexImage2D(GL_TEXTURE_2D, 0, GL_RGB, w_f, h_f, 0, GL_BGR, GL_UNSIGNED_BYTE, img_c)
                    _gl_tex_size = (w_f, h_f)
                else:
                    # glTexSubImage2D is faster (no realloc); update pixels in-place.
                    glTexSubImage2D(GL_TEXTURE_2D, 0, 0, 0, w_f, h_f, GL_BGR, GL_UNSIGNED_BYTE, img_c)
                vw, vh = glfw.get_framebuffer_size(_glfw_win)
                glViewport(0, 0, vw, vh)
                glClear(GL_COLOR_BUFFER_BIT)
                glEnable(GL_TEXTURE_2D)
                # Full-screen quad; v=0 maps to first image row (top of screen).
                glBegin(GL_TRIANGLE_STRIP)
                glTexCoord2f(0.0, 0.0); glVertex2f(-1.0,  1.0)  # top-left
                glTexCoord2f(1.0, 0.0); glVertex2f( 1.0,  1.0)  # top-right
                glTexCoord2f(0.0, 1.0); glVertex2f(-1.0, -1.0)  # bottom-left
                glTexCoord2f(1.0, 1.0); glVertex2f( 1.0, -1.0)  # bottom-right
                glEnd()
                glDisable(GL_TEXTURE_2D)
                glfw.swap_buffers(_glfw_win)
                _stage_times["gl_upload_render"] = (_stage_times.get("gl_upload_render", 0.0) * 0.9 +
                                                     (time.perf_counter() - t_stage) * 0.1)
            else:
                t_stage = time.perf_counter()
                cv2.imshow(title, image)
                _stage_times["cv2_imshow"] = (_stage_times.get("cv2_imshow", 0.0) * 0.9 +
                                               (time.perf_counter() - t_stage) * 0.1)

            # Track actual rendered output FPS and log stage times periodically.
            now = time.perf_counter()
            _stage_frame_count += 1
            with _stats_lock:
                _stats["output_frames"] += 1
                out_elapsed = now - _stats["output_tick"]
                if out_elapsed >= 1.0:
                    _stats["output_fps"] = _stats["output_frames"] / out_elapsed
                    if _stage_frame_count >= 30 and _stage_times:
                        # Print stage timing breakdown every ~30 frames
                        total_stage = sum(_stage_times.values())
                        breakdown = " | ".join(f"{k}={v*1000:.1f}ms" for k, v in sorted(_stage_times.items()))
                        print(f"Display stages (total {total_stage*1000:.1f}ms): {breakdown}")
                        _stage_frame_count = 0
                    _stats["output_frames"] = 0
                    _stats["output_tick"] = now
    except KeyboardInterrupt:
        pass
    finally:
        _quit.set()
        if _timer_set:
            try:
                ctypes.windll.winmm.timeEndPeriod(1)
            except Exception:
                pass
        recv_thread.join(timeout=3.0)
        for t in analyze_threads:
            t.join(timeout=3.0)
        display_prep_thread.join(timeout=3.0)
        freed_thread.join(timeout=2.0)
        raw_recorder.shutdown()
        if board_pose_stream is not None:
            board_pose_stream.stop()
        mqtt_pub.stop()
        telemetry.stop()
        ndi.recv_destroy(recv)
        if not no_display:
            if _use_gl and _glfw_win is not None:
                glfw.terminate()
            else:
                cv2.destroyAllWindows()

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Video-file GPU preview with AprilTag detection")
    parser.add_argument("--list", action="store_true", help="List discovered video files and exit")
    parser.add_argument(
        "--video-path",
        type=str,
        default="",
        help="Input video file path. If omitted, the first supported file in --video-search-dir is used.",
    )
    parser.add_argument("--video-search-dir", type=str, default="Videos", help="Folder used for --list and implicit input selection")
    parser.add_argument("--video-loop", action="store_true", help="Loop playback when the video reaches end-of-file")
    parser.add_argument("--video-no-realtime", action="store_true", help="Process as fast as possible instead of pacing to media timestamps")
    parser.add_argument("--video-decode-backend", type=str, choices=["auto", "opencv", "pyav"], default="auto", help="Video decode backend (auto prefers pyav when available)")
    parser.add_argument("--video-decode-threads", type=int, default=0, help="Decoder thread count for PyAV (0=FFmpeg default)")
    parser.add_argument("--video-deinterlace", type=str, choices=["auto", "off", "blend"], default="auto", help="Video deinterlace mode: auto uses stream metadata, blend applies a simple deinterlace filter, off keeps source frames untouched")
    parser.add_argument("--interlaced-fast-profile", action="store_true", help="Use a faster AprilTag detector profile when the source is interlaced or deinterlacing is enabled")
    parser.add_argument("--no-display", action="store_true", help="Receive and decode without opening an OpenCV window")
    parser.add_argument("--gpu-index", type=int, default=0, help="GPU index for nvidia-smi telemetry")
    parser.add_argument("--telemetry-interval", type=float, default=1.0, help="Seconds between GPU telemetry samples")
    parser.add_argument(
        "--dicts",
        type=str,
        default="DICT_APRILTAG_36h11",
        help="Comma-separated ArUco dictionaries (default: DICT_APRILTAG_36h11)",
    )
    parser.add_argument("--show-timestamp", action="store_true", help="Also show local system timestamp in overlay")
    parser.add_argument("--raw-record-output-dir", type=str, default="recordings", help="Output root directory for raw recording sessions")
    parser.add_argument("--raw-record-backend", type=str, choices=["auto", "opencv", "ffmpeg"], default="ffmpeg", help="Raw recorder backend: ffmpeg, auto (ffmpeg->opencv), or opencv")
    parser.add_argument("--raw-record-ffmpeg-bin", type=str, default="ffmpeg", help="FFmpeg executable/path used when raw-record backend is ffmpeg or auto")
    parser.add_argument("--raw-record-ffmpeg-encoder", type=str, default="h264_nvenc", help="FFmpeg video encoder (for example: h264_nvenc, hevc_nvenc, libx264)")
    parser.add_argument("--raw-record-ffmpeg-preset", type=str, default="p5", help="FFmpeg encoder preset (for NVENC typical values: p1..p7)")
    parser.add_argument("--raw-record-scale", type=float, default=0.5, help="Scale factor applied to recorded frames (analysis still uses full frame)")
    parser.add_argument("--raw-record-start-disabled", action="store_true", help="Do not auto-start clean recording at run start")
    parser.add_argument("--overlay-data-disable", action="store_true", help="Disable overlay metadata capture and second-pass overlay rendering")
    parser.add_argument("--focal-length", type=float, default=1000.0, help="Camera focal length (pixels) for pose estimation")
    parser.add_argument("--tag-size-mm", type=float, default=148.6, help="Physical tag width in millimeters (default: 148.6)")
    parser.add_argument(
        "--tag-size-map-json",
        type=str,
        default="",
        help="Optional JSON path mapping tag_id to tag_size_mm; overrides board/default sizes",
    )
    parser.add_argument("--analysis-workers", type=int, default=0, help="Number of analysis worker threads (0=auto)")
    parser.add_argument("--display-fps", type=float, default=30.0, help="Display frame rate cap for color preview (default: 30)")
    parser.add_argument("--display-scale", type=float, default=0.5, help="Display-only scale factor for preview frames (default: 0.5)")
    parser.add_argument("--display-prep-oversample", type=float, default=4.0 / 3.0, help="Oversample factor for display-prep frame capture (default: 1.333)")
    parser.add_argument("--display-delay-frames", type=int, default=2, help="Display jitter buffer depth in frames (default: 2)")
    parser.add_argument("--sync-timeout-ms", type=float, default=33.0, help="Max wait for exact frame-match before fallback (default: 33ms)")
    parser.add_argument("--freed-angle-scale", type=float, default=32768.0, help="Degrees divisor for freeD pan/tilt/roll")
    parser.add_argument("--freed-listen-ip", type=str, default="0.0.0.0", help="IP to bind for freeD UDP listener (default: 0.0.0.0)")
    parser.add_argument("--freed-port", type=int, default=10244, help="UDP port for freeD packets (default: 10244)")
    parser.add_argument("--mqtt-enable", action="store_true", help="Enable MQTT telemetry publishing")
    parser.add_argument("--parquet-disable", action="store_true", help="Disable Parquet telemetry sink (enabled by default)")
    parser.add_argument("--parquet-output-dir", type=str, default="recordings/telemetry", help="Output directory for Parquet telemetry sessions")
    parser.add_argument("--mqtt-host", type=str, default="127.0.0.1", help="MQTT broker host (default: 127.0.0.1)")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port (default: 1883)")
    parser.add_argument("--mqtt-topic-prefix", type=str, default="video/telemetry", help="MQTT topic prefix")
    parser.add_argument("--board-pose-stream-enable", action="store_true", help="Enable FurioDataStream-compatible board pose server")
    parser.add_argument("--board-pose-stream-host", type=str, default="0.0.0.0", help="Board pose stream bind host (default: 0.0.0.0)")
    parser.add_argument("--board-pose-stream-port", type=int, default=9102, help="Board pose stream TCP port (default: 9102)")
    parser.add_argument("--board-pose-stream-hz", type=float, default=50.0, help="Board pose stream send rate in Hz (default: 50)")
    parser.add_argument("--telemetry-record-start-disabled", action="store_true", help="Start with telemetry recording disabled")
    parser.add_argument(
        "--board-json",
        action="append",
        default=[],
        help="Path to ArUco board JSON for OpenCV estimatePoseBoard (repeatable or comma-separated)",
    )
    # Detector tuning parameters for robustness (distance, soft focus, lighting)
    parser.add_argument(
        "--april-tag-quad-decimate",
        type=float,
        default=2.0,
        help="Decimation factor for edge detection. Lower (1.0-1.5) for small/distant tags. Default: 2.0",
    )
    parser.add_argument(
        "--april-tag-quad-sigma",
        type=float,
        default=0.0,
        help="Gaussian blur sigma before edge detection. Higher (0.5-1.5) improves soft-focus tolerance. Default: 0.0",
    )
    parser.add_argument(
        "--adaptive-thresh-win-size-min",
        type=int,
        default=3,
        help="Minimum adaptive threshold window size (must be odd). Default: 3",
    )
    parser.add_argument(
        "--adaptive-thresh-win-size-max",
        type=int,
        default=23,
        help="Maximum adaptive threshold window size (must be odd). Larger tolerates bigger lighting gradients. Default: 23",
    )
    parser.add_argument(
        "--corner-refinement-method",
        type=int,
        default=None,
        help="Corner refinement method: 0=NONE, 1=SUBPIX, 2=CONTOUR, 3=APRILTAG (best quality). Default: None (uses detector default)",
    )
    parser.add_argument(
        "--min-marker-perimeter-rate",
        type=float,
        default=0.03,
        help="Minimum marker perimeter as fraction of image diagonal. Lower (0.01-0.02) catches smaller tags. Default: 0.03",
    )
    parser.add_argument(
        "--error-correction-rate",
        type=float,
        default=0.6,
        help="Hamming distance acceptance threshold (0.0-1.0). Higher tolerates more bit errors. Default: 0.6",
    )
    parser.add_argument(
        "--april-tag-min-white-black-diff",
        type=int,
        default=5,
        help="Minimum pixel value difference to detect edge. Higher (10-20) reduces noise sensitivity. Default: 5",
    )
    parser.add_argument(
        "--enable-board-refinement",
        action="store_true",
        help="Enable board-geometry-aware marker refinement. Recovers markers missed by initial detection using board constraints.",
    )
    args = parser.parse_args()

    board_json_paths: list[str] = []
    for raw_value in args.board_json:
        for item in str(raw_value).split(","):
            p = item.strip()
            if p and p not in board_json_paths:
                board_json_paths.append(p)

    if args.list:
        return list_videos(args.video_search_dir)

    try:
        video_path = resolve_video_path(args.video_path, args.video_search_dir)
        dict_names = parse_dict_names(args.dicts)

        print(f"Opening video: {video_path}")
        print("Press 'q'/'Esc' to quit, 't' to toggle telemetry recording, 'r' to toggle raw recording." if not args.no_display else "Running headless. Press Ctrl+C to stop.")
        return run_video_preview(
            video_path=video_path,
            no_display=args.no_display,
            telemetry_interval=args.telemetry_interval,
            gpu_index=max(0, args.gpu_index),
            dict_names=dict_names,
            show_timestamp=args.show_timestamp,
            focal_length=args.focal_length,
            tag_size_mm=args.tag_size_mm,
            tag_size_map_json_path=(args.tag_size_map_json.strip() if args.tag_size_map_json else None),
            analysis_workers=max(0, args.analysis_workers),
            display_fps=max(1.0, args.display_fps),
            display_scale=min(1.0, max(0.1, args.display_scale)),
            display_prep_oversample=max(1.0, args.display_prep_oversample),
            display_delay_frames=max(0, args.display_delay_frames),
            sync_timeout_ms=max(0.0, args.sync_timeout_ms),
            freed_angle_scale=float(args.freed_angle_scale),
            freed_listen_ip=args.freed_listen_ip,
            freed_port=int(args.freed_port),
            mqtt_enable=bool(args.mqtt_enable),
            mqtt_host=args.mqtt_host,
            mqtt_port=int(args.mqtt_port),
            mqtt_topic_prefix=args.mqtt_topic_prefix,
            parquet_enable=not bool(args.parquet_disable),
            parquet_output_dir=args.parquet_output_dir,
            board_pose_stream_enable=bool(args.board_pose_stream_enable),
            board_pose_stream_host=args.board_pose_stream_host,
            board_pose_stream_port=int(args.board_pose_stream_port),
            board_pose_stream_hz=float(args.board_pose_stream_hz),
            raw_record_output_dir=args.raw_record_output_dir,
            raw_record_backend=args.raw_record_backend,
            raw_record_ffmpeg_bin=args.raw_record_ffmpeg_bin,
            raw_record_ffmpeg_encoder=args.raw_record_ffmpeg_encoder,
            raw_record_ffmpeg_preset=args.raw_record_ffmpeg_preset,
            raw_record_scale=float(args.raw_record_scale),
            telemetry_record_start_enabled=not bool(args.telemetry_record_start_disabled),
            board_json_paths=board_json_paths,
            # Detector tuning parameters
            april_tag_quad_decimate=float(args.april_tag_quad_decimate),
            april_tag_quad_sigma=float(args.april_tag_quad_sigma),
            adaptive_thresh_win_size_min=int(args.adaptive_thresh_win_size_min),
            adaptive_thresh_win_size_max=int(args.adaptive_thresh_win_size_max),
            corner_refinement_method=args.corner_refinement_method,
            min_marker_perimeter_rate=float(args.min_marker_perimeter_rate),
            error_correction_rate=float(args.error_correction_rate),
            april_tag_min_white_black_diff=int(args.april_tag_min_white_black_diff),
            enable_board_refinement=bool(args.enable_board_refinement),
            video_realtime=not bool(args.video_no_realtime),
            video_loop=bool(args.video_loop),
            video_deinterlace=args.video_deinterlace,
            interlaced_fast_profile=bool(args.interlaced_fast_profile),
            video_decode_backend=args.video_decode_backend,
            video_decode_threads=max(0, int(args.video_decode_threads)),
            raw_record_start_enabled=not bool(args.raw_record_start_disabled),
            overlay_data_enable=not bool(args.overlay_data_disable),
        )
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
