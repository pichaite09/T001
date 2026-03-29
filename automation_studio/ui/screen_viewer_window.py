from __future__ import annotations

import concurrent.futures
import importlib
import io
import os
import re
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets


def _safe_name(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "_", value or "").strip("_")
    return sanitized or "device"


def _build_frame_output_path(device_name: str, serial: str, output_dir: Path | None = None) -> Path:
    root = output_dir or (Path.cwd() / "artifacts" / "screen_wall")
    root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    safe_device = _safe_name(device_name)
    safe_serial = _safe_name(serial)
    return root / f"{safe_device}_{safe_serial}_{timestamp}.png"


def _settings() -> QtCore.QSettings:
    return QtCore.QSettings("AutomationStudio", "ScreenViewer")


def load_saved_scrcpy_path() -> str | None:
    candidate = str(_settings().value("scrcpy/program_path", "") or "").strip()
    if candidate and Path(candidate).exists():
        return candidate
    return None


def save_scrcpy_path(path: str) -> None:
    _settings().setValue("scrcpy/program_path", path)


def find_scrcpy_executable(search_roots: list[Path] | None = None) -> str | None:
    env_candidate = str(os.environ.get("SCRCPY_PATH") or "").strip()
    if env_candidate and Path(env_candidate).exists():
        return env_candidate
    saved_candidate = load_saved_scrcpy_path()
    if saved_candidate:
        return saved_candidate
    executable = shutil.which("scrcpy") or shutil.which("scrcpy.exe")
    if executable:
        return executable
    candidate_roots = list(search_roots or [])
    candidate_roots.extend(
        [
            Path.cwd(),
            Path.cwd() / "tools",
            Path.cwd() / "bin",
        ]
    )
    for root in candidate_roots:
        for candidate in (
            root / "scrcpy.exe",
            root / "scrcpy",
            root / "scrcpy" / "scrcpy.exe",
        ):
            if candidate.exists():
                return str(candidate)
    return None


class ViewerImageLabel(QtWidgets.QLabel):
    double_clicked = QtCore.Signal()
    clicked = QtCore.Signal(QtCore.QPoint)
    pointer_moved = QtCore.Signal(QtCore.QPoint)
    pointer_left = QtCore.Signal()

    def __init__(self, text: str = "", parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(text, parent)
        self._hover_point: QtCore.QPoint | None = None
        self._last_tap_point: QtCore.QPoint | None = None
        self.setMouseTracking(True)

    def mouseDoubleClickEvent(self, event: QtGui.QMouseEvent) -> None:
        self.double_clicked.emit()
        super().mouseDoubleClickEvent(event)

    def mousePressEvent(self, event: QtGui.QMouseEvent) -> None:
        if event.button() == QtCore.Qt.MouseButton.LeftButton:
            self.clicked.emit(event.position().toPoint())
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event: QtGui.QMouseEvent) -> None:
        point = event.position().toPoint()
        self.pointer_moved.emit(point)
        self._hover_point = point
        self.update()
        super().mouseMoveEvent(event)

    def leaveEvent(self, event: QtCore.QEvent) -> None:
        self._hover_point = None
        self.pointer_left.emit()
        self.update()
        super().leaveEvent(event)

    def set_overlay_points(
        self,
        *,
        hover_point: QtCore.QPoint | None = None,
        last_tap_point: QtCore.QPoint | None = None,
    ) -> None:
        self._hover_point = hover_point
        self._last_tap_point = last_tap_point
        self.update()

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:
        super().paintEvent(event)
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
        if self._hover_point is not None:
            painter.setPen(QtGui.QPen(QtGui.QColor("#7dd3fc"), 1, QtCore.Qt.PenStyle.DashLine))
            painter.drawLine(0, self._hover_point.y(), self.width(), self._hover_point.y())
            painter.drawLine(self._hover_point.x(), 0, self._hover_point.x(), self.height())
        if self._last_tap_point is not None:
            painter.setPen(QtGui.QPen(QtGui.QColor("#f87171"), 2))
            painter.setBrush(QtCore.Qt.BrushStyle.NoBrush)
            painter.drawEllipse(self._last_tap_point, 10, 10)
        painter.end()


class StatusVisualLabel(QtWidgets.QWidget):
    _COLORS = {
        "idle": ("#101c2d", "#26405d", "#94a3b8"),
        "busy": ("#0f2342", "#1d4ed8", "#60a5fa"),
        "success": ("#0e2a1d", "#15803d", "#86efac"),
        "warning": ("#2f2305", "#ca8a04", "#fcd34d"),
        "error": ("#301018", "#dc2626", "#fda4af"),
    }

    def __init__(self, text: str = "Status: idle", parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._text = ""
        self._state = "idle"
        self.setSizePolicy(QtWidgets.QSizePolicy.Policy.Fixed, QtWidgets.QSizePolicy.Policy.Fixed)
        self.setFixedSize(28, 20)
        self.setText(text)

    def text(self) -> str:
        return self._text

    def setText(self, text: str) -> None:
        normalized = str(text or "").strip() or "Status: idle"
        self._text = normalized
        self._state = self._infer_state(normalized)
        self.setToolTip(normalized)
        self.setStatusTip(normalized)
        self.update()

    def sizeHint(self) -> QtCore.QSize:
        return QtCore.QSize(28, 20)

    def minimumSizeHint(self) -> QtCore.QSize:
        return self.sizeHint()

    def _infer_state(self, text: str) -> str:
        lower = text.lower()
        if any(keyword in lower for keyword in ("fail", "error", "unavailable", "not configured")):
            return "error"
        if any(
            keyword in lower
            for keyword in (
                "no devices",
                "no selected",
                "select a workflow",
                "stop already",
                "paused",
                "stopping",
                "stopped",
            )
        ):
            return "warning"
        if any(keyword in lower for keyword in ("refreshing", "running", "reconnecting")):
            return "busy"
        if any(
            keyword in lower
            for keyword in (
                "saved",
                "opened realtime",
                "refreshed",
                "workflow finished",
                "workflow completed",
                "selected",
                "selection cleared",
                "brightness set",
                "home sent",
                "back sent",
                "recent apps sent",
                "preset ",
                "resolution set",
                "using scrcpy",
                "loaded",
            )
        ):
            return "success"
        return "idle"

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:
        super().paintEvent(event)
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.RenderHint.Antialiasing, True)
        rect = self.rect().adjusted(1, 1, -1, -1)
        background, border, foreground = self._COLORS.get(self._state, self._COLORS["idle"])
        painter.setPen(QtGui.QPen(QtGui.QColor(border), 1))
        painter.setBrush(QtGui.QColor(background))
        painter.drawRoundedRect(rect, 9, 9)

        icon_rect = rect.adjusted(6, 4, -6, -4)
        pen = QtGui.QPen(QtGui.QColor(foreground), 2, QtCore.Qt.PenStyle.SolidLine, QtCore.Qt.PenCapStyle.RoundCap)
        painter.setPen(pen)
        painter.setBrush(QtGui.QColor(foreground))

        if self._state == "success":
            path = QtGui.QPainterPath()
            path.moveTo(icon_rect.left() + 1, icon_rect.center().y())
            path.lineTo(icon_rect.left() + 5, icon_rect.bottom() - 1)
            path.lineTo(icon_rect.right() - 1, icon_rect.top() + 1)
            painter.drawPath(path)
        elif self._state == "error":
            painter.drawLine(icon_rect.topLeft(), icon_rect.bottomRight())
            painter.drawLine(icon_rect.topRight(), icon_rect.bottomLeft())
        elif self._state == "warning":
            center_x = icon_rect.center().x()
            painter.drawLine(center_x, icon_rect.top(), center_x, icon_rect.bottom() - 4)
            painter.drawPoint(center_x, icon_rect.bottom())
        elif self._state == "busy":
            bar_width = 2
            spacing = 2
            heights = (4, 7, 10)
            start_x = icon_rect.center().x() - ((bar_width * len(heights)) + (spacing * (len(heights) - 1))) // 2
            for index, height in enumerate(heights):
                x = start_x + (index * (bar_width + spacing))
                bar_rect = QtCore.QRectF(
                    x,
                    icon_rect.bottom() - height,
                    bar_width,
                    height,
                )
                painter.drawRoundedRect(bar_rect, 1, 1)
        else:
            painter.drawEllipse(icon_rect.center(), 3, 3)
        painter.end()


class FrameRefreshSignals(QtCore.QObject):
    finished = QtCore.Signal(object, object)


class FrameRefreshWorker(QtCore.QRunnable):
    def __init__(self, tile: object, capture_fn) -> None:
        super().__init__()
        self.tile = tile
        self.capture_fn = capture_fn
        self.signals = FrameRefreshSignals()

    def run(self) -> None:
        try:
            result = self.capture_fn()
        except Exception as exc:
            result = {"ok": False, "error": str(exc)}
        self.signals.finished.emit(self.tile, result)


class WorkflowBatchRunner(QtCore.QThread):
    progress = QtCore.Signal(object)
    result_ready = QtCore.Signal(dict)

    def __init__(
        self,
        workflow_service: Any,
        workflow_id: int,
        device_records: list[dict[str, Any]],
        max_parallel: int = 1,
    ) -> None:
        super().__init__()
        self.workflow_service = workflow_service
        self.workflow_id = int(workflow_id)
        self.device_records = [dict(record) for record in device_records]
        self.max_parallel = max(int(max_parallel or 1), 1)
        self._stop_requested = False

    def request_stop(self) -> None:
        self._stop_requested = True

    def run(self) -> None:
        total = len(self.device_records)
        results: list[dict[str, Any]] = []
        success_count = 0
        stopped_count = 0
        completed_count = 0

        def _run_one(device_record: dict[str, Any]) -> tuple[int, str, dict[str, Any]]:
            device_id = int(device_record.get("id") or 0)
            device_name = str(device_record.get("name") or device_record.get("serial") or f"Device {device_id}")
            if not device_id:
                return device_id, device_name, {"success": False, "message": "Device is missing a database id"}
            try:
                return device_id, device_name, self.workflow_service.execute_workflow(self.workflow_id, device_id)
            except Exception as exc:
                return device_id, device_name, {"success": False, "message": str(exc)}

        queued_records = list(self.device_records)
        in_flight: dict[concurrent.futures.Future[tuple[int, str, dict[str, Any]]], tuple[int, dict[str, Any]]] = {}
        launch_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_parallel, max(total, 1))) as executor:
            while (queued_records and not self._stop_requested) or in_flight:
                while queued_records and not self._stop_requested and len(in_flight) < self.max_parallel:
                    device_record = queued_records.pop(0)
                    launch_count += 1
                    device_id = int(device_record.get("id") or 0)
                    device_name = str(device_record.get("name") or device_record.get("serial") or f"Device {device_id}")
                    self.progress.emit(
                        {
                            "phase": "started",
                            "current": launch_count,
                            "total": total,
                            "device_id": device_id,
                            "device_name": device_name,
                        }
                    )
                    future = executor.submit(_run_one, device_record)
                    in_flight[future] = (launch_count, dict(device_record))

                if not in_flight:
                    break

                done, _pending = concurrent.futures.wait(
                    list(in_flight.keys()),
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    launch_index, device_record = in_flight.pop(future)
                    device_id, device_name, result = future.result()
                    if result.get("success"):
                        success_count += 1
                    results.append(
                        {
                            "device_id": device_id,
                            "device_name": device_name,
                            "result": result,
                        }
                    )
                    completed_count += 1
                    self.progress.emit(
                        {
                            "phase": "finished",
                            "current": completed_count,
                            "launch_index": launch_index,
                            "total": total,
                            "device_id": device_id,
                            "device_name": device_name,
                            "result": result,
                        }
                    )

        if self._stop_requested and queued_records:
            for device_record in queued_records:
                device_id = int(device_record.get("id") or 0)
                device_name = str(device_record.get("name") or device_record.get("serial") or f"Device {device_id}")
                stopped_count += 1
                results.append(
                    {
                        "device_id": device_id,
                        "device_name": device_name,
                        "result": {"success": False, "stopped": True, "message": "Stopped before execution"},
                    }
                )
        self.result_ready.emit(
            {
                "workflow_id": self.workflow_id,
                "total": total,
                "success_count": success_count,
                "failure_count": max(total - success_count - stopped_count, 0),
                "stopped_count": stopped_count,
                "stopped": bool(self._stop_requested),
                "results": results,
                "success": success_count == total and total > 0 and not self._stop_requested,
            }
        )


class DeviceDetailViewerWindow(QtWidgets.QMainWindow):
    def __init__(
        self,
        *,
        device_name: str,
        serial: str,
        initial_pixmap: QtGui.QPixmap | None = None,
        scrcpy_program: str | None = None,
    ) -> None:
        super().__init__()
        self.device_name = device_name
        self.serial = serial
        self._device = None
        self._scrcpy_program = scrcpy_program if scrcpy_program is not None else find_scrcpy_executable()
        self._scrcpy_process: QtCore.QProcess | None = None
        self._last_pixmap: QtGui.QPixmap | None = None
        self._display_rect = QtCore.QRect()
        self._zoom_factor = 1.0
        self._hover_label_point: QtCore.QPoint | None = None
        self._last_tap_label_point: QtCore.QPoint | None = None
        self._build_ui()
        if initial_pixmap is not None and not initial_pixmap.isNull():
            self.set_pixmap(initial_pixmap)

    def _build_ui(self) -> None:
        self.setWindowTitle(f"{self.device_name} - Detail Viewer")
        self.resize(520, 920)

        root = QtWidgets.QWidget()
        self.setCentralWidget(root)
        layout = QtWidgets.QVBoxLayout(root)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        title = QtWidgets.QLabel(self.device_name)
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel(self.serial)
        subtitle.setObjectName("subtitleLabel")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)

        controls = QtWidgets.QHBoxLayout()
        controls.setSpacing(8)
        controls.addWidget(QtWidgets.QLabel("Zoom"))
        self.zoom_out_button = QtWidgets.QPushButton("-")
        self.zoom_in_button = QtWidgets.QPushButton("+")
        self.zoom_combo = QtWidgets.QComboBox()
        for label, value in (
            ("50%", 0.5),
            ("75%", 0.75),
            ("100%", 1.0),
            ("125%", 1.25),
            ("150%", 1.5),
            ("175%", 1.75),
            ("200%", 2.0),
            ("300%", 3.0),
        ):
            self.zoom_combo.addItem(label, value)
        self.zoom_combo.setCurrentIndex(max(self.zoom_combo.findData(1.0), 0))
        self.fit_button = QtWidgets.QPushButton("Fit")
        self.fit_button.setCheckable(True)
        self.tap_mode_button = QtWidgets.QPushButton("Tap Mode")
        self.tap_mode_button.setCheckable(True)
        self.realtime_button = QtWidgets.QPushButton("Open Realtime")
        self.save_button = QtWidgets.QPushButton("Save Frame")
        self.status_label = QtWidgets.QLabel("Status: waiting for frame")
        self.coord_label = QtWidgets.QLabel("Coords: -")
        self.coord_label.setObjectName("subtitleLabel")
        self.backend_label = QtWidgets.QLabel("Backend: Polling")
        self.backend_label.setObjectName("subtitleLabel")
        controls.addWidget(self.zoom_out_button)
        controls.addWidget(self.zoom_combo)
        controls.addWidget(self.zoom_in_button)
        controls.addWidget(self.fit_button)
        controls.addWidget(self.tap_mode_button)
        controls.addWidget(self.realtime_button)
        controls.addWidget(self.save_button)
        controls.addWidget(self.coord_label)
        controls.addWidget(self.backend_label)
        controls.addWidget(self.status_label)
        controls.addStretch(1)
        layout.addLayout(controls)

        self.scroll_area = QtWidgets.QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        layout.addWidget(self.scroll_area, 1)

        self.image_label = ViewerImageLabel("Waiting for frame")
        self.image_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.image_label.setMinimumSize(360, 640)
        self.scroll_area.setWidget(self.image_label)

        self.zoom_combo.currentIndexChanged.connect(self._update_zoom_factor)
        self.zoom_out_button.clicked.connect(self._step_zoom_out)
        self.zoom_in_button.clicked.connect(self._step_zoom_in)
        self.fit_button.toggled.connect(self._render_pixmap)
        self.realtime_button.clicked.connect(self.toggle_realtime_backend)
        self.save_button.clicked.connect(self.save_current_frame)
        self.image_label.pointer_moved.connect(self._handle_pointer_moved)
        self.image_label.pointer_left.connect(self._handle_pointer_left)
        self.image_label.clicked.connect(self._handle_image_clicked)
        self._update_realtime_controls()

    def set_pixmap(self, pixmap: QtGui.QPixmap | None) -> None:
        if pixmap is None or pixmap.isNull():
            return
        self._last_pixmap = pixmap
        self.status_label.setText(f"Status: {pixmap.width()} x {pixmap.height()}")
        self._render_pixmap()

    def save_current_frame(self, output_dir: Path | None = None) -> Path | None:
        if self._last_pixmap is None or self._last_pixmap.isNull():
            self.status_label.setText("Status: no frame to save")
            return None
        output_path = _build_frame_output_path(self.device_name, self.serial, output_dir=output_dir)
        self._last_pixmap.save(str(output_path), "PNG")
        self.status_label.setText(f"Saved: {output_path.name}")
        return output_path

    def is_realtime_running(self) -> bool:
        return self._scrcpy_process is not None and self._scrcpy_process.state() != QtCore.QProcess.ProcessState.NotRunning

    def ensure_realtime_backend(self) -> bool:
        if self.is_realtime_running():
            return True
        if not self._scrcpy_program:
            self.status_label.setText("Realtime unavailable: scrcpy not found")
            self._update_realtime_controls()
            return False
        arguments = ["-s", self.serial]
        if not self._start_scrcpy_process(self._scrcpy_program, arguments):
            self.status_label.setText("Failed to start scrcpy")
            self._update_realtime_controls()
            return False
        self.status_label.setText("Opened realtime scrcpy window")
        self._update_realtime_controls()
        return True

    def toggle_realtime_backend(self) -> None:
        if self.is_realtime_running():
            self._stop_scrcpy_process()
            self.status_label.setText("Closed realtime backend")
            self._update_realtime_controls()
            return
        self.ensure_realtime_backend()

    def _update_zoom_factor(self) -> None:
        self._zoom_factor = float(self.zoom_combo.currentData() or 1.0)
        self._render_pixmap()

    def _step_zoom_out(self) -> None:
        self.fit_button.setChecked(False)
        self.zoom_combo.setCurrentIndex(max(0, self.zoom_combo.currentIndex() - 1))

    def _step_zoom_in(self) -> None:
        self.fit_button.setChecked(False)
        self.zoom_combo.setCurrentIndex(min(self.zoom_combo.count() - 1, self.zoom_combo.currentIndex() + 1))

    def _render_pixmap(self) -> None:
        if self._last_pixmap is None or self._last_pixmap.isNull():
            self.image_label.setText("Waiting for frame")
            self.image_label.setPixmap(QtGui.QPixmap())
            self._display_rect = QtCore.QRect()
            return
        if self.fit_button.isChecked():
            target_size = self.scroll_area.viewport().size()
        else:
            target_size = QtCore.QSize(
                max(int(self._last_pixmap.width() * self._zoom_factor), 1),
                max(int(self._last_pixmap.height() * self._zoom_factor), 1),
            )
        scaled = self._last_pixmap.scaled(
            target_size,
            QtCore.Qt.AspectRatioMode.KeepAspectRatio,
            QtCore.Qt.TransformationMode.SmoothTransformation,
        )
        self.image_label.setPixmap(scaled)
        self.image_label.setMinimumSize(scaled.size())
        self.image_label.setText("")
        label_rect = self.image_label.contentsRect()
        x = label_rect.x() + max((label_rect.width() - scaled.width()) // 2, 0)
        y = label_rect.y() + max((label_rect.height() - scaled.height()) // 2, 0)
        self._display_rect = QtCore.QRect(x, y, scaled.width(), scaled.height())
        self.image_label.set_overlay_points(
            hover_point=self._hover_label_point,
            last_tap_point=self._last_tap_label_point,
        )

    def _load_uiautomator2(self):
        try:
            return importlib.import_module("uiautomator2")
        except ImportError as exc:
            raise RuntimeError("uiautomator2 is not installed. Run pip install -r requirements.txt") from exc

    def _ensure_connected_device(self):
        if self._device is not None:
            return self._device
        uiautomator2 = self._load_uiautomator2()
        self._device = uiautomator2.connect(self.serial)
        return self._device

    def _map_label_point_to_device_coords(self, point: QtCore.QPoint) -> tuple[int, int] | None:
        if self._last_pixmap is None or self._last_pixmap.isNull() or not self._display_rect.contains(point):
            return None
        if self._display_rect.width() <= 0 or self._display_rect.height() <= 0:
            return None
        relative_x = (point.x() - self._display_rect.x()) / self._display_rect.width()
        relative_y = (point.y() - self._display_rect.y()) / self._display_rect.height()
        mapped_x = int(round(relative_x * self._last_pixmap.width()))
        mapped_y = int(round(relative_y * self._last_pixmap.height()))
        mapped_x = max(0, min(mapped_x, self._last_pixmap.width() - 1))
        mapped_y = max(0, min(mapped_y, self._last_pixmap.height() - 1))
        return mapped_x, mapped_y

    def _handle_pointer_moved(self, point: QtCore.QPoint) -> None:
        mapped = self._map_label_point_to_device_coords(point)
        self._hover_label_point = point if mapped is not None else None
        self.image_label.set_overlay_points(
            hover_point=self._hover_label_point,
            last_tap_point=self._last_tap_label_point,
        )
        if mapped is None:
            self.coord_label.setText("Coords: -")
            return
        self.coord_label.setText(f"Coords: {mapped[0]}, {mapped[1]}")

    def _handle_pointer_left(self) -> None:
        self._hover_label_point = None
        self.coord_label.setText("Coords: -")
        self.image_label.set_overlay_points(
            hover_point=None,
            last_tap_point=self._last_tap_label_point,
        )

    def _tap_device(self, x: int, y: int) -> None:
        device = self._ensure_connected_device()
        device.click(x, y)

    def _handle_image_clicked(self, point: QtCore.QPoint) -> None:
        mapped = self._map_label_point_to_device_coords(point)
        if mapped is None:
            return
        self._last_tap_label_point = point
        self.image_label.set_overlay_points(
            hover_point=self._hover_label_point,
            last_tap_point=self._last_tap_label_point,
        )
        if not self.tap_mode_button.isChecked():
            self.status_label.setText(f"Preview tap: {mapped[0]}, {mapped[1]}")
            return
        try:
            self._tap_device(*mapped)
        except Exception as exc:
            self._device = None
            self.status_label.setText(f"Tap failed: {exc}")
            return
        self.status_label.setText(f"Tapped: {mapped[0]}, {mapped[1]}")
        QtCore.QTimer.singleShot(150, self._clear_last_tap_overlay)

    def _clear_last_tap_overlay(self) -> None:
        self._last_tap_label_point = None
        self.image_label.set_overlay_points(
            hover_point=self._hover_label_point,
            last_tap_point=None,
        )

    def _start_scrcpy_process(self, program: str, arguments: list[str]) -> bool:
        self._stop_scrcpy_process()
        process = QtCore.QProcess(self)
        process.finished.connect(self._handle_scrcpy_finished)
        process.start(program, arguments)
        if not process.waitForStarted(1500):
            process.deleteLater()
            return False
        self._scrcpy_process = process
        return True

    def _stop_scrcpy_process(self) -> None:
        if self._scrcpy_process is None:
            return
        if self._scrcpy_process.state() != QtCore.QProcess.ProcessState.NotRunning:
            self._scrcpy_process.terminate()
            self._scrcpy_process.waitForFinished(1000)
            if self._scrcpy_process.state() != QtCore.QProcess.ProcessState.NotRunning:
                self._scrcpy_process.kill()
                self._scrcpy_process.waitForFinished(1000)
        self._scrcpy_process.deleteLater()
        self._scrcpy_process = None

    def _handle_scrcpy_finished(self, *_args) -> None:
        if self._scrcpy_process is not None:
            self._scrcpy_process.deleteLater()
        self._scrcpy_process = None
        self._update_realtime_controls()

    def _update_realtime_controls(self) -> None:
        if not self._scrcpy_program:
            self.realtime_button.setEnabled(False)
            self.realtime_button.setText("Realtime Unavailable")
            self.backend_label.setText("Backend: Polling only")
            return
        self.realtime_button.setEnabled(True)
        if self.is_realtime_running():
            self.realtime_button.setText("Close Realtime")
            self.backend_label.setText("Backend: scrcpy realtime")
            return
        self.realtime_button.setText("Open Realtime")
        self.backend_label.setText("Backend: Polling + scrcpy ready")

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        if self.fit_button.isChecked():
            self._render_pixmap()

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        self._stop_scrcpy_process()
        super().closeEvent(event)


class DeviceScreenTile(QtWidgets.QFrame):
    BASE_IMAGE_SIZE = QtCore.QSize(220, 360)
    DEFAULT_SCREEN_RATIO = 9 / 19.5
    IMAGE_FRAME_PADDING = 0
    TILE_SIDE_MARGIN = 12
    OFFLINE_RETRY_COOLDOWN_SECONDS = 5.0
    ADB_STATE_TIMEOUT_SECONDS = 1.0

    save_requested = QtCore.Signal(object)
    realtime_requested = QtCore.Signal(object)
    frame_updated = QtCore.Signal(object)
    selection_changed = QtCore.Signal(object, bool)

    def __init__(self, device_record: dict[str, Any], parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.device_record = device_record
        self.serial = str(device_record.get("serial") or "").strip()
        self.device_name = str(device_record.get("name") or self.serial or "Device")
        self.device_id = int(device_record.get("id") or 0) or None
        self._device = None
        self._last_pixmap: QtGui.QPixmap | None = None
        self._failure_streak = 0
        self._refresh_pending = False
        self._zoom_factor = 1.0
        self._resolution_scale = 1.0
        self._selected = False
        self._next_refresh_allowed_at = 0.0
        self._status_badge_state = "connected"
        self._workflow_state_key = "idle"
        self._workflow_state_message = ""
        self._build_ui()

    def _build_ui(self) -> None:
        self.setObjectName("deviceTile")
        self.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        self.setSizePolicy(QtWidgets.QSizePolicy.Policy.Fixed, QtWidgets.QSizePolicy.Policy.Maximum)
        self.setMinimumWidth(self._preferred_tile_width())
        self._apply_selection_style()
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(0)

        self.image_card = QtWidgets.QFrame()
        self.image_card.setStyleSheet("background:transparent; border:none;")
        self.image_card.setSizePolicy(QtWidgets.QSizePolicy.Policy.Fixed, QtWidgets.QSizePolicy.Policy.Fixed)
        image_layout = QtWidgets.QGridLayout(self.image_card)
        image_layout.setContentsMargins(0, 0, 0, 0)
        image_layout.setSpacing(0)
        self.image_label = ViewerImageLabel("Waiting for frame")
        self.image_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.image_label.setToolTip("Double-click to open scrcpy realtime")
        placeholder_size = self._target_screen_size()
        self.image_label.setFixedSize(placeholder_size)
        image_layout.addWidget(self.image_label, 0, 0, QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignTop)

        self.overlay_container = QtWidgets.QFrame()
        self.overlay_container.setAttribute(QtCore.Qt.WidgetAttribute.WA_TransparentForMouseEvents, True)
        self.overlay_container.setStyleSheet("background:transparent; border:none;")
        self.overlay_container.setFixedSize(placeholder_size)
        overlay_layout = QtWidgets.QVBoxLayout(self.overlay_container)
        overlay_layout.setContentsMargins(6, 6, 6, 6)
        overlay_layout.setSpacing(4)

        top_overlay_row = QtWidgets.QHBoxLayout()
        top_overlay_row.setSpacing(0)
        self.select_checkbox = QtWidgets.QCheckBox()
        self.select_checkbox.setToolTip("Select device for batch actions")
        self.select_checkbox.setStyleSheet("spacing:0px;")
        self.status_badge = QtWidgets.QLabel("Connected")
        self.status_badge.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
        self.status_badge.setStyleSheet(
            "background:#16a34a; border:1px solid #1f5b3f; border-radius:6px;"
        )
        self.status_badge.setFixedSize(12, 12)
        self.status_badge.setText("")
        self.status_badge.setToolTip("Connected")
        top_overlay_row.addStretch(1)
        top_overlay_row.addWidget(self.status_badge, 0, QtCore.Qt.AlignmentFlag.AlignTop)
        overlay_layout.addLayout(top_overlay_row)

        overlay_layout.addStretch(1)

        footer_row = QtWidgets.QHBoxLayout()
        footer_row.setSpacing(4)
        self.size_label = QtWidgets.QLabel("-")
        self.size_label.setObjectName("subtitleLabel")
        self.size_label.setStyleSheet(
            "font-size:8pt; color:#dbeafe; background:rgba(7, 14, 25, 120); border:1px solid rgba(51, 73, 104, 110); border-radius:8px; padding:2px 6px;"
        )
        self.workflow_state_label = QtWidgets.QLabel("Idle")
        self.workflow_state_label.setObjectName("subtitleLabel")
        self.workflow_state_label.setStyleSheet(
            "font-size:8pt; color:#6ee7b7; background:#0f1b15; border:1px solid #1f4a34; border-radius:8px; padding:2px 6px;"
        )
        footer_row.addWidget(self.size_label, 0)
        footer_row.addStretch(1)
        footer_row.addWidget(self.workflow_state_label, 0)
        overlay_layout.addLayout(footer_row)

        image_layout.addWidget(self.overlay_container, 0, 0, QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignTop)
        self.checkbox_overlay = QtWidgets.QFrame()
        self.checkbox_overlay.setStyleSheet("background:rgba(7, 14, 25, 90); border:1px solid rgba(51, 73, 104, 90); border-radius:8px;")
        checkbox_layout = QtWidgets.QHBoxLayout(self.checkbox_overlay)
        checkbox_layout.setContentsMargins(4, 4, 4, 4)
        checkbox_layout.setSpacing(0)
        checkbox_layout.addWidget(self.select_checkbox, 0, QtCore.Qt.AlignmentFlag.AlignCenter)
        self.checkbox_overlay.setFixedSize(22, 22)
        image_layout.addWidget(self.checkbox_overlay, 0, 0, QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignTop)
        self.image_card.setFixedSize(placeholder_size)
        layout.addWidget(self.image_card, 0, QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignTop)
        self.image_label.double_clicked.connect(lambda: self.realtime_requested.emit(self))
        self.select_checkbox.toggled.connect(self.set_selected)
        self._apply_overlay_metrics(placeholder_size)

    def _load_uiautomator2(self):
        try:
            return importlib.import_module("uiautomator2")
        except ImportError as exc:
            raise RuntimeError("uiautomator2 is not installed. Run pip install -r requirements.txt") from exc

    def _connect_device(self):
        uiautomator2 = self._load_uiautomator2()
        self._device = uiautomator2.connect(self.serial)
        return self._device

    def _ensure_connected(self):
        return self._device if self._device is not None else self._connect_device()

    def _host_device_is_ready(self) -> tuple[bool, str]:
        if not self.serial:
            return False, "Device serial is missing"
        try:
            result = subprocess.run(
                ["adb", "-s", self.serial, "get-state"],
                capture_output=True,
                text=True,
                timeout=self.ADB_STATE_TIMEOUT_SECONDS,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return False, f"ADB unavailable: {exc}"
        state = str(result.stdout or result.stderr or "").strip().lower()
        if result.returncode == 0 and state == "device":
            return True, ""
        if not state:
            state = f"adb get-state failed ({result.returncode})"
        return False, state

    def _capture_to_bytes(self, capture) -> bytes:
        if isinstance(capture, (bytes, bytearray)):
            return bytes(capture)
        elif isinstance(capture, str):
            return Path(capture).read_bytes()
        elif hasattr(capture, "save"):
            buffer = io.BytesIO()
            capture.save(buffer, format="PNG")
            return buffer.getvalue()
        else:
            raise RuntimeError("Unsupported screenshot format returned by device")

    def _decode_pixmap(self, data: bytes) -> QtGui.QPixmap:
        pixmap = QtGui.QPixmap()
        if not pixmap.loadFromData(data):
            raise RuntimeError("Failed to decode screenshot data")
        return pixmap

    def _prepare_frame_bytes(self, data: bytes) -> tuple[bytes, QtCore.QSize | None, QtCore.QSize | None]:
        image = QtGui.QImage.fromData(data)
        if image.isNull():
            return data, None, None
        source_size = image.size()
        if self._resolution_scale < 0.999:
            target_size = QtCore.QSize(
                max(int(round(source_size.width() * self._resolution_scale)), 1),
                max(int(round(source_size.height() * self._resolution_scale)), 1),
            )
            image = image.scaled(
                target_size,
                QtCore.Qt.AspectRatioMode.KeepAspectRatio,
                QtCore.Qt.TransformationMode.SmoothTransformation,
            )
            buffer = QtCore.QBuffer()
            buffer.open(QtCore.QIODevice.OpenModeFlag.WriteOnly)
            image.save(buffer, "PNG")
            data = bytes(buffer.data())
        return data, source_size, image.size()

    def capture_frame_payload(self) -> dict[str, Any]:
        now = time.monotonic()
        if now < self._next_refresh_allowed_at:
            remaining = max(self._next_refresh_allowed_at - now, 0.0)
            return {
                "ok": False,
                "error": f"Device reconnect cooldown ({remaining:.1f}s remaining)",
                "cooldown": True,
            }
        try:
            ready, reason = self._host_device_is_ready()
            if not ready:
                raise RuntimeError(reason or "Device is offline")
            device = self._ensure_connected()
            capture = device.screenshot()
            data = self._capture_to_bytes(capture)
            prepared_data, source_size, display_size = self._prepare_frame_bytes(data)
            self._next_refresh_allowed_at = 0.0
            return {
                "ok": True,
                "data": prepared_data,
                "source_size": source_size,
                "display_size": display_size,
                "resolution_scale": self._resolution_scale,
            }
        except Exception as exc:
            self._device = None
            self._next_refresh_allowed_at = time.monotonic() + self.OFFLINE_RETRY_COOLDOWN_SECONDS
            return {"ok": False, "error": str(exc)}

    def apply_frame_payload(self, payload: dict[str, Any]) -> None:
        self._refresh_pending = False
        if bool(payload.get("ok")):
            pixmap = self._decode_pixmap(bytes(payload.get("data") or b""))
            self._last_pixmap = pixmap
            self._failure_streak = 0
            self._render_pixmap()
            display_size = payload.get("display_size")
            source_size = payload.get("source_size")
            if isinstance(display_size, QtCore.QSize):
                self.size_label.setText(f"{display_size.width()} x {display_size.height()}")
                if isinstance(source_size, QtCore.QSize):
                    self.size_label.setToolTip(
                        f"Original: {source_size.width()} x {source_size.height()}"
                    )
                else:
                    self.size_label.setToolTip("")
            elif isinstance(source_size, QtCore.QSize):
                self.size_label.setText(f"{source_size.width()} x {source_size.height()}")
                self.size_label.setToolTip("")
            else:
                self.size_label.setText(f"{pixmap.width()} x {pixmap.height()}")
                self.size_label.setToolTip("")
            self._set_status_badge("connected")
            self.frame_updated.emit(pixmap)
            return
        self._device = None
        self._failure_streak += 1
        self.size_label.setText("-")
        self.size_label.setToolTip("")
        self._set_status_badge("reconnecting")

    def refresh_frame(self) -> None:
        self.apply_frame_payload(self.capture_frame_payload())

    def force_reconnect(self) -> None:
        self._device = None
        self._failure_streak = 0
        self._next_refresh_allowed_at = 0.0
        self.size_label.setText("-")
        self.size_label.setToolTip("")
        self._set_status_badge("reconnecting")

    def set_min_brightness(self) -> tuple[bool, str]:
        try:
            device = self._ensure_connected()
            device.shell("settings put system screen_brightness_mode 0")
            device.shell("settings put system screen_brightness 1")
            try:
                device.shell("cmd display brightness 0.0")
            except Exception:
                pass
            return True, "Brightness set to minimum"
        except Exception as exc:
            self._device = None
            return False, str(exc)

    def set_max_brightness(self) -> tuple[bool, str]:
        try:
            device = self._ensure_connected()
            device.shell("settings put system screen_brightness_mode 0")
            device.shell("settings put system screen_brightness 255")
            try:
                device.shell("cmd display brightness 1.0")
            except Exception:
                pass
            return True, "Brightness set to maximum"
        except Exception as exc:
            self._device = None
            return False, str(exc)

    def _press_device_key(
        self,
        *,
        label: str,
        key_names: list[str],
        fallback_keycodes: list[int],
    ) -> tuple[bool, str]:
        try:
            device = self._ensure_connected()
            press_fn = getattr(device, "press", None)
            if callable(press_fn):
                for key_name in key_names:
                    try:
                        press_fn(key_name)
                        return True, f"Pressed {label}"
                    except Exception:
                        continue
            for keycode in fallback_keycodes:
                try:
                    device.shell(f"input keyevent {keycode}")
                    return True, f"Pressed {label}"
                except Exception:
                    continue
            raise RuntimeError(f"Unable to send {label} key event")
        except Exception as exc:
            self._device = None
            return False, str(exc)

    def press_home(self) -> tuple[bool, str]:
        return self._press_device_key(label="Home", key_names=["home"], fallback_keycodes=[3])

    def press_back(self) -> tuple[bool, str]:
        return self._press_device_key(label="Back", key_names=["back"], fallback_keycodes=[4])

    def press_recent_apps(self) -> tuple[bool, str]:
        return self._press_device_key(
            label="Recent Apps",
            key_names=["recent", "recent_apps", "app_switch"],
            fallback_keycodes=[187],
        )

    def _target_screen_size(self) -> QtCore.QSize:
        target_height = max(int(self.BASE_IMAGE_SIZE.height() * self._zoom_factor), 1)
        if self._last_pixmap is not None and not self._last_pixmap.isNull() and self._last_pixmap.height() > 0:
            target_width = max(int(round(target_height * (self._last_pixmap.width() / self._last_pixmap.height()))), 1)
        else:
            target_width = max(int(round(target_height * self.DEFAULT_SCREEN_RATIO)), 1)
        return QtCore.QSize(target_width, target_height)

    def _preferred_tile_width(self) -> int:
        return self._target_screen_size().width() + self.TILE_SIDE_MARGIN

    def _workflow_style_for_state(self, normalized: str) -> str:
        style_map = {
            "idle": "color:#6ee7b7; background:rgba(15, 27, 21, 160); border:1px solid rgba(31, 74, 52, 170);",
            "queued": "color:#c4b5fd; background:rgba(22, 18, 39, 160); border:1px solid rgba(76, 29, 149, 170);",
            "running": "color:#7dd3fc; background:rgba(14, 26, 43, 160); border:1px solid rgba(29, 78, 216, 170);",
            "success": "color:#86efac; background:rgba(16, 36, 23, 160); border:1px solid rgba(22, 101, 52, 170);",
            "failed": "color:#fca5a5; background:rgba(42, 18, 21, 160); border:1px solid rgba(153, 27, 27, 170);",
            "stopped": "color:#fcd34d; background:rgba(43, 33, 16, 160); border:1px solid rgba(146, 64, 14, 170);",
        }
        return style_map.get(normalized, style_map["idle"])

    def _apply_overlay_metrics(self, screen_size: QtCore.QSize | None = None) -> None:
        target = screen_size or self.image_label.size() or self._target_screen_size()
        width = max(target.width(), 120)
        scale = max(0.78, min(width / 220.0, 1.45))
        margin = max(4, int(round(6 * scale)))
        spacing = max(2, int(round(4 * scale)))
        label_pt = max(7.0, round(7.8 * scale, 1))
        chip_pt = max(6.8, round(7.4 * scale, 1))
        dot_size = max(9, int(round(12 * scale)))
        checkbox_size = max(10, int(round(12 * scale)))
        checkbox_box = max(18, int(round(22 * scale)))
        radius = max(5, int(round(8 * scale)))
        hpad = max(4, int(round(6 * scale)))
        vpad = max(1, int(round(2 * scale)))

        overlay_layout = self.overlay_container.layout()
        if isinstance(overlay_layout, QtWidgets.QVBoxLayout):
            overlay_layout.setContentsMargins(margin, margin, margin, margin)
            overlay_layout.setSpacing(spacing)
        self.select_checkbox.setStyleSheet(
            "spacing:0px;"
            f" QCheckBox::indicator {{ width:{checkbox_size}px; height:{checkbox_size}px; }}"
        )
        self.checkbox_overlay.setFixedSize(checkbox_box, checkbox_box)
        self.checkbox_overlay.setStyleSheet(
            f"background:rgba(7, 14, 25, 82); border:1px solid rgba(51, 73, 104, 80); border-radius:{max(6, checkbox_box // 3)}px;"
        )
        checkbox_layout = self.checkbox_overlay.layout()
        if isinstance(checkbox_layout, QtWidgets.QHBoxLayout):
            inset = max(2, int(round(4 * scale)))
            checkbox_layout.setContentsMargins(inset, inset, inset, inset)
        self.status_badge.setFixedSize(dot_size, dot_size)
        self.status_badge.setStyleSheet(
            f"{self._status_badge_style()} border-radius:{max(4, dot_size // 2)}px;"
        )
        self.size_label.setStyleSheet(
            f"font-size:{label_pt}pt; color:#dbeafe; background:rgba(7, 14, 25, 110); "
            f"border:1px solid rgba(51, 73, 104, 95); border-radius:{radius}px; padding:{vpad}px {hpad}px;"
        )
        self.workflow_state_label.setStyleSheet(
            f"font-size:{chip_pt}pt; border-radius:{radius}px; padding:{vpad}px {hpad}px; {self._workflow_style_for_state(self._workflow_state_key)}"
        )

    def is_selected(self) -> bool:
        return self._selected

    def set_selected(self, selected: bool) -> None:
        normalized = bool(selected)
        if self._selected == normalized:
            if self.select_checkbox.isChecked() != normalized:
                self.select_checkbox.blockSignals(True)
                self.select_checkbox.setChecked(normalized)
                self.select_checkbox.blockSignals(False)
            return
        self._selected = normalized
        if self.select_checkbox.isChecked() != normalized:
            self.select_checkbox.blockSignals(True)
            self.select_checkbox.setChecked(normalized)
            self.select_checkbox.blockSignals(False)
        self._apply_selection_style()
        self.selection_changed.emit(self, normalized)

    def _apply_selection_style(self) -> None:
        if self._selected:
            self.setStyleSheet(
                "QFrame#deviceTile { background:#0e1a2b; border:1px solid #2563eb; border-radius:12px; }"
            )
            return
        self.setStyleSheet("QFrame#deviceTile { background:transparent; border:none; }")

    def set_zoom_factor(self, zoom_factor: float) -> None:
        self._zoom_factor = max(0.5, min(float(zoom_factor or 1.0), 2.0))
        scaled_size = self._target_screen_size()
        self.image_label.setFixedSize(scaled_size)
        self.overlay_container.setFixedSize(scaled_size)
        self.image_card.setFixedSize(
            scaled_size.width() + self.IMAGE_FRAME_PADDING * 2,
            scaled_size.height() + self.IMAGE_FRAME_PADDING * 2,
        )
        self.setMinimumWidth(self._preferred_tile_width())
        self.setMaximumWidth(self._preferred_tile_width())
        self._apply_overlay_metrics(scaled_size)
        self._render_pixmap()

    def set_resolution_scale(self, resolution_scale: float) -> None:
        self._resolution_scale = max(0.25, min(float(resolution_scale or 1.0), 1.0))

    def set_workflow_state(self, state: str, message: str = "") -> None:
        normalized = str(state or "").strip().lower() or "idle"
        label_text = {
            "idle": "Idle",
            "queued": "Queued",
            "running": "Running",
            "success": "Done",
            "failed": "Failed",
            "stopped": "Stopped",
        }.get(normalized, normalized.title())
        self._workflow_state_key = normalized
        self._workflow_state_message = message or ""
        self.workflow_state_label.setText(label_text)
        self.workflow_state_label.setToolTip(self._workflow_state_message)
        self._apply_overlay_metrics()

    def current_pixmap(self) -> QtGui.QPixmap | None:
        return self._last_pixmap

    def save_current_frame(self, output_dir: Path | None = None) -> Path:
        if self._last_pixmap is None or self._last_pixmap.isNull():
            raise RuntimeError("No frame available to save yet")
        output_path = _build_frame_output_path(self.device_name, self.serial, output_dir=output_dir)
        self._last_pixmap.save(str(output_path), "PNG")
        return output_path

    def _render_pixmap(self) -> None:
        target_size = self._target_screen_size()
        if self._last_pixmap is None or self._last_pixmap.isNull():
            self.image_label.setText("Waiting for frame")
            self.image_label.setPixmap(QtGui.QPixmap())
            self.image_label.setFixedSize(target_size)
            self.overlay_container.setFixedSize(target_size)
            self.image_card.setFixedSize(target_size)
            self._apply_overlay_metrics(target_size)
            return
        scaled = self._last_pixmap.scaled(
            target_size,
            QtCore.Qt.AspectRatioMode.KeepAspectRatio,
            QtCore.Qt.TransformationMode.SmoothTransformation,
        )
        self.image_label.setPixmap(scaled)
        self.image_label.setText("")
        self.image_label.setFixedSize(scaled.size())
        self.overlay_container.setFixedSize(scaled.size())
        self.image_card.setFixedSize(scaled.size())
        self.setMinimumWidth(self._preferred_tile_width())
        self.setMaximumWidth(self._preferred_tile_width())
        self._apply_overlay_metrics(scaled.size())

    def _status_badge_style(self) -> str:
        normalized = (self._status_badge_state or "").casefold()
        if normalized == "connected":
            return "background:#16a34a; border:1px solid #1f5b3f;"
        if normalized == "reconnecting":
            return "background:#f59e0b; border:1px solid #92400e;"
        return "background:#64748b; border:1px solid #334864;"

    def _set_status_badge(self, status: str) -> None:
        normalized = (status or "").casefold()
        self._status_badge_state = normalized or "unknown"
        if normalized == "connected":
            tooltip = "Connected"
        elif normalized == "reconnecting":
            tooltip = "Refreshing"
        else:
            tooltip = status.title() if status else "Unknown"
        self.status_badge.setText("")
        self.status_badge.setToolTip(tooltip)
        self._apply_overlay_metrics()

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._render_pixmap()


class ScreenViewerWindow(QtWidgets.QMainWindow):
    VIEW_PRESETS = {
        "performance": {"refresh_interval_ms": 2000, "resolution_scale": 0.33},
        "balanced": {"refresh_interval_ms": 1000, "resolution_scale": 0.5},
        "quality": {"refresh_interval_ms": 500, "resolution_scale": 1.0},
    }
    SCRCPY_PRESETS = {
        "low_latency": {"max_size": 800, "max_fps": 30, "bit_rate": "4M"},
        "balanced": {"max_size": 1024, "max_fps": 30, "bit_rate": "8M"},
        "high_quality": {"max_size": 1280, "max_fps": 60, "bit_rate": "16M"},
    }
    WORKFLOW_RUN_MODES = (
        ("Sequential", 1),
        ("Parallel x2", 2),
        ("Parallel x4", 4),
        ("Parallel All", -1),
    )

    def __init__(
        self,
        *,
        devices: list[dict[str, Any]],
        workflows: list[dict[str, Any]] | None = None,
        workflow_service: Any | None = None,
        refresh_interval_ms: int = 1000,
        autostart: bool = True,
    ) -> None:
        super().__init__()
        self.devices = [device for device in devices if str(device.get("serial") or "").strip()]
        self.workflows = [dict(workflow) for workflow in (workflows or [])]
        self.workflow_service = workflow_service
        self.refresh_interval_ms = max(int(refresh_interval_ms or 1000), 250)
        self.autostart = autostart
        self._scrcpy_program = find_scrcpy_executable()
        self._tiles: list[DeviceScreenTile] = []
        self._detail_windows: dict[str, DeviceDetailViewerWindow] = {}
        self._zoom_factor = 1.0
        self._resolution_scale = 1.0
        self._scrcpy_max_size = 0
        self._scrcpy_max_fps = 0
        self._scrcpy_bit_rate = ""
        self._updating_view_preset = False
        self._updating_scrcpy_preset = False
        self._suspend_settings_save = True
        self._refresh_in_progress = False
        self._workflow_runner: WorkflowBatchRunner | None = None
        self._workflow_target_device_ids: set[int] = set()
        self._workflow_running_device_ids: set[int] = set()
        self._workflow_stop_requested = False
        self._pending_tiles: list[DeviceScreenTile] = []
        self._active_refresh_workers = 0
        self._refreshed_count = 0
        self._thread_pool = QtCore.QThreadPool(self)
        self._thread_pool.setMaxThreadCount(3)
        self._build_ui()
        self._init_timer()
        self._restore_screen_wall_settings()
        self._suspend_settings_save = False
        if self.autostart:
            QtCore.QTimer.singleShot(0, self.refresh_all)

    def _build_ui(self) -> None:
        self.setWindowTitle(f"Screen Wall - {len(self.devices)} devices")
        self.resize(1480, 920)

        root = QtWidgets.QWidget()
        self.setCentralWidget(root)
        layout = QtWidgets.QVBoxLayout(root)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        title = QtWidgets.QLabel("All Device Screens")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel(f"{len(self.devices)} devices / live screenshot polling in a separate process")
        subtitle.setObjectName("subtitleLabel")
        subtitle.setWordWrap(True)
        layout.addWidget(title)
        layout.addWidget(subtitle)

        toolbar_card = QtWidgets.QFrame()
        toolbar_card.setProperty("panel", True)
        toolbar_layout = QtWidgets.QVBoxLayout(toolbar_card)
        toolbar_layout.setContentsMargins(14, 12, 14, 12)
        toolbar_layout.setSpacing(10)

        controls = QtWidgets.QHBoxLayout()
        controls.setSpacing(10)
        preset_label = QtWidgets.QLabel("Preset")
        preset_label.setObjectName("subtitleLabel")
        self.view_preset_combo = QtWidgets.QComboBox()
        self.view_preset_combo.addItem("Custom", "custom")
        self.view_preset_combo.addItem("Performance", "performance")
        self.view_preset_combo.addItem("Balanced", "balanced")
        self.view_preset_combo.addItem("Quality", "quality")
        refresh_label = QtWidgets.QLabel("Refresh")
        refresh_label.setObjectName("subtitleLabel")
        self.refresh_rate_combo = QtWidgets.QComboBox()
        for label, value in (
            ("250 ms", 250),
            ("500 ms", 500),
            ("1 sec", 1000),
            ("2 sec", 2000),
            ("5 sec", 5000),
            ("10 sec", 10000),
        ):
            self.refresh_rate_combo.addItem(label, value)
        default_index = max(self.refresh_rate_combo.findData(self.refresh_interval_ms), 0)
        self.refresh_rate_combo.setCurrentIndex(default_index)
        self.pause_button = QtWidgets.QPushButton("Pause")
        self.reconnect_button = QtWidgets.QPushButton("Reconnect All")
        self.refresh_now_button = QtWidgets.QPushButton("Refresh Now")
        self.save_all_button = QtWidgets.QPushButton("Save All")
        self.locate_scrcpy_button = QtWidgets.QPushButton("Locate scrcpy")
        scrcpy_resolution_label = QtWidgets.QLabel("Preset")
        scrcpy_resolution_label.setObjectName("subtitleLabel")
        self.scrcpy_preset_combo = QtWidgets.QComboBox()
        self.scrcpy_preset_combo.addItem("Custom", "custom")
        self.scrcpy_preset_combo.addItem("Low Latency", "low_latency")
        self.scrcpy_preset_combo.addItem("Balanced", "balanced")
        self.scrcpy_preset_combo.addItem("High Quality", "high_quality")
        scrcpy_fps_label = QtWidgets.QLabel("FPS")
        scrcpy_fps_label.setObjectName("subtitleLabel")
        scrcpy_bitrate_label = QtWidgets.QLabel("Bitrate")
        scrcpy_bitrate_label.setObjectName("subtitleLabel")
        resolution_label = QtWidgets.QLabel("Resolution")
        resolution_label.setObjectName("subtitleLabel")
        zoom_label = QtWidgets.QLabel("Zoom")
        zoom_label.setObjectName("subtitleLabel")
        self.scrcpy_max_size_combo = QtWidgets.QComboBox()
        for label, value in (
            ("Original", 0),
            ("640 px", 640),
            ("800 px", 800),
            ("1024 px", 1024),
            ("1280 px", 1280),
        ):
            self.scrcpy_max_size_combo.addItem(label, value)
        self.scrcpy_max_size_combo.setCurrentIndex(max(self.scrcpy_max_size_combo.findData(0), 0))
        controls.addWidget(scrcpy_fps_label)
        self.scrcpy_max_fps_combo = QtWidgets.QComboBox()
        for label, value in (
            ("Default", 0),
            ("15 fps", 15),
            ("30 fps", 30),
            ("45 fps", 45),
            ("60 fps", 60),
        ):
            self.scrcpy_max_fps_combo.addItem(label, value)
        self.scrcpy_max_fps_combo.setCurrentIndex(max(self.scrcpy_max_fps_combo.findData(0), 0))
        controls.addWidget(scrcpy_bitrate_label)
        self.scrcpy_bit_rate_combo = QtWidgets.QComboBox()
        for label, value in (
            ("Default", ""),
            ("2M", "2M"),
            ("4M", "4M"),
            ("8M", "8M"),
            ("12M", "12M"),
            ("16M", "16M"),
        ):
            self.scrcpy_bit_rate_combo.addItem(label, value)
        self.scrcpy_bit_rate_combo.setCurrentIndex(max(self.scrcpy_bit_rate_combo.findData(""), 0))
        controls.addWidget(resolution_label)
        self.resolution_combo = QtWidgets.QComboBox()
        for label, value in (
            ("100%", 1.0),
            ("75%", 0.75),
            ("50%", 0.5),
            ("33%", 0.33),
        ):
            self.resolution_combo.addItem(label, value)
        self.resolution_combo.setCurrentIndex(max(self.resolution_combo.findData(1.0), 0))
        controls.addWidget(zoom_label)
        self.zoom_out_button = QtWidgets.QPushButton("-")
        self.zoom_in_button = QtWidgets.QPushButton("+")
        self.zoom_combo = QtWidgets.QComboBox()
        for label, value in (
            ("50%", 0.5),
            ("75%", 0.75),
            ("100%", 1.0),
            ("125%", 1.25),
            ("150%", 1.5),
            ("175%", 1.75),
            ("200%", 2.0),
        ):
            self.zoom_combo.addItem(label, value)
        self.zoom_combo.setCurrentIndex(max(self.zoom_combo.findData(1.0), 0))
        self.status_label = StatusVisualLabel("Status: idle")
        self.status_label.setObjectName("wallStatusIndicator")
        for button in (
            self.pause_button,
            self.reconnect_button,
            self.refresh_now_button,
            self.save_all_button,
            self.locate_scrcpy_button,
            self.zoom_out_button,
            self.zoom_in_button,
        ):
            button.setMinimumHeight(26)
            button.setStyleSheet("padding:5px 9px; font-size:8.5pt;")
        polling_group = QtWidgets.QFrame()
        polling_group.setProperty("panel", True)
        polling_group.setStyleSheet("QFrame { background:#0d1726; border:1px solid #1c2b40; border-radius:12px; }")
        polling_layout = QtWidgets.QVBoxLayout(polling_group)
        polling_layout.setContentsMargins(10, 8, 10, 8)
        polling_layout.setSpacing(6)
        polling_title = QtWidgets.QLabel("Screen Wall Polling")
        polling_title.setStyleSheet("font-size:8.5pt; font-weight:700; color:#dbe7ff;")
        polling_layout.addWidget(polling_title)
        polling_controls = QtWidgets.QHBoxLayout()
        polling_controls.setSpacing(6)
        polling_controls.addWidget(preset_label)
        polling_controls.addWidget(self.view_preset_combo)
        polling_controls.addWidget(refresh_label)
        polling_controls.addWidget(self.refresh_rate_combo)
        polling_controls.addWidget(resolution_label)
        polling_controls.addWidget(self.resolution_combo)
        polling_controls.addWidget(zoom_label)
        polling_controls.addWidget(self.zoom_out_button)
        polling_controls.addWidget(self.zoom_combo)
        polling_controls.addWidget(self.zoom_in_button)
        polling_controls.addWidget(self.pause_button)
        polling_controls.addWidget(self.refresh_now_button)
        polling_controls.addWidget(self.save_all_button)
        polling_controls.addStretch(1)
        polling_layout.addLayout(polling_controls)

        scrcpy_group = QtWidgets.QFrame()
        scrcpy_group.setProperty("panel", True)
        scrcpy_group.setStyleSheet("QFrame { background:#0d1726; border:1px solid #1c2b40; border-radius:12px; }")
        scrcpy_layout = QtWidgets.QVBoxLayout(scrcpy_group)
        scrcpy_layout.setContentsMargins(10, 8, 10, 8)
        scrcpy_layout.setSpacing(6)
        scrcpy_title = QtWidgets.QLabel("scrcpy Realtime")
        scrcpy_title.setStyleSheet("font-size:8.5pt; font-weight:700; color:#dbe7ff;")
        scrcpy_layout.addWidget(scrcpy_title)
        scrcpy_controls = QtWidgets.QHBoxLayout()
        scrcpy_controls.setSpacing(6)
        scrcpy_controls.addWidget(scrcpy_resolution_label)
        scrcpy_controls.addWidget(self.scrcpy_preset_combo)
        scrcpy_controls.addWidget(self.scrcpy_max_size_combo)
        scrcpy_controls.addWidget(scrcpy_fps_label)
        scrcpy_controls.addWidget(self.scrcpy_max_fps_combo)
        scrcpy_controls.addWidget(scrcpy_bitrate_label)
        scrcpy_controls.addWidget(self.scrcpy_bit_rate_combo)
        scrcpy_controls.addWidget(self.locate_scrcpy_button)
        scrcpy_controls.addWidget(self.reconnect_button)
        scrcpy_controls.addStretch(1)
        scrcpy_layout.addLayout(scrcpy_controls)

        controls.addWidget(polling_group, 1)
        controls.addWidget(scrcpy_group, 1)
        controls.addStretch(1)
        toolbar_layout.addLayout(controls)

        summary_row = QtWidgets.QHBoxLayout()
        summary_row.setSpacing(12)
        self.device_count_chip = QtWidgets.QLabel(f"{len(self.devices)} devices")
        self.device_count_chip.setStyleSheet(
            "background:#101c2d; color:#dbe7ff; border:1px solid #26405d; border-radius:12px; padding:6px 10px; font-weight:600;"
        )
        self.selection_count_chip = QtWidgets.QLabel("0 selected")
        self.selection_count_chip.setStyleSheet(
            "background:#0f2342; color:#bfdbfe; border:1px solid #1d4ed8; border-radius:12px; padding:6px 10px; font-weight:600;"
        )
        summary_row.addWidget(self.device_count_chip, 0)
        summary_row.addWidget(self.selection_count_chip, 0)
        summary_row.addWidget(self.status_label, 0, QtCore.Qt.AlignmentFlag.AlignVCenter)
        summary_row.addStretch(1)
        toolbar_layout.addLayout(summary_row)

        workflow_row = QtWidgets.QHBoxLayout()
        workflow_row.setSpacing(6)
        workflow_group = QtWidgets.QFrame()
        workflow_group.setProperty("panel", True)
        workflow_group.setStyleSheet("QFrame { background:#0d1726; border:1px solid #1c2b40; border-radius:12px; }")
        workflow_layout = QtWidgets.QVBoxLayout(workflow_group)
        workflow_layout.setContentsMargins(10, 8, 10, 8)
        workflow_layout.setSpacing(6)
        workflow_title = QtWidgets.QLabel("Workflow Actions")
        workflow_title.setStyleSheet("font-size:8.5pt; font-weight:700; color:#dbe7ff;")
        workflow_layout.addWidget(workflow_title)
        workflow_controls = QtWidgets.QHBoxLayout()
        workflow_controls.setSpacing(6)
        workflow_controls.addWidget(QtWidgets.QLabel("Workflow"))
        self.workflow_combo = QtWidgets.QComboBox()
        self.workflow_combo.setMinimumWidth(280)
        workflow_controls.addWidget(self.workflow_combo, 1)
        workflow_controls.addWidget(QtWidgets.QLabel("Mode"))
        self.workflow_mode_combo = QtWidgets.QComboBox()
        for label, value in self.WORKFLOW_RUN_MODES:
            self.workflow_mode_combo.addItem(label, value)
        self.workflow_mode_combo.setMinimumWidth(120)
        workflow_controls.addWidget(self.workflow_mode_combo, 0)
        self.reload_workflows_button = QtWidgets.QPushButton("Reload")
        self.run_selected_workflow_button = QtWidgets.QPushButton("Run Selected")
        self.run_all_workflow_button = QtWidgets.QPushButton("Run All")
        self.stop_workflow_button = QtWidgets.QPushButton("Stop Workflow")
        for button in (
            self.reload_workflows_button,
            self.run_selected_workflow_button,
            self.run_all_workflow_button,
            self.stop_workflow_button,
        ):
            button.setMinimumHeight(24)
            button.setStyleSheet("padding:4px 8px; font-size:8.5pt;")
            workflow_controls.addWidget(button)
        workflow_controls.addStretch(1)
        workflow_layout.addLayout(workflow_controls)
        workflow_row.addWidget(workflow_group, 1)
        toolbar_layout.addLayout(workflow_row)

        batch_row = QtWidgets.QHBoxLayout()
        batch_row.setSpacing(6)
        self.select_all_button = QtWidgets.QPushButton("Select All")
        self.clear_selection_button = QtWidgets.QPushButton("Clear")
        self.refresh_selected_button = QtWidgets.QPushButton("Refresh Selected")
        self.reconnect_selected_button = QtWidgets.QPushButton("Reconnect Selected")
        self.save_selected_button = QtWidgets.QPushButton("Save Selected")
        self.realtime_selected_button = QtWidgets.QPushButton("Realtime Selected")
        self.min_brightness_button = QtWidgets.QPushButton("Min Brightness")
        self.max_brightness_button = QtWidgets.QPushButton("Max Brightness")
        self.home_selected_button = QtWidgets.QPushButton("Home")
        self.back_selected_button = QtWidgets.QPushButton("Back")
        self.recent_apps_selected_button = QtWidgets.QPushButton("Recent Apps")
        for button in (
            self.select_all_button,
            self.clear_selection_button,
            self.refresh_selected_button,
            self.reconnect_selected_button,
            self.save_selected_button,
            self.realtime_selected_button,
            self.min_brightness_button,
            self.max_brightness_button,
            self.home_selected_button,
            self.back_selected_button,
            self.recent_apps_selected_button,
        ):
            button.setMinimumHeight(24)
            button.setStyleSheet("padding:4px 8px; font-size:8.5pt;")
            batch_row.addWidget(button)
        batch_row.addStretch(1)
        toolbar_layout.addLayout(batch_row)
        layout.addWidget(toolbar_card, 0)

        self.scroll_area = QtWidgets.QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        layout.addWidget(self.scroll_area, 1)

        self.scroll_content = QtWidgets.QWidget()
        self.grid_layout = QtWidgets.QGridLayout(self.scroll_content)
        self.grid_layout.setContentsMargins(8, 0, 12, 0)
        self.grid_layout.setHorizontalSpacing(10)
        self.grid_layout.setVerticalSpacing(8)
        self.grid_layout.setAlignment(QtCore.Qt.AlignmentFlag.AlignTop | QtCore.Qt.AlignmentFlag.AlignLeft)
        self.scroll_area.setWidget(self.scroll_content)

        self._rebuild_tiles()

        self.refresh_rate_combo.currentIndexChanged.connect(self._update_timer_interval)
        self.view_preset_combo.currentIndexChanged.connect(self._apply_view_preset)
        self.pause_button.clicked.connect(self._toggle_pause)
        self.reconnect_button.clicked.connect(self.force_reconnect_all)
        self.refresh_now_button.clicked.connect(self.refresh_all)
        self.save_all_button.clicked.connect(self.save_all_frames)
        self.locate_scrcpy_button.clicked.connect(self._select_scrcpy_program)
        self.scrcpy_preset_combo.currentIndexChanged.connect(self._apply_scrcpy_preset)
        self.scrcpy_max_size_combo.currentIndexChanged.connect(self._update_scrcpy_max_size)
        self.scrcpy_max_fps_combo.currentIndexChanged.connect(self._update_scrcpy_max_fps)
        self.scrcpy_bit_rate_combo.currentIndexChanged.connect(self._update_scrcpy_bit_rate)
        self.reload_workflows_button.clicked.connect(self._reload_workflows)
        self.run_selected_workflow_button.clicked.connect(self.run_selected_workflow)
        self.run_all_workflow_button.clicked.connect(self.run_all_workflow)
        self.stop_workflow_button.clicked.connect(self.stop_workflow_run)
        self.workflow_combo.currentIndexChanged.connect(self._update_workflow_actions)
        self.workflow_mode_combo.currentIndexChanged.connect(self._save_screen_wall_settings)
        self.select_all_button.clicked.connect(self._select_all_tiles)
        self.clear_selection_button.clicked.connect(self._clear_selected_tiles)
        self.refresh_selected_button.clicked.connect(self.refresh_selected_tiles)
        self.reconnect_selected_button.clicked.connect(self.force_reconnect_selected_tiles)
        self.save_selected_button.clicked.connect(self.save_selected_frames)
        self.realtime_selected_button.clicked.connect(self.open_realtime_selected_viewers)
        self.min_brightness_button.clicked.connect(self.set_min_brightness_selected_tiles)
        self.max_brightness_button.clicked.connect(self.set_max_brightness_selected_tiles)
        self.home_selected_button.clicked.connect(self.press_home_selected_tiles)
        self.back_selected_button.clicked.connect(self.press_back_selected_tiles)
        self.recent_apps_selected_button.clicked.connect(self.press_recent_apps_selected_tiles)
        self.resolution_combo.currentIndexChanged.connect(self._update_resolution_scale)
        self.zoom_combo.currentIndexChanged.connect(self._update_zoom_factor)
        self.zoom_out_button.clicked.connect(self._step_zoom_out)
        self.zoom_in_button.clicked.connect(self._step_zoom_in)
        self._sync_view_preset_from_controls()
        self._sync_scrcpy_preset_from_controls()
        self._populate_workflows()
        self._update_scrcpy_controls()
        self._update_selection_actions()

    def _init_timer(self) -> None:
        self.timer = QtCore.QTimer(self)
        self.timer.setInterval(self.refresh_interval_ms)
        self.timer.timeout.connect(self.refresh_all)
        if self.autostart:
            self.timer.start()

    def _column_count(self) -> int:
        count = len(self.devices)
        if count <= 1:
            return 1
        if count <= 4:
            return 2
        if count <= 9:
            return 3
        return 4

    def _rebuild_tiles(self) -> None:
        while self.grid_layout.count():
            item = self.grid_layout.takeAt(0)
            widget = item.widget()
            if widget is not None:
                widget.deleteLater()
        self._tiles.clear()

        columns = self._column_count()
        for index, device in enumerate(self.devices):
            tile = DeviceScreenTile(device, self.scroll_content)
            tile.set_zoom_factor(self._zoom_factor)
            tile.set_resolution_scale(self._resolution_scale)
            tile.save_requested.connect(self._save_tile_frame)
            tile.realtime_requested.connect(self._open_realtime_viewer)
            tile.selection_changed.connect(self._handle_tile_selection_changed)
            tile.frame_updated.connect(lambda pixmap, tile=tile: self._sync_detail_window(tile, pixmap))
            row = index // columns
            column = index % columns
            self.grid_layout.addWidget(tile, row, column)
            self._tiles.append(tile)

        for column in range(columns):
            self.grid_layout.setColumnStretch(column, 0)
        self.grid_layout.setRowStretch((len(self.devices) // max(columns, 1)) + 1, 1)
        QtCore.QTimer.singleShot(0, self._apply_tile_widths)
        self._update_selection_actions()

    def refresh_all(self) -> None:
        if not self._tiles:
            self.status_label.setText("Status: no devices")
            return
        self._refresh_tiles(self._tiles, label="refreshing")

    def _dispatch_refresh_workers(self) -> None:
        if not self._pending_tiles and self._active_refresh_workers == 0:
            self._refresh_in_progress = False
            self.status_label.setText(f"Status: refreshed {self._refreshed_count} devices")
            return
        max_parallel = max(1, min(self._thread_pool.maxThreadCount(), len(self._tiles)))
        while self._pending_tiles and self._active_refresh_workers < max_parallel:
            tile = self._pending_tiles.pop(0)
            tile._refresh_pending = True
            worker = FrameRefreshWorker(tile, tile.capture_frame_payload)
            worker.signals.finished.connect(self._handle_refresh_result)
            self._active_refresh_workers += 1
            self._thread_pool.start(worker)

    def _selected_tiles(self) -> list[DeviceScreenTile]:
        return [tile for tile in self._tiles if tile.is_selected()]

    def _handle_tile_selection_changed(self, *_args) -> None:
        self._update_selection_actions()

    def _update_selection_actions(self) -> None:
        selected_count = len(self._selected_tiles())
        self.selection_count_chip.setText(f"{selected_count} selected")
        has_selection = selected_count > 0
        for button in (
            self.clear_selection_button,
            self.refresh_selected_button,
            self.reconnect_selected_button,
            self.save_selected_button,
            self.realtime_selected_button,
            self.min_brightness_button,
            self.max_brightness_button,
            self.home_selected_button,
            self.back_selected_button,
            self.recent_apps_selected_button,
        ):
            button.setEnabled(has_selection)
        self.select_all_button.setEnabled(bool(self._tiles) and selected_count < len(self._tiles))
        self._update_workflow_actions()

    def _selected_device_records(self) -> list[dict[str, Any]]:
        return [dict(tile.device_record) for tile in self._selected_tiles()]

    def _tile_for_device_id(self, device_id: int) -> DeviceScreenTile | None:
        for tile in self._tiles:
            if int(tile.device_record.get("id") or 0) == int(device_id or 0):
                return tile
        return None

    def _populate_workflows(self) -> None:
        self.workflow_combo.clear()
        self.workflow_combo.addItem("Select workflow...", None)
        for workflow in sorted(self.workflows, key=lambda item: (str(item.get("name") or "").lower(), int(item.get("id") or 0))):
            self.workflow_combo.addItem(str(workflow.get("name") or f"Workflow {workflow.get('id')}"), int(workflow["id"]))
        self._restore_workflow_selection()
        self._update_workflow_actions()

    def _restore_workflow_selection(self) -> None:
        saved_workflow_id = int(_settings().value("wall/workflow_id", 0) or 0)
        if saved_workflow_id:
            index = self.workflow_combo.findData(saved_workflow_id)
            if index >= 0:
                self.workflow_combo.setCurrentIndex(index)
        saved_mode = int(_settings().value("wall/workflow_parallelism", 1) or 1)
        mode_index = self.workflow_mode_combo.findData(saved_mode)
        if mode_index >= 0:
            self.workflow_mode_combo.setCurrentIndex(mode_index)

    def _reload_workflows(self) -> None:
        if self.workflow_service is None:
            self.status_label.setText("Status: workflow service unavailable")
            return
        try:
            self.workflows = self.workflow_service.list_workflows()
        except Exception as exc:
            self.status_label.setText(f"Status: failed to reload workflows ({exc})")
            return
        self._populate_workflows()
        self.status_label.setText(f"Status: loaded {len(self.workflows)} workflows")

    def _selected_workflow_id(self) -> int | None:
        value = self.workflow_combo.currentData()
        return int(value) if isinstance(value, int) and value > 0 else None

    def _selected_workflow_parallelism(self) -> int:
        value = self.workflow_mode_combo.currentData()
        if isinstance(value, int):
            if value < 0:
                return max(len(self.devices), 1)
            return max(value, 1)
        return 1

    def _selected_workflow_mode_label(self) -> str:
        return self.workflow_mode_combo.currentText().strip() or "Sequential"

    def _update_workflow_actions(self) -> None:
        workflow_id = self._selected_workflow_id()
        workflow_ready = workflow_id is not None and self.workflow_service is not None and self._workflow_runner is None
        self.reload_workflows_button.setEnabled(self.workflow_service is not None and self._workflow_runner is None)
        self.run_all_workflow_button.setEnabled(workflow_ready and bool(self.devices))
        self.run_selected_workflow_button.setEnabled(workflow_ready and bool(self._selected_tiles()))
        self.workflow_mode_combo.setEnabled(self._workflow_runner is None)
        self.stop_workflow_button.setEnabled(self._workflow_runner is not None)

    def _start_workflow_runner(self, workflow_id: int, device_records: list[dict[str, Any]], label: str) -> None:
        if self._workflow_runner is not None:
            self.status_label.setText("Status: workflow run already in progress")
            return
        workflow_name = self.workflow_combo.currentText().strip()
        parallelism = min(self._selected_workflow_parallelism(), max(len(device_records), 1))
        self._workflow_target_device_ids = {int(record.get("id") or 0) for record in device_records if int(record.get("id") or 0) > 0}
        self._workflow_running_device_ids.clear()
        self._workflow_stop_requested = False
        for device_record in device_records:
            tile = self._tile_for_device_id(int(device_record.get("id") or 0))
            if tile is not None:
                tile.set_workflow_state("queued", workflow_name)
        self._workflow_runner = WorkflowBatchRunner(
            self.workflow_service,
            workflow_id,
            device_records,
            max_parallel=parallelism,
        )
        self._workflow_runner.progress.connect(self._on_workflow_progress)
        self._workflow_runner.result_ready.connect(self._on_workflow_result)
        self._workflow_runner.finished.connect(self._on_workflow_runner_finished)
        self.status_label.setText(label)
        self._update_workflow_actions()
        self._workflow_runner.start()

    def stop_workflow_run(self) -> None:
        if self._workflow_runner is None:
            self.status_label.setText("Status: no workflow is running")
            return
        if self._workflow_stop_requested:
            self.status_label.setText("Status: stop already requested")
            return
        self._workflow_stop_requested = True
        self._workflow_runner.request_stop()
        if self.workflow_service is not None and self._workflow_running_device_ids:
            self.workflow_service.request_stop_for_devices(
                list(self._workflow_running_device_ids),
                reason="Stopped from Screen Wall",
            )
        for device_id in self._workflow_target_device_ids:
            if device_id in self._workflow_running_device_ids:
                continue
            tile = self._tile_for_device_id(device_id)
            if tile is not None:
                tile.set_workflow_state("stopped", "Stopped before execution")
        self.status_label.setText("Status: stopping workflow after current device")
        self._update_workflow_actions()

    def run_selected_workflow(self) -> None:
        workflow_id = self._selected_workflow_id()
        if workflow_id is None:
            self.status_label.setText("Status: select a workflow first")
            return
        device_records = self._selected_device_records()
        if not device_records:
            self.status_label.setText("Status: no selected devices")
            return
        self._start_workflow_runner(
            workflow_id,
            device_records,
            f"Status: running workflow on {len(device_records)} selected devices ({self._selected_workflow_mode_label()})",
        )

    def run_all_workflow(self) -> None:
        workflow_id = self._selected_workflow_id()
        if workflow_id is None:
            self.status_label.setText("Status: select a workflow first")
            return
        if not self.devices:
            self.status_label.setText("Status: no devices available")
            return
        self._start_workflow_runner(
            workflow_id,
            self.devices,
            f"Status: running workflow on all {len(self.devices)} devices ({self._selected_workflow_mode_label()})",
        )

    def _on_workflow_progress(self, payload: dict[str, Any]) -> None:
        current = int(payload.get("current") or 0)
        total = int(payload.get("total") or 0)
        device_name = str(payload.get("device_name") or "device")
        device_id = int(payload.get("device_id") or 0)
        tile = self._tile_for_device_id(device_id)
        if str(payload.get("phase") or "") == "started":
            self._workflow_running_device_ids.add(device_id)
            if tile is not None:
                tile.set_workflow_state("running", device_name)
            self.status_label.setText(f"Status: workflow running {current}/{total} on {device_name}")
            return
        result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
        if tile is not None:
            tile.set_workflow_state(
                "stopped" if result.get("stopped") else ("success" if result.get("success") else "failed"),
                str(result.get("message") or ""),
            )
        self._workflow_running_device_ids.discard(device_id)
        self.status_label.setText(f"Status: workflow completed {current}/{total} on {device_name}")

    def _on_workflow_result(self, result: dict[str, Any]) -> None:
        for item in result.get("results") or []:
            if not isinstance(item, dict):
                continue
            tile = self._tile_for_device_id(int(item.get("device_id") or 0))
            run_result = item.get("result") if isinstance(item.get("result"), dict) else {}
            if tile is not None:
                tile.set_workflow_state(
                    "stopped" if run_result.get("stopped") else ("success" if run_result.get("success") else "failed"),
                    str(run_result.get("message") or ""),
                )
        success_count = int(result.get("success_count") or 0)
        total = int(result.get("total") or 0)
        stopped_count = int(result.get("stopped_count") or 0)
        if bool(result.get("stopped")):
            self.status_label.setText(
                f"Status: workflow stopped ({success_count} done, {stopped_count} stopped, {total} total)"
            )
            return
        self.status_label.setText(f"Status: workflow finished {success_count}/{total} devices")

    def _on_workflow_runner_finished(self) -> None:
        self._workflow_runner = None
        self._workflow_target_device_ids.clear()
        self._workflow_running_device_ids.clear()
        self._workflow_stop_requested = False
        self._update_workflow_actions()

    def _select_all_tiles(self) -> None:
        for tile in self._tiles:
            tile.set_selected(True)
        self.status_label.setText(f"Status: selected {len(self._tiles)} devices")

    def _clear_selected_tiles(self) -> None:
        for tile in self._selected_tiles():
            tile.set_selected(False)
        self.status_label.setText("Status: selection cleared")

    def _refresh_tiles(self, tiles: list[DeviceScreenTile], *, label: str) -> None:
        target_tiles = [tile for tile in tiles if not tile._refresh_pending]
        if not target_tiles:
            self.status_label.setText("Status: no devices ready for refresh")
            return
        if self._refresh_in_progress:
            self.status_label.setText("Status: refresh already in progress")
            return
        self._refresh_in_progress = True
        self._pending_tiles = target_tiles
        self._active_refresh_workers = 0
        self._refreshed_count = 0
        self.status_label.setText(f"Status: {label} {len(target_tiles)} devices")
        self._dispatch_refresh_workers()

    def _handle_refresh_result(self, tile: DeviceScreenTile, payload: dict[str, Any]) -> None:
        tile.apply_frame_payload(payload)
        self._refreshed_count += 1
        self._active_refresh_workers = max(0, self._active_refresh_workers - 1)
        if self._refresh_in_progress:
            self.status_label.setText(
                f"Status: refreshing {self._refreshed_count}/{self._refreshed_count + len(self._pending_tiles) + self._active_refresh_workers}"
            )
        self._dispatch_refresh_workers()

    def force_reconnect_all(self) -> None:
        for tile in self._tiles:
            tile.force_reconnect()
        self.status_label.setText("Status: reconnecting all")
        self.refresh_all()

    def refresh_selected_tiles(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        self._refresh_tiles(selected_tiles, label="refreshing selected")

    def force_reconnect_selected_tiles(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        for tile in selected_tiles:
            tile.force_reconnect()
        self.status_label.setText(f"Status: reconnecting {len(selected_tiles)} selected devices")
        self._refresh_tiles(selected_tiles, label="refreshing selected")

    def set_min_brightness_selected_tiles(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        success_count = 0
        failures: list[str] = []
        for tile in selected_tiles:
            success, message = tile.set_min_brightness()
            if success:
                success_count += 1
            else:
                failures.append(f"{tile.device_name}: {message}")
        if failures:
            self.status_label.setText(
                f"Status: brightness set on {success_count}/{len(selected_tiles)} devices; {failures[0]}"
            )
            return
        self.status_label.setText(f"Status: brightness set to minimum on {success_count} devices")

    def set_max_brightness_selected_tiles(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        success_count = 0
        failures: list[str] = []
        for tile in selected_tiles:
            success, message = tile.set_max_brightness()
            if success:
                success_count += 1
            else:
                failures.append(f"{tile.device_name}: {message}")
        if failures:
            self.status_label.setText(
                f"Status: brightness set on {success_count}/{len(selected_tiles)} devices; {failures[0]}"
            )
            return
        self.status_label.setText(f"Status: brightness set to maximum on {success_count} devices")

    def press_home_selected_tiles(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        success_count = 0
        failures: list[str] = []
        for tile in selected_tiles:
            success, message = tile.press_home()
            if success:
                success_count += 1
            else:
                failures.append(f"{tile.device_name}: {message}")
        if failures:
            self.status_label.setText(
                f"Status: Home sent to {success_count}/{len(selected_tiles)} devices; {failures[0]}"
            )
            return
        self.status_label.setText(f"Status: Home sent to {success_count} devices")

    def press_back_selected_tiles(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        success_count = 0
        failures: list[str] = []
        for tile in selected_tiles:
            success, message = tile.press_back()
            if success:
                success_count += 1
            else:
                failures.append(f"{tile.device_name}: {message}")
        if failures:
            self.status_label.setText(
                f"Status: Back sent to {success_count}/{len(selected_tiles)} devices; {failures[0]}"
            )
            return
        self.status_label.setText(f"Status: Back sent to {success_count} devices")

    def press_recent_apps_selected_tiles(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        success_count = 0
        failures: list[str] = []
        for tile in selected_tiles:
            success, message = tile.press_recent_apps()
            if success:
                success_count += 1
            else:
                failures.append(f"{tile.device_name}: {message}")
        if failures:
            self.status_label.setText(
                f"Status: Recent Apps sent to {success_count}/{len(selected_tiles)} devices; {failures[0]}"
            )
            return
        self.status_label.setText(f"Status: Recent Apps sent to {success_count} devices")

    def save_all_frames(self) -> None:
        saved_count = 0
        for tile in self._tiles:
            try:
                tile.save_current_frame()
            except Exception:
                continue
            saved_count += 1
        self.status_label.setText(f"Saved {saved_count} frames")

    def save_selected_frames(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        saved_count = 0
        for tile in selected_tiles:
            try:
                tile.save_current_frame()
            except Exception:
                continue
            saved_count += 1
        self.status_label.setText(f"Saved {saved_count}/{len(selected_tiles)} selected frames")

    def open_realtime_selected_viewers(self) -> None:
        selected_tiles = self._selected_tiles()
        if not selected_tiles:
            self.status_label.setText("Status: no selected devices")
            return
        opened_count = 0
        for tile in selected_tiles:
            previous = self.status_label.text()
            self._open_realtime_viewer(tile)
            if "Opened realtime backend" in self.status_label.text():
                opened_count += 1
            else:
                self.status_label.setText(previous)
        self.status_label.setText(f"Opened realtime for {opened_count}/{len(selected_tiles)} devices")

    def _save_tile_frame(self, tile: DeviceScreenTile) -> None:
        try:
            output_path = tile.save_current_frame()
        except Exception as exc:
            self.status_label.setText(f"Save failed for {tile.device_name}: {exc}")
            return
        self.status_label.setText(f"Saved {tile.device_name}: {output_path.name}")

    def _build_scrcpy_arguments(self, serial: str) -> list[str]:
        arguments = ["-s", serial]
        if self._scrcpy_max_size > 0:
            arguments.extend(["--max-size", str(self._scrcpy_max_size)])
        if self._scrcpy_max_fps > 0:
            arguments.extend(["--max-fps", str(self._scrcpy_max_fps)])
        if self._scrcpy_bit_rate:
            arguments.extend(["--video-bit-rate", self._scrcpy_bit_rate])
        return arguments

    def _start_detached_scrcpy(self, serial: str) -> bool:
        if not self._scrcpy_program:
            return False
        result = QtCore.QProcess.startDetached(self._scrcpy_program, self._build_scrcpy_arguments(serial))
        if isinstance(result, tuple):
            return bool(result[0])
        return bool(result)

    def _open_detail_viewer(self, tile: DeviceScreenTile) -> None:
        serial = tile.serial
        viewer = self._detail_windows.get(serial)
        if viewer is None:
            viewer = DeviceDetailViewerWindow(
                device_name=tile.device_name,
                serial=serial,
                initial_pixmap=tile.current_pixmap(),
                scrcpy_program=self._scrcpy_program,
            )
            viewer.destroyed.connect(lambda _obj=None, serial=serial: self._detail_windows.pop(serial, None))
            self._detail_windows[serial] = viewer
        else:
            viewer._scrcpy_program = self._scrcpy_program
            viewer._update_realtime_controls()
            viewer.set_pixmap(tile.current_pixmap())
        viewer.show()
        viewer.raise_()
        viewer.activateWindow()

    def _open_realtime_viewer(self, tile: DeviceScreenTile) -> None:
        if not self._scrcpy_program:
            if not self._select_scrcpy_program():
                self.status_label.setText("scrcpy not configured")
                return
        if self._start_detached_scrcpy(tile.serial):
            self.status_label.setText(f"Opened realtime backend for {tile.device_name}")
            return
        self.status_label.setText(f"Failed to open realtime backend for {tile.device_name}")

    def _sync_detail_window(self, tile: DeviceScreenTile, pixmap: QtGui.QPixmap) -> None:
        viewer = self._detail_windows.get(tile.serial)
        if viewer is not None:
            viewer.set_pixmap(pixmap)

    def _update_timer_interval(self) -> None:
        self.refresh_interval_ms = int(self.refresh_rate_combo.currentData() or 1000)
        self.timer.setInterval(self.refresh_interval_ms)
        self._sync_view_preset_from_controls()

    def _toggle_pause(self) -> None:
        if self.timer.isActive():
            self.timer.stop()
            self.pause_button.setText("Resume")
            self.status_label.setText("Status: paused")
            return
        self.timer.start()
        self.pause_button.setText("Pause")
        self.refresh_all()

    def _update_zoom_factor(self) -> None:
        self._zoom_factor = float(self.zoom_combo.currentData() or 1.0)
        for tile in self._tiles:
            tile.set_zoom_factor(self._zoom_factor)
        self._apply_tile_widths()

    def _update_resolution_scale(self) -> None:
        self._resolution_scale = float(self.resolution_combo.currentData() or 1.0)
        for tile in self._tiles:
            tile.set_resolution_scale(self._resolution_scale)
        scale_label = int(round(self._resolution_scale * 100))
        self.status_label.setText(f"Status: resolution set to {scale_label}%")
        self._sync_view_preset_from_controls()
        if self.autostart or self.timer.isActive():
            self.refresh_all()

    def _update_scrcpy_max_size(self) -> None:
        self._scrcpy_max_size = int(self.scrcpy_max_size_combo.currentData() or 0)
        self._sync_scrcpy_preset_from_controls()
        if self._scrcpy_max_size > 0:
            self.status_label.setText(f"Status: scrcpy max size set to {self._scrcpy_max_size}px")
            return
        self.status_label.setText("Status: scrcpy max size set to original")

    def _update_scrcpy_max_fps(self) -> None:
        self._scrcpy_max_fps = int(self.scrcpy_max_fps_combo.currentData() or 0)
        self._sync_scrcpy_preset_from_controls()
        if self._scrcpy_max_fps > 0:
            self.status_label.setText(f"Status: scrcpy max fps set to {self._scrcpy_max_fps}")
            return
        self.status_label.setText("Status: scrcpy max fps set to default")

    def _update_scrcpy_bit_rate(self) -> None:
        self._scrcpy_bit_rate = str(self.scrcpy_bit_rate_combo.currentData() or "")
        self._sync_scrcpy_preset_from_controls()
        if self._scrcpy_bit_rate:
            self.status_label.setText(f"Status: scrcpy bitrate set to {self._scrcpy_bit_rate}")
            return
        self.status_label.setText("Status: scrcpy bitrate set to default")

    def _apply_scrcpy_preset(self) -> None:
        if self._updating_scrcpy_preset:
            return
        preset_key = str(self.scrcpy_preset_combo.currentData() or "custom")
        if preset_key == "custom":
            return
        preset = self.SCRCPY_PRESETS.get(preset_key)
        if not preset:
            return
        self._updating_scrcpy_preset = True
        try:
            self.scrcpy_max_size_combo.setCurrentIndex(max(self.scrcpy_max_size_combo.findData(preset["max_size"]), 0))
            self.scrcpy_max_fps_combo.setCurrentIndex(max(self.scrcpy_max_fps_combo.findData(preset["max_fps"]), 0))
            self.scrcpy_bit_rate_combo.setCurrentIndex(max(self.scrcpy_bit_rate_combo.findData(preset["bit_rate"]), 0))
        finally:
            self._updating_scrcpy_preset = False
        self.status_label.setText(f"Status: scrcpy preset {self.scrcpy_preset_combo.currentText()} applied")

    def _sync_scrcpy_preset_from_controls(self) -> None:
        if self._updating_scrcpy_preset:
            return
        matched_key = "custom"
        for preset_key, preset in self.SCRCPY_PRESETS.items():
            if (
                int(self.scrcpy_max_size_combo.currentData() or 0) == int(preset["max_size"])
                and int(self.scrcpy_max_fps_combo.currentData() or 0) == int(preset["max_fps"])
                and str(self.scrcpy_bit_rate_combo.currentData() or "") == str(preset["bit_rate"])
            ):
                matched_key = preset_key
                break
        self._updating_scrcpy_preset = True
        try:
            self.scrcpy_preset_combo.setCurrentIndex(max(self.scrcpy_preset_combo.findData(matched_key), 0))
        finally:
            self._updating_scrcpy_preset = False

    def _apply_view_preset(self) -> None:
        if self._updating_view_preset:
            return
        preset_key = str(self.view_preset_combo.currentData() or "custom")
        if preset_key == "custom":
            return
        preset = self.VIEW_PRESETS.get(preset_key)
        if not preset:
            return
        self._updating_view_preset = True
        try:
            refresh_index = max(self.refresh_rate_combo.findData(preset["refresh_interval_ms"]), 0)
            resolution_index = max(self.resolution_combo.findData(preset["resolution_scale"]), 0)
            self.refresh_rate_combo.setCurrentIndex(refresh_index)
            self.resolution_combo.setCurrentIndex(resolution_index)
        finally:
            self._updating_view_preset = False
        self.status_label.setText(f"Status: preset {self.view_preset_combo.currentText()} applied")
        if self.autostart or self.timer.isActive():
            self.refresh_all()

    def _sync_view_preset_from_controls(self) -> None:
        if self._updating_view_preset:
            return
        matched_key = "custom"
        for preset_key, preset in self.VIEW_PRESETS.items():
            if (
                int(self.refresh_rate_combo.currentData() or 0) == int(preset["refresh_interval_ms"])
                and abs(float(self.resolution_combo.currentData() or 0.0) - float(preset["resolution_scale"])) < 0.001
            ):
                matched_key = preset_key
                break
        self._updating_view_preset = True
        try:
            preset_index = max(self.view_preset_combo.findData(matched_key), 0)
            self.view_preset_combo.setCurrentIndex(preset_index)
        finally:
            self._updating_view_preset = False

    def _step_zoom_out(self) -> None:
        self.zoom_combo.setCurrentIndex(max(0, self.zoom_combo.currentIndex() - 1))

    def _step_zoom_in(self) -> None:
        self.zoom_combo.setCurrentIndex(min(self.zoom_combo.count() - 1, self.zoom_combo.currentIndex() + 1))

    def _apply_tile_widths(self) -> None:
        for tile in self._tiles:
            target_width = tile._preferred_tile_width()
            tile.setMinimumWidth(target_width)
            tile.setMaximumWidth(target_width)
            tile.updateGeometry()

    def _set_scrcpy_program(self, program: str | None, *, persist: bool = True) -> None:
        normalized = str(program or "").strip() or None
        self._scrcpy_program = normalized
        if persist:
            save_scrcpy_path(normalized or "")
        for viewer in self._detail_windows.values():
            viewer._scrcpy_program = normalized
            viewer._update_realtime_controls()
        self._update_scrcpy_controls()

    def _select_scrcpy_program(self) -> bool:
        selected_path, _selected_filter = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Locate scrcpy",
            str(Path.home()),
            "scrcpy executable (scrcpy.exe);;Executable (*.exe);;All files (*)",
        )
        if not selected_path:
            return False
        self._set_scrcpy_program(selected_path)
        self.status_label.setText(f"Using scrcpy: {Path(selected_path).name}")
        return True

    def _update_scrcpy_controls(self) -> None:
        if self._scrcpy_program:
            self.locate_scrcpy_button.setText("Change scrcpy")
            return
        self.locate_scrcpy_button.setText("Locate scrcpy")

    def _set_combo_data_if_present(self, combo: QtWidgets.QComboBox, value: object) -> bool:
        index = combo.findData(value)
        if index < 0:
            return False
        combo.setCurrentIndex(index)
        return True

    def _restore_screen_wall_settings(self) -> None:
        settings = _settings()
        self._set_combo_data_if_present(
            self.view_preset_combo,
            str(settings.value("wall/view_preset", "custom") or "custom"),
        )
        self._set_combo_data_if_present(
            self.refresh_rate_combo,
            int(settings.value("wall/refresh_interval_ms", self.refresh_interval_ms) or self.refresh_interval_ms),
        )
        self._set_combo_data_if_present(
            self.resolution_combo,
            float(settings.value("wall/resolution_scale", self._resolution_scale) or self._resolution_scale),
        )
        self._set_combo_data_if_present(
            self.zoom_combo,
            float(settings.value("wall/zoom_factor", self._zoom_factor) or self._zoom_factor),
        )
        self._set_combo_data_if_present(
            self.scrcpy_preset_combo,
            str(settings.value("wall/scrcpy_preset", "custom") or "custom"),
        )
        self._set_combo_data_if_present(
            self.scrcpy_max_size_combo,
            int(settings.value("wall/scrcpy_max_size", self._scrcpy_max_size) or self._scrcpy_max_size),
        )
        self._set_combo_data_if_present(
            self.scrcpy_max_fps_combo,
            int(settings.value("wall/scrcpy_max_fps", self._scrcpy_max_fps) or self._scrcpy_max_fps),
        )
        self._set_combo_data_if_present(
            self.scrcpy_bit_rate_combo,
            str(settings.value("wall/scrcpy_bit_rate", self._scrcpy_bit_rate) or self._scrcpy_bit_rate),
        )

    def _save_screen_wall_settings(self) -> None:
        if self._suspend_settings_save:
            return
        settings = _settings()
        settings.setValue("wall/view_preset", self.view_preset_combo.currentData())
        settings.setValue("wall/refresh_interval_ms", self.refresh_rate_combo.currentData())
        settings.setValue("wall/resolution_scale", self.resolution_combo.currentData())
        settings.setValue("wall/zoom_factor", self.zoom_combo.currentData())
        settings.setValue("wall/workflow_id", self.workflow_combo.currentData() or 0)
        settings.setValue("wall/workflow_parallelism", self.workflow_mode_combo.currentData() or 1)
        settings.setValue("wall/scrcpy_preset", self.scrcpy_preset_combo.currentData())
        settings.setValue("wall/scrcpy_max_size", self.scrcpy_max_size_combo.currentData())
        settings.setValue("wall/scrcpy_max_fps", self.scrcpy_max_fps_combo.currentData())
        settings.setValue("wall/scrcpy_bit_rate", self.scrcpy_bit_rate_combo.currentData())
        settings.sync()

    def resizeEvent(self, event: QtGui.QResizeEvent) -> None:
        super().resizeEvent(event)
        self._apply_tile_widths()

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        self._save_screen_wall_settings()
        self.timer.stop()
        if self._workflow_runner is not None:
            self._workflow_runner.request_stop()
            wait_fn = getattr(self._workflow_runner, "wait", None)
            if callable(wait_fn):
                wait_fn(1500)
        self._thread_pool.waitForDone(2000)
        for viewer in list(self._detail_windows.values()):
            viewer.close()
        super().closeEvent(event)
