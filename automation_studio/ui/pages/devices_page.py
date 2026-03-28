from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class DevicesPage(QtWidgets.QWidget):
    devices_changed = QtCore.Signal()
    open_screen_requested = QtCore.Signal()

    STATUS_LABELS = {
        "connected": "Connected",
        "failed": "Failed",
        "unknown": "Unknown",
    }

    STATUS_COLORS = {
        "connected": ("#86efac", "#0f2f23"),
        "failed": ("#fca5a5", "#3a1620"),
        "unknown": ("#dbe7ff", "#14233b"),
    }

    AUTO_REFRESH_INTERVALS = {
        "15 sec": 15_000,
        "30 sec": 30_000,
        "60 sec": 60_000,
        "120 sec": 120_000,
    }

    def __init__(self, device_service, log_service=None, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.device_service = device_service
        self.log_service = log_service
        self.current_device_id: int | None = None
        self._devices: list[dict[str, Any]] = []
        self._filtered_devices: list[dict[str, Any]] = []
        self._last_artifact_path: Path | None = None
        self._build_ui()
        self._init_auto_refresh()
        self.load_devices()

    def _build_ui(self) -> None:
        root_layout = QtWidgets.QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(16)

        header = QtWidgets.QVBoxLayout()
        title = QtWidgets.QLabel("Devices")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("จัดการอุปกรณ์ เช็กสุขภาพเครื่อง และทำ maintenance สำหรับงาน automation")
        subtitle.setObjectName("subtitleLabel")
        subtitle.setWordWrap(True)
        header.addWidget(title)
        header.addWidget(subtitle)
        root_layout.addLayout(header)

        stats_layout = QtWidgets.QGridLayout()
        stats_layout.setHorizontalSpacing(12)
        stats_layout.setVerticalSpacing(12)
        self.summary_labels: dict[str, QtWidgets.QLabel] = {}
        for index, (key, label_text) in enumerate(
            (
                ("total", "Total Devices"),
                ("connected", "Connected"),
                ("failed", "Failed"),
                ("wireless", "Wireless ADB"),
            )
        ):
            card = CardFrame()
            card_layout = QtWidgets.QVBoxLayout(card)
            card_layout.setContentsMargins(12, 10, 12, 10)
            card_layout.setSpacing(4)
            label = QtWidgets.QLabel(label_text)
            label.setObjectName("subtitleLabel")
            value = QtWidgets.QLabel("0")
            value.setObjectName("titleLabel")
            card_layout.addWidget(label)
            card_layout.addWidget(value)
            stats_layout.addWidget(card, 0, index)
            self.summary_labels[key] = value
        root_layout.addLayout(stats_layout)

        content = QtWidgets.QSplitter()
        content.setChildrenCollapsible(False)
        root_layout.addWidget(content, 1)

        table_card = CardFrame()
        table_layout = QtWidgets.QVBoxLayout(table_card)
        table_layout.setContentsMargins(18, 18, 18, 18)
        table_layout.setSpacing(12)

        action_row = QtWidgets.QHBoxLayout()
        self.refresh_button = make_button("Refresh", "secondary")
        self.refresh_info_button = make_button("Refresh Info", "secondary")
        self.test_button = make_button("Test Connection")
        action_row.addWidget(self.refresh_button)
        action_row.addWidget(self.refresh_info_button)
        action_row.addWidget(self.test_button)
        action_row.addStretch(1)
        table_layout.addLayout(action_row)

        auto_refresh_row = QtWidgets.QHBoxLayout()
        auto_refresh_row.addWidget(make_form_label("Auto Refresh"))
        self.auto_refresh_check = QtWidgets.QCheckBox("Enable")
        self.auto_refresh_interval = QtWidgets.QComboBox()
        for label_text, interval in self.AUTO_REFRESH_INTERVALS.items():
            self.auto_refresh_interval.addItem(label_text, interval)
        self.auto_refresh_status_label = QtWidgets.QLabel("Off")
        self.auto_refresh_status_label.setObjectName("subtitleLabel")
        auto_refresh_row.addWidget(self.auto_refresh_check)
        auto_refresh_row.addWidget(self.auto_refresh_interval)
        auto_refresh_row.addWidget(self.auto_refresh_status_label)
        auto_refresh_row.addStretch(1)
        table_layout.addLayout(auto_refresh_row)

        filter_row = QtWidgets.QHBoxLayout()
        filter_row.addWidget(make_form_label("Search"))
        self.search_input = QtWidgets.QLineEdit()
        self.search_input.setPlaceholderText("Search by name, serial, notes, current app, or status")
        self.clear_search_button = make_button("Clear", "secondary")
        filter_row.addWidget(self.search_input, 1)
        filter_row.addWidget(make_form_label("Status"))
        self.status_filter = QtWidgets.QComboBox()
        self.status_filter.addItem("All Status", "all")
        self.status_filter.addItem("Connected", "connected")
        self.status_filter.addItem("Failed", "failed")
        self.status_filter.addItem("Unknown", "unknown")
        filter_row.addWidget(self.status_filter)
        filter_row.addWidget(self.clear_search_button)
        table_layout.addLayout(filter_row)

        self.result_count_label = QtWidgets.QLabel("0 devices")
        self.result_count_label.setObjectName("subtitleLabel")
        table_layout.addWidget(self.result_count_label)

        self.table = QtWidgets.QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["ID", "Name", "Serial", "Type", "Status", "Last Seen"])
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setColumnHidden(0, True)
        self.table.setWordWrap(False)
        table_layout.addWidget(self.table, 1)
        content.addWidget(table_card)

        details_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        details_splitter.setChildrenCollapsible(False)
        details_splitter.setHandleWidth(10)

        form_card = CardFrame()
        form_card.setMinimumHeight(440)
        form_layout = QtWidgets.QVBoxLayout(form_card)
        form_layout.setContentsMargins(18, 18, 18, 18)
        form_layout.setSpacing(8)

        form_title = QtWidgets.QLabel("Device Details")
        form_title.setObjectName("subtitleLabel")
        form_layout.addWidget(form_title)

        details_grid = QtWidgets.QGridLayout()
        details_grid.setHorizontalSpacing(12)
        details_grid.setVerticalSpacing(8)
        self.detail_labels: dict[str, QtWidgets.QLabel] = {}
        for row, (key, label_text) in enumerate(
            (
                ("status", "Status"),
                ("type", "Connection Type"),
                ("last_seen", "Last Seen"),
                ("manufacturer", "Manufacturer"),
                ("model", "Model"),
                ("android", "Android"),
                ("current_app", "Current App"),
                ("screen_on", "Screen On"),
                ("display", "Display"),
            )
        ):
            details_grid.addWidget(make_form_label(label_text), row, 0)
            value = QtWidgets.QLabel("-")
            value.setWordWrap(True)
            details_grid.addWidget(value, row, 1)
            self.detail_labels[key] = value
        form_layout.addLayout(details_grid)

        maintenance_row = QtWidgets.QHBoxLayout()
        maintenance_row.setContentsMargins(0, 0, 0, 0)
        maintenance_row.setSpacing(8)
        self.screenshot_button = make_button("Screenshot", "secondary")
        self.dump_hierarchy_button = make_button("Dump Hierarchy", "secondary")
        self.open_artifacts_button = make_button("Open Artifacts", "secondary")
        maintenance_row.addWidget(self.screenshot_button)
        maintenance_row.addWidget(self.dump_hierarchy_button)
        maintenance_row.addWidget(self.open_artifacts_button)
        form_layout.addLayout(maintenance_row)

        form_layout.addWidget(make_form_label("Name"))
        self.name_input = QtWidgets.QLineEdit()
        self.name_input.setPlaceholderText("Pixel 7 Pro")
        form_layout.addWidget(self.name_input)

        form_layout.addWidget(make_form_label("Serial / ADB Address"))
        self.serial_input = QtWidgets.QLineEdit()
        self.serial_input.setPlaceholderText("emulator-5554 หรือ 192.168.1.10:5555")
        form_layout.addWidget(self.serial_input)

        self.serial_hint_label = QtWidgets.QLabel("รองรับ serial ของ ADB, emulator, และการเชื่อมต่อแบบ IP:PORT")
        self.serial_hint_label.setObjectName("subtitleLabel")
        self.serial_hint_label.setWordWrap(True)
        form_layout.addWidget(self.serial_hint_label)

        form_layout.addWidget(make_form_label("Notes"))
        self.notes_input = QtWidgets.QTextEdit()
        self.notes_input.setPlaceholderText("เช่น ใช้กับงานทดสอบ, เครื่องสำหรับ schedule กลางคืน, หรือกลุ่มอุปกรณ์เฉพาะ")
        self.notes_input.setFixedHeight(68)
        form_layout.addWidget(self.notes_input)

        button_row = QtWidgets.QHBoxLayout()
        self.new_button = make_button("New", "secondary")
        self.save_button = make_button("Save Device")
        self.delete_button = make_button("Delete", "danger")
        button_row.addWidget(self.new_button)
        button_row.addWidget(self.save_button)
        button_row.addWidget(self.delete_button)
        form_layout.addLayout(button_row)

        self.status_label = QtWidgets.QLabel("เลือกอุปกรณ์หรือกรอกข้อมูลใหม่เพื่อเริ่มต้น")
        self.status_label.setWordWrap(True)
        self.status_label.setObjectName("subtitleLabel")
        form_layout.addWidget(self.status_label)
        details_splitter.addWidget(form_card)

        activity_card = CardFrame()
        activity_card.setMinimumHeight(110)
        activity_card.setMaximumHeight(160)
        activity_layout = QtWidgets.QVBoxLayout(activity_card)
        activity_layout.setContentsMargins(18, 18, 18, 18)
        activity_layout.setSpacing(10)
        activity_layout.addWidget(make_form_label("Device Activity"))
        self.activity_list = QtWidgets.QListWidget()
        self.activity_list.setMinimumHeight(64)
        activity_layout.addWidget(self.activity_list, 1)
        details_splitter.addWidget(activity_card)
        details_splitter.setStretchFactor(0, 5)
        details_splitter.setStretchFactor(1, 1)
        details_splitter.setSizes([560, 120])

        content.addWidget(details_splitter)
        content.setStretchFactor(0, 3)
        content.setStretchFactor(1, 2)

        self.refresh_button.clicked.connect(self.load_devices)
        self.refresh_info_button.clicked.connect(self.refresh_device_info)
        self.new_button.clicked.connect(self.clear_form)
        self.save_button.clicked.connect(self.save_device)
        self.delete_button.clicked.connect(self.delete_device)
        self.test_button.clicked.connect(self.test_connection)
        self.screenshot_button.clicked.connect(self.capture_screenshot)
        self.dump_hierarchy_button.clicked.connect(self.dump_hierarchy)
        self.open_artifacts_button.clicked.connect(self.open_artifacts_folder)
        self.clear_search_button.clicked.connect(self._clear_search)
        self.status_filter.currentIndexChanged.connect(self._apply_filters)
        self.search_input.textChanged.connect(self._apply_filters)
        self.table.itemSelectionChanged.connect(self._on_row_selected)
        self.auto_refresh_check.toggled.connect(self._update_auto_refresh_timer)
        self.auto_refresh_interval.currentIndexChanged.connect(self._update_auto_refresh_timer)
        self._update_action_buttons(False)

    def _init_auto_refresh(self) -> None:
        self.auto_refresh_timer = QtCore.QTimer(self)
        self.auto_refresh_timer.timeout.connect(self._auto_refresh_tick)
        self._update_auto_refresh_timer()

    def load_devices(self) -> None:
        self._devices = self.device_service.list_devices()
        self._update_summary_cards()
        self._apply_filters()
        self.devices_changed.emit()

    def _apply_filters(self) -> None:
        search_text = self.search_input.text().strip().casefold()
        selected_status = str(self.status_filter.currentData() or "all")
        self._filtered_devices = []
        for device in self._devices:
            status = str(device.get("last_status") or "unknown")
            if selected_status != "all" and status != selected_status:
                continue
            if search_text and not self._matches_device_search(device, search_text):
                continue
            self._filtered_devices.append(device)
        self._populate_table()

    def _matches_device_search(self, device: dict[str, Any], search_text: str) -> bool:
        runtime_info = self._saved_runtime_info(device)
        current_app = runtime_info.get("current_app", {})
        haystack = " ".join(
            [
                str(device.get("name") or ""),
                str(device.get("serial") or ""),
                str(device.get("notes") or ""),
                str(device.get("last_status") or ""),
                self._connection_type_label(str(device.get("serial") or "")),
                str(runtime_info.get("manufacturer") or runtime_info.get("brand") or ""),
                str(runtime_info.get("model") or runtime_info.get("marketName") or ""),
                str(current_app.get("package") or current_app.get("packageName") or ""),
            ]
        ).casefold()
        return search_text in haystack

    def _populate_table(self) -> None:
        previous_id = self.current_device_id
        self.table.blockSignals(True)
        self.table.setRowCount(len(self._filtered_devices))
        for row_index, device in enumerate(self._filtered_devices):
            status = str(device.get("last_status") or "unknown")
            values = [
                str(device["id"]),
                str(device.get("name") or ""),
                str(device.get("serial") or ""),
                self._connection_type_label(str(device.get("serial") or "")),
                self._status_label(status),
                str(device.get("last_seen") or "-"),
            ]
            for column, value in enumerate(values):
                item = QtWidgets.QTableWidgetItem(value)
                self.table.setItem(row_index, column, item)
            self._style_row(row_index, status)
        self.table.blockSignals(False)
        self.table.resizeColumnsToContents()
        self.table.setColumnWidth(2, min(max(self.table.columnWidth(2), 180), 280))
        self.result_count_label.setText(f"{len(self._filtered_devices)} devices")

        if not self._filtered_devices:
            self.current_device_id = None
            self.table.clearSelection()
            self._update_detail_panel(None)
            return

        target_row = 0
        if previous_id is not None:
            for row_index, device in enumerate(self._filtered_devices):
                if int(device["id"]) == previous_id:
                    target_row = row_index
                    break
        self.table.selectRow(target_row)

    def _style_row(self, row_index: int, status: str) -> None:
        foreground_hex, background_hex = self.STATUS_COLORS.get(status, self.STATUS_COLORS["unknown"])
        foreground = QtGui.QColor(foreground_hex)
        background = QtGui.QColor(background_hex)
        for column in range(self.table.columnCount()):
            item = self.table.item(row_index, column)
            if item is None:
                continue
            item.setForeground(foreground)
            if column == 4:
                item.setBackground(background)

    def _update_summary_cards(self) -> None:
        connected_count = 0
        failed_count = 0
        wireless_count = 0
        for device in self._devices:
            status = str(device.get("last_status") or "unknown")
            serial = str(device.get("serial") or "")
            if status == "connected":
                connected_count += 1
            elif status == "failed":
                failed_count += 1
            if self._is_wireless_serial(serial):
                wireless_count += 1
        self.summary_labels["total"].setText(str(len(self._devices)))
        self.summary_labels["connected"].setText(str(connected_count))
        self.summary_labels["failed"].setText(str(failed_count))
        self.summary_labels["wireless"].setText(str(wireless_count))

    def _on_row_selected(self) -> None:
        row = self.table.currentRow()
        if row < 0 or row >= len(self._filtered_devices):
            self.current_device_id = None
            self._update_detail_panel(None)
            return
        device = self._filtered_devices[row]
        self.current_device_id = int(device["id"])
        self._update_detail_panel(device)

    def _update_detail_panel(self, device: dict[str, Any] | None) -> None:
        has_selection = device is not None
        self._update_action_buttons(has_selection)
        if device is None:
            self.name_input.clear()
            self.serial_input.clear()
            self.notes_input.clear()
            self.serial_hint_label.setText("รองรับ serial ของ ADB, emulator, และการเชื่อมต่อแบบ IP:PORT")
            self.status_label.setText("เลือกอุปกรณ์หรือกรอกข้อมูลใหม่เพื่อเริ่มต้น")
            for label in self.detail_labels.values():
                label.setText("-")
            self.activity_list.clear()
            return

        self.name_input.setText(str(device.get("name") or ""))
        self.serial_input.setText(str(device.get("serial") or ""))
        self.notes_input.setPlainText(str(device.get("notes") or ""))
        self.serial_hint_label.setText(self._serial_validation_message(str(device.get("serial") or "")))

        status = str(device.get("last_status") or "unknown")
        runtime_info = self._saved_runtime_info(device)
        current_app = runtime_info.get("current_app", {})
        window_size = runtime_info.get("window_size", {})
        self.detail_labels["status"].setText(self._status_label(status))
        self.detail_labels["type"].setText(self._connection_type_label(str(device.get("serial") or "")))
        self.detail_labels["last_seen"].setText(str(device.get("last_seen") or "-"))
        self.detail_labels["manufacturer"].setText(
            str(runtime_info.get("manufacturer") or runtime_info.get("brand") or runtime_info.get("productName") or "-")
        )
        self.detail_labels["model"].setText(
            str(runtime_info.get("model") or runtime_info.get("marketName") or runtime_info.get("device") or "-")
        )
        self.detail_labels["android"].setText(
            str(runtime_info.get("version") or runtime_info.get("release") or runtime_info.get("sdkInt") or "-")
        )
        self.detail_labels["current_app"].setText(
            str(current_app.get("package") or current_app.get("packageName") or current_app.get("activity") or "-")
        )
        self.detail_labels["screen_on"].setText(self._screen_on_label(runtime_info.get("screen_on")))
        self.detail_labels["display"].setText(self._display_label(window_size))
        self.status_label.setText("ใช้ Test Connection / Refresh Info เพื่อตรวจสุขภาพเครื่อง หรือใช้ maintenance actions เพื่อเก็บหลักฐานหน้างาน")
        self._load_device_activity()

    def _load_device_activity(self) -> None:
        self.activity_list.clear()
        if self.log_service is None or self.current_device_id is None:
            return
        rows = self.log_service.list_logs(device_id=self.current_device_id, limit=50)
        events = [row for row in rows if str(row.get("status") or "").startswith("device_")]
        for row in events[:20]:
            item = QtWidgets.QListWidgetItem(
                f"{row.get('created_at') or '-'}\n{row.get('status') or '-'} / {row.get('message') or '-'}"
            )
            status = str(row.get("status") or "")
            if "failed" in status or "error" in str(row.get("level") or "").casefold():
                item.setForeground(QtGui.QColor("#fca5a5"))
            elif "saved" in status or "refreshed" in status or "connected" in status:
                item.setForeground(QtGui.QColor("#86efac"))
            self.activity_list.addItem(item)

    def clear_form(self) -> None:
        self.current_device_id = None
        self.table.clearSelection()
        self._update_detail_panel(None)
        self.status_label.setText("พร้อมเพิ่มอุปกรณ์ใหม่")

    def save_device(self) -> None:
        name = self.name_input.text().strip()
        serial = self.serial_input.text().strip()
        notes = self.notes_input.toPlainText().strip()
        validation_error = self._validate_device_input(name, serial)
        if validation_error:
            QtWidgets.QMessageBox.warning(self, "Missing data", validation_error)
            return
        try:
            device_id = self.device_service.save_device(self.current_device_id, name, serial, notes)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Save failed", str(exc))
            return
        self.current_device_id = device_id
        self.status_label.setText("บันทึกอุปกรณ์เรียบร้อย")
        self._log_device_event("device_saved", "Saved device record", {"serial": serial})
        self.load_devices()

    def delete_device(self) -> None:
        if not self.current_device_id:
            return
        confirmation = QtWidgets.QMessageBox.question(
            self,
            "Delete device",
            "ต้องการลบอุปกรณ์นี้หรือไม่?",
        )
        if confirmation != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        deleted_id = self.current_device_id
        deleted_serial = self.serial_input.text().strip()
        self.device_service.delete_device(self.current_device_id)
        self.current_device_id = None
        self.clear_form()
        self.load_devices()
        self._log_device_event(
            "device_deleted",
            "Deleted device record",
            {"serial": deleted_serial},
            device_id_override=deleted_id,
        )

    def refresh_device_info(self) -> None:
        self._run_runtime_refresh(show_dialog=False, action_label="อัปเดตข้อมูลอุปกรณ์", log_success_status="device_refreshed")

    def test_connection(self) -> None:
        self._run_runtime_refresh(show_dialog=True, action_label="ทดสอบการเชื่อมต่อ", log_success_status="device_connected")

    def _run_runtime_refresh(self, show_dialog: bool, action_label: str, log_success_status: str) -> None:
        serial = self.serial_input.text().strip()
        if not serial:
            QtWidgets.QMessageBox.warning(self, "Missing serial", "กรุณากรอก Serial / ADB Address")
            return
        success, message, info = self.device_service.refresh_runtime_info(serial, self.current_device_id)
        self.status_label.setText(message)
        self.load_devices()
        if self.current_device_id is not None:
            device = self._current_device_record()
            if device is not None:
                self._update_detail_panel(device)
        if success:
            current_app = {}
            if isinstance(info, dict):
                current_app = info.get("current_app", {})
            self._log_device_event(
                log_success_status,
                message,
                {
                    "serial": serial,
                    "current_app": current_app.get("package") or current_app.get("packageName") or "",
                    "screen_on": info.get("screen_on") if isinstance(info, dict) else None,
                },
            )
        else:
            self._log_device_event("device_refresh_failed", message, {"serial": serial}, level="ERROR")
        if not show_dialog:
            return
        if success:
            QtWidgets.QMessageBox.information(self, "Connection OK", f"{action_label} สำเร็จ\n{message}")
        else:
            QtWidgets.QMessageBox.warning(self, "Connection failed", f"{action_label} ไม่สำเร็จ\n{message}")

    def capture_screenshot(self) -> None:
        serial = self.serial_input.text().strip()
        if not serial:
            QtWidgets.QMessageBox.warning(self, "Missing serial", "กรุณากรอก Serial / ADB Address")
            return
        output_path = self._artifact_path("screenshots", "png")
        success, message, saved_path = self.device_service.capture_screenshot(serial, output_path, self.current_device_id)
        self.status_label.setText(message)
        if success and saved_path:
            self._last_artifact_path = Path(saved_path)
            self._log_device_event("device_screenshot_saved", message, {"path": saved_path})
            QtWidgets.QMessageBox.information(self, "Screenshot Saved", message)
        else:
            self._log_device_event("device_screenshot_failed", message, {"path": str(output_path)}, level="ERROR")
            QtWidgets.QMessageBox.warning(self, "Screenshot failed", message)
        self.load_devices()

    def _open_selected_screen_viewer_legacy(self) -> None:
        device = self._current_device_record()
        serial = self.serial_input.text().strip()
        if device is None or not serial:
            QtWidgets.QMessageBox.warning(self, "Missing device", "กรุณาเลือกอุปกรณ์และตรวจสอบ Serial / ADB Address")
            return
        device_id = int(device["id"])
        device_name = str(device.get("name") or f"Device {device_id}")
        self._log_device_event("device_viewer_open_requested", "Opened screen viewer request", {"serial": serial})
        self.open_screen_requested.emit(device_id, serial, device_name)

    def open_screen_viewer(self) -> None:
        if not self._devices:
            QtWidgets.QMessageBox.warning(self, "Missing devices", "กรุณาเพิ่มอุปกรณ์ก่อนเปิด Screen Wall")
            return
        self._log_device_event(
            "device_viewer_open_requested",
            "Opened screen viewer request",
            {"device_count": len(self._devices), "mode": "all_devices"},
        )
        self.open_screen_requested.emit()

    def has_devices(self) -> bool:
        return bool(self._devices)

    def dump_hierarchy(self) -> None:
        serial = self.serial_input.text().strip()
        if not serial:
            QtWidgets.QMessageBox.warning(self, "Missing serial", "กรุณากรอก Serial / ADB Address")
            return
        output_path = self._artifact_path("hierarchy", "xml")
        success, message, saved_path = self.device_service.dump_hierarchy(serial, output_path, self.current_device_id)
        self.status_label.setText(message)
        if success and saved_path:
            self._last_artifact_path = Path(saved_path)
            self._log_device_event("device_hierarchy_saved", message, {"path": saved_path})
            QtWidgets.QMessageBox.information(self, "Hierarchy Saved", message)
        else:
            self._log_device_event("device_hierarchy_failed", message, {"path": str(output_path)}, level="ERROR")
            QtWidgets.QMessageBox.warning(self, "Hierarchy failed", message)
        self.load_devices()

    def open_artifacts_folder(self) -> None:
        target = self._artifacts_root()
        target.mkdir(parents=True, exist_ok=True)
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(target)))

    def _artifact_path(self, category: str, extension: str) -> Path:
        device = self._current_device_record()
        if device:
            raw_name = str(device.get("name") or "device")
            device_name = re.sub(r"[^A-Za-z0-9_-]+", "_", raw_name).strip("_") or "device"
        else:
            device_name = "device"
        timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        root = self._artifacts_root() / category
        root.mkdir(parents=True, exist_ok=True)
        return root / f"{device_name}_{timestamp}.{extension}"

    def _artifacts_root(self) -> Path:
        device_id = self.current_device_id or 0
        return Path.cwd() / "artifacts" / "devices" / f"device_{device_id}"

    def _current_device_record(self) -> dict[str, Any] | None:
        if self.current_device_id is None:
            return None
        return next((item for item in self._devices if int(item["id"]) == self.current_device_id), None)

    def _saved_runtime_info(self, device: dict[str, Any]) -> dict[str, Any]:
        payload = str(device.get("last_info_json") or "").strip()
        if not payload:
            return {}
        try:
            parsed = json.loads(payload)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _clear_search(self) -> None:
        self.search_input.clear()
        self.status_filter.setCurrentIndex(0)

    def _validate_device_input(self, name: str, serial: str) -> str | None:
        if not name or not serial:
            return "กรุณากรอก Name และ Serial"
        if len(name) < 2:
            return "Name ควรมีอย่างน้อย 2 ตัวอักษร"
        if len(serial) < 3:
            return "Serial / ADB Address สั้นเกินไป"
        if " " in serial:
            return "Serial / ADB Address ไม่ควรมีช่องว่าง"
        return None

    def _serial_validation_message(self, serial: str) -> str:
        if not serial:
            return "รองรับ serial ของ ADB, emulator, และการเชื่อมต่อแบบ IP:PORT"
        if self._is_wireless_serial(serial):
            return "ตรวจพบรูปแบบ Wireless ADB: เหมาะกับการเชื่อมต่อผ่านเครือข่าย"
        if serial.startswith("emulator-"):
            return "ตรวจพบ Emulator: ใช้งานผ่าน ADB emulator serial"
        if re.fullmatch(r"[A-Za-z0-9._:-]+", serial):
            return "รูปแบบ serial ใช้งานได้"
        return "ตรวจสอบ serial อีกครั้ง: ควรใช้ serial ของ ADB หรือ IP:PORT"

    def _connection_type_label(self, serial: str) -> str:
        if self._is_wireless_serial(serial):
            return "Wireless"
        if serial.startswith("emulator-"):
            return "Emulator"
        return "USB / ADB"

    def _is_wireless_serial(self, serial: str) -> bool:
        return bool(re.fullmatch(r"\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}", serial))

    def _status_label(self, status: str) -> str:
        return self.STATUS_LABELS.get(status, status.title() if status else "Unknown")

    def _screen_on_label(self, value: Any) -> str:
        if value is None:
            return "-"
        return "On" if bool(value) else "Off"

    def _display_label(self, window_size: dict[str, Any]) -> str:
        width = int(window_size.get("width") or 0)
        height = int(window_size.get("height") or 0)
        if width and height:
            return f"{width} x {height}"
        return "-"

    def _update_action_buttons(self, enabled: bool) -> None:
        for button in (
            self.refresh_info_button,
            self.test_button,
            self.delete_button,
            self.screenshot_button,
            self.dump_hierarchy_button,
            self.open_artifacts_button,
        ):
            button.setEnabled(enabled)

    def _log_device_event(
        self,
        status: str,
        message: str,
        metadata: dict[str, Any] | None = None,
        *,
        level: str = "INFO",
        device_id_override: int | None = None,
    ) -> None:
        if self.log_service is None:
            return
        device_id = device_id_override if device_id_override is not None else self.current_device_id
        payload = dict(metadata or {})
        payload.setdefault("source", "devices_page")
        self.log_service.add(None, device_id, level, status, message, payload)
        if device_id == self.current_device_id:
            self._load_device_activity()

    def _update_auto_refresh_timer(self) -> None:
        if not self.auto_refresh_check.isChecked():
            self.auto_refresh_timer.stop()
            self.auto_refresh_status_label.setText("Off")
            return
        interval = int(self.auto_refresh_interval.currentData() or 30_000)
        self.auto_refresh_timer.setInterval(interval)
        self.auto_refresh_timer.start()
        self.auto_refresh_status_label.setText(f"Every {interval // 1000}s")

    def _auto_refresh_tick(self) -> None:
        if self.current_device_id is None:
            self.load_devices()
            return
        device = self._current_device_record()
        if device is None:
            self.load_devices()
            return
        serial = str(device.get("serial") or "").strip()
        if not serial:
            return
        success, message, _info = self.device_service.refresh_runtime_info(serial, self.current_device_id)
        if not success:
            self._log_device_event("device_auto_refresh_failed", message, {"serial": serial}, level="ERROR")
        self.load_devices()
