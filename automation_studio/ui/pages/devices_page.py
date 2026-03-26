from __future__ import annotations

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class DevicesPage(QtWidgets.QWidget):
    devices_changed = QtCore.Signal()

    def __init__(self, device_service, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.device_service = device_service
        self.current_device_id: int | None = None
        self._build_ui()
        self.load_devices()

    def _build_ui(self) -> None:
        root_layout = QtWidgets.QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(16)

        header = QtWidgets.QHBoxLayout()
        title = QtWidgets.QLabel("Devices")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("จัดการรายการอุปกรณ์และเช็กการเชื่อมต่อกับ uiautomator2")
        subtitle.setObjectName("subtitleLabel")

        title_box = QtWidgets.QVBoxLayout()
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)
        root_layout.addLayout(header)

        content = QtWidgets.QSplitter()
        content.setChildrenCollapsible(False)
        root_layout.addWidget(content, 1)

        table_card = CardFrame()
        table_layout = QtWidgets.QVBoxLayout(table_card)
        table_layout.setContentsMargins(18, 18, 18, 18)
        table_layout.setSpacing(12)

        action_row = QtWidgets.QHBoxLayout()
        self.refresh_button = make_button("Refresh", "secondary")
        self.test_button = make_button("Test Connection")
        action_row.addWidget(self.refresh_button)
        action_row.addWidget(self.test_button)
        action_row.addStretch(1)
        table_layout.addLayout(action_row)

        self.table = QtWidgets.QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["ID", "Name", "Serial", "Status", "Last Seen"])
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setColumnHidden(0, True)
        table_layout.addWidget(self.table)
        content.addWidget(table_card)

        form_card = CardFrame()
        form_layout = QtWidgets.QVBoxLayout(form_card)
        form_layout.setContentsMargins(18, 18, 18, 18)
        form_layout.setSpacing(10)

        form_title = QtWidgets.QLabel("Device Details")
        form_title.setObjectName("subtitleLabel")
        form_layout.addWidget(form_title)

        form_layout.addWidget(make_form_label("Name"))
        self.name_input = QtWidgets.QLineEdit()
        self.name_input.setPlaceholderText("Pixel 7 Pro")
        form_layout.addWidget(self.name_input)

        form_layout.addWidget(make_form_label("Serial / ADB Address"))
        self.serial_input = QtWidgets.QLineEdit()
        self.serial_input.setPlaceholderText("emulator-5554 หรือ 192.168.1.10:5555")
        form_layout.addWidget(self.serial_input)

        form_layout.addWidget(make_form_label("Notes"))
        self.notes_input = QtWidgets.QTextEdit()
        self.notes_input.setPlaceholderText("เช่น ใช้กับบัญชีทดสอบ หรือกลุ่มงานเฉพาะ")
        self.notes_input.setFixedHeight(120)
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
        form_layout.addStretch(1)
        content.addWidget(form_card)

        content.setStretchFactor(0, 3)
        content.setStretchFactor(1, 2)

        self.refresh_button.clicked.connect(self.load_devices)
        self.new_button.clicked.connect(self.clear_form)
        self.save_button.clicked.connect(self.save_device)
        self.delete_button.clicked.connect(self.delete_device)
        self.test_button.clicked.connect(self.test_connection)
        self.table.itemSelectionChanged.connect(self._on_row_selected)

    def load_devices(self) -> None:
        devices = self.device_service.list_devices()
        self.table.setRowCount(len(devices))
        for row_index, device in enumerate(devices):
            values = [
                str(device["id"]),
                device["name"],
                device["serial"],
                device.get("last_status") or "unknown",
                device.get("last_seen") or "-",
            ]
            for column, value in enumerate(values):
                item = QtWidgets.QTableWidgetItem(value)
                self.table.setItem(row_index, column, item)
        self.table.resizeColumnsToContents()
        self.devices_changed.emit()

    def _on_row_selected(self) -> None:
        row = self.table.currentRow()
        if row < 0:
            return
        self.current_device_id = int(self.table.item(row, 0).text())
        self.name_input.setText(self.table.item(row, 1).text())
        self.serial_input.setText(self.table.item(row, 2).text())
        device = next(
            (item for item in self.device_service.list_devices() if item["id"] == self.current_device_id),
            None,
        )
        self.notes_input.setPlainText(device.get("notes", "") if device else "")
        self.status_label.setText("แก้ไขรายละเอียดอุปกรณ์ หรือกด Test Connection เพื่อตรวจสอบ")

    def clear_form(self) -> None:
        self.current_device_id = None
        self.table.clearSelection()
        self.name_input.clear()
        self.serial_input.clear()
        self.notes_input.clear()
        self.status_label.setText("พร้อมเพิ่มอุปกรณ์ใหม่")

    def save_device(self) -> None:
        name = self.name_input.text().strip()
        serial = self.serial_input.text().strip()
        notes = self.notes_input.toPlainText().strip()
        if not name or not serial:
            QtWidgets.QMessageBox.warning(self, "Missing data", "กรุณากรอก Name และ Serial")
            return
        try:
            device_id = self.device_service.save_device(self.current_device_id, name, serial, notes)
        except Exception as exc:
            QtWidgets.QMessageBox.critical(self, "Save failed", str(exc))
            return
        self.current_device_id = device_id
        self.status_label.setText("บันทึกอุปกรณ์เรียบร้อย")
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
        self.device_service.delete_device(self.current_device_id)
        self.clear_form()
        self.load_devices()

    def test_connection(self) -> None:
        serial = self.serial_input.text().strip()
        if not serial:
            QtWidgets.QMessageBox.warning(self, "Missing serial", "กรุณากรอก Serial / ADB Address")
            return
        success, message, _ = self.device_service.test_connection(serial, self.current_device_id)
        self.status_label.setText(message)
        self.load_devices()
        if success:
            QtWidgets.QMessageBox.information(self, "Connection OK", message)
        else:
            QtWidgets.QMessageBox.warning(self, "Connection failed", message)
