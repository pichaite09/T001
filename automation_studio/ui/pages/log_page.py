from __future__ import annotations

import json

from PySide6 import QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class LogPage(QtWidgets.QWidget):
    def __init__(
        self,
        log_service,
        workflow_service,
        device_service,
        telemetry_service,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.log_service = log_service
        self.workflow_service = workflow_service
        self.device_service = device_service
        self.telemetry_service = telemetry_service
        self._build_ui()
        self.refresh_filters()
        self.load_logs()

    def _build_ui(self) -> None:
        root_layout = QtWidgets.QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(16)

        title = QtWidgets.QLabel("Log")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("ดูผลการรันย้อนหลัง สถานะของแต่ละ step และ metadata ที่บันทึกไว้")
        subtitle.setObjectName("subtitleLabel")
        root_layout.addWidget(title)
        root_layout.addWidget(subtitle)

        card = CardFrame()
        card_layout = QtWidgets.QVBoxLayout(card)
        card_layout.setContentsMargins(18, 18, 18, 18)
        card_layout.setSpacing(12)

        filters = QtWidgets.QHBoxLayout()
        self.workflow_filter = QtWidgets.QComboBox()
        self.device_filter = QtWidgets.QComboBox()
        self.status_filter = QtWidgets.QComboBox()
        self.status_filter.addItem("All Status", "all")
        for label, value in (
            ("Workflow Started", "workflow_started"),
            ("Workflow Success", "workflow_success"),
            ("Workflow Failed", "workflow_failed"),
            ("Validation Failed", "validation_failed"),
            ("Step Started", "step_started"),
            ("Step Success", "step_success"),
            ("Step Retry", "step_retry"),
            ("Step Failed", "step_failed"),
            ("Step Failed Continued", "step_failed_continued"),
            ("Step Skipped", "step_skipped"),
            ("Condition Skipped", "step_condition_skipped"),
            ("Step Skipped Failure", "step_skipped_failure"),
        ):
            self.status_filter.addItem(label, value)
        self.refresh_button = make_button("Refresh", "secondary")

        filters.addWidget(self._labeled_field("Workflow", self.workflow_filter))
        filters.addWidget(self._labeled_field("Device", self.device_filter))
        filters.addWidget(self._labeled_field("Status", self.status_filter))
        filters.addWidget(self.refresh_button)
        card_layout.addLayout(filters)

        self.table = QtWidgets.QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(
            ["Time", "Level", "Workflow", "Device", "Status", "Message", "Metadata"]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        card_layout.addWidget(self.table)

        telemetry_title = QtWidgets.QLabel("Step Telemetry")
        telemetry_title.setObjectName("subtitleLabel")
        card_layout.addWidget(telemetry_title)

        self.telemetry_table = QtWidgets.QTableWidget(0, 8)
        self.telemetry_table.setHorizontalHeaderLabels(
            ["Step Type", "Workflow", "Device", "Success", "Failure", "Continued", "Skipped", "Failure %"]
        )
        self.telemetry_table.verticalHeader().setVisible(False)
        self.telemetry_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.telemetry_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.telemetry_table.horizontalHeader().setStretchLastSection(True)
        card_layout.addWidget(self.telemetry_table)
        root_layout.addWidget(card, 1)

        self.refresh_button.clicked.connect(self.load_logs)
        self.workflow_filter.currentIndexChanged.connect(self.load_logs)
        self.device_filter.currentIndexChanged.connect(self.load_logs)
        self.status_filter.currentIndexChanged.connect(self.load_logs)

    def _labeled_field(self, text: str, widget: QtWidgets.QWidget) -> QtWidgets.QWidget:
        container = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        layout.addWidget(make_form_label(text))
        layout.addWidget(widget)
        return container

    def refresh_filters(self) -> None:
        current_workflow = self.workflow_filter.currentData()
        current_device = self.device_filter.currentData()

        self.workflow_filter.clear()
        self.workflow_filter.addItem("All Workflows", None)
        for workflow in self.workflow_service.list_workflows():
            self.workflow_filter.addItem(workflow["name"], workflow["id"])

        self.device_filter.clear()
        self.device_filter.addItem("All Devices", None)
        for device in self.device_service.list_devices():
            self.device_filter.addItem(device["name"], device["id"])

        workflow_index = self.workflow_filter.findData(current_workflow)
        device_index = self.device_filter.findData(current_device)
        if workflow_index >= 0:
            self.workflow_filter.setCurrentIndex(workflow_index)
        if device_index >= 0:
            self.device_filter.setCurrentIndex(device_index)

    def load_logs(self) -> None:
        logs = self.log_service.list_logs(
            workflow_id=self.workflow_filter.currentData(),
            device_id=self.device_filter.currentData(),
            status=self.status_filter.currentData(),
            limit=500,
        )
        self.table.setRowCount(len(logs))
        for row_index, log in enumerate(logs):
            try:
                metadata_text = json.dumps(json.loads(log["metadata"]), indent=2, ensure_ascii=False)
            except Exception:
                metadata_text = str(log["metadata"])
            values = [
                log["created_at"],
                log["level"],
                log.get("workflow_name") or "-",
                log.get("device_name") or "-",
                log["status"],
                log["message"],
                metadata_text,
            ]
            for column, value in enumerate(values):
                self.table.setItem(row_index, column, QtWidgets.QTableWidgetItem(str(value)))
        self.table.resizeColumnsToContents()

        telemetry_rows = self.telemetry_service.summary(
            workflow_id=self.workflow_filter.currentData(),
            device_id=self.device_filter.currentData(),
            limit=10,
        )
        self.telemetry_table.setRowCount(len(telemetry_rows))
        for row_index, telemetry in enumerate(telemetry_rows):
            values = [
                telemetry["step_type"],
                telemetry.get("workflow_name") or "-",
                telemetry.get("device_name") or "-",
                telemetry["success_count"],
                telemetry["failure_count"],
                telemetry["continued_failure_count"],
                telemetry["skipped_count"],
                telemetry["failure_rate"],
            ]
            for column, value in enumerate(values):
                self.telemetry_table.setItem(row_index, column, QtWidgets.QTableWidgetItem(str(value)))
        self.telemetry_table.resizeColumnsToContents()
