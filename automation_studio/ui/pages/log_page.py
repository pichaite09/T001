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
        watcher_service,
        telemetry_service,
        watcher_telemetry_service,
        account_service,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.log_service = log_service
        self.workflow_service = workflow_service
        self.device_service = device_service
        self.watcher_service = watcher_service
        self.telemetry_service = telemetry_service
        self.watcher_telemetry_service = watcher_telemetry_service
        self.account_service = account_service
        self._build_ui()
        self.refresh_filters()
        self.load_logs()

    def _build_ui(self) -> None:
        root_layout = QtWidgets.QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(16)

        title = QtWidgets.QLabel("Log")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Review workflow history, step outcomes, watcher events, and execution context.")
        subtitle.setObjectName("subtitleLabel")
        root_layout.addWidget(title)
        root_layout.addWidget(subtitle)

        card = CardFrame()
        card_layout = QtWidgets.QVBoxLayout(card)
        card_layout.setContentsMargins(18, 18, 18, 18)
        card_layout.setSpacing(12)

        filters = QtWidgets.QGridLayout()
        filters.setHorizontalSpacing(12)
        filters.setVerticalSpacing(10)
        self.workflow_filter = QtWidgets.QComboBox()
        self.device_filter = QtWidgets.QComboBox()
        self.platform_filter = QtWidgets.QComboBox()
        self.account_filter = QtWidgets.QComboBox()
        self.watcher_filter = QtWidgets.QComboBox()
        self.status_filter = QtWidgets.QComboBox()
        self.status_filter.addItem("All Status", "all")
        for label, value in (
            ("Workflow Started", "workflow_started"),
            ("Workflow Success", "workflow_success"),
            ("Workflow Stopped", "workflow_stopped"),
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
            ("Watcher Matched", "watcher_matched"),
            ("Watcher Action Success", "watcher_action_success"),
            ("Watcher Action Failed", "watcher_action_failed"),
            ("Watcher Safety Stop", "watcher_safety_stop"),
        ):
            self.status_filter.addItem(label, value)
        self.refresh_button = make_button("Refresh", "secondary")

        fields = [
            ("Workflow", self.workflow_filter, 0, 0),
            ("Device", self.device_filter, 0, 1),
            ("Platform", self.platform_filter, 0, 2),
            ("Account", self.account_filter, 1, 0),
            ("Watcher", self.watcher_filter, 1, 1),
            ("Status", self.status_filter, 1, 2),
        ]
        for label, widget, row, column in fields:
            filters.addWidget(self._labeled_field(label, widget), row, column)
        filters.addWidget(self.refresh_button, 1, 3)
        card_layout.addLayout(filters)

        self.table = QtWidgets.QTableWidget(0, 10)
        self.table.setHorizontalHeaderLabels(
            ["Time", "Level", "Workflow", "Device", "Platform", "Account", "Watcher", "Status", "Message", "Metadata"]
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

        watcher_telemetry_title = QtWidgets.QLabel("Watcher Telemetry")
        watcher_telemetry_title.setObjectName("subtitleLabel")
        card_layout.addWidget(watcher_telemetry_title)

        self.watcher_telemetry_table = QtWidgets.QTableWidget(0, 9)
        self.watcher_telemetry_table.setHorizontalHeaderLabels(
            ["Watcher", "Workflow", "Device", "Triggers", "Success", "Failure", "Success %", "Failure %", "Last Error"]
        )
        self.watcher_telemetry_table.verticalHeader().setVisible(False)
        self.watcher_telemetry_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.watcher_telemetry_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.watcher_telemetry_table.horizontalHeader().setStretchLastSection(True)
        card_layout.addWidget(self.watcher_telemetry_table)
        root_layout.addWidget(card, 1)

        self.refresh_button.clicked.connect(self.load_logs)
        self.workflow_filter.currentIndexChanged.connect(self.load_logs)
        self.watcher_filter.currentIndexChanged.connect(self.load_logs)
        self.status_filter.currentIndexChanged.connect(self.load_logs)
        self.account_filter.currentIndexChanged.connect(self.load_logs)
        self.device_filter.currentIndexChanged.connect(self._on_device_filter_changed)
        self.platform_filter.currentIndexChanged.connect(self._on_platform_filter_changed)

    def _labeled_field(self, text: str, widget: QtWidgets.QWidget) -> QtWidgets.QWidget:
        container = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        layout.addWidget(make_form_label(text))
        layout.addWidget(widget)
        return container

    def _all_platforms(self) -> list[dict]:
        platforms: list[dict] = []
        for device in self.device_service.list_devices():
            platforms.extend(self.account_service.list_device_platforms(int(device["id"])))
        return platforms

    def _account_options(self, device_id: int | None, platform_key: str | None) -> list[dict]:
        platforms = (
            self.account_service.list_device_platforms(int(device_id))
            if device_id is not None
            else self._all_platforms()
        )
        rows: list[dict] = []
        for platform in platforms:
            if platform_key and str(platform.get("platform_key") or "").strip().lower() != platform_key:
                continue
            rows.extend(self.account_service.list_accounts(int(platform["id"])))
        return rows

    def refresh_filters(self) -> None:
        current_workflow = self.workflow_filter.currentData()
        current_device = self.device_filter.currentData()
        current_platform = self.platform_filter.currentData()
        current_account = self.account_filter.currentData()
        current_watcher = self.watcher_filter.currentData()

        combos = (
            self.workflow_filter,
            self.device_filter,
            self.platform_filter,
            self.account_filter,
            self.watcher_filter,
        )
        for combo in combos:
            combo.blockSignals(True)

        self.workflow_filter.clear()
        self.workflow_filter.addItem("All Workflows", None)
        for workflow in self.workflow_service.list_workflows():
            self.workflow_filter.addItem(workflow["name"], workflow["id"])

        self.device_filter.clear()
        self.device_filter.addItem("All Devices", None)
        for device in self.device_service.list_devices():
            self.device_filter.addItem(device["name"], device["id"])

        selected_device_id = current_device if current_device is not None else None
        self.platform_filter.clear()
        self.platform_filter.addItem("All Platforms", None)
        platform_labels: dict[str, str] = {}
        platforms = (
            self.account_service.list_device_platforms(int(selected_device_id))
            if selected_device_id is not None
            else self._all_platforms()
        )
        for platform in platforms:
            platform_key = str(platform.get("platform_key") or "").strip().lower()
            if not platform_key or platform_key in platform_labels:
                continue
            platform_labels[platform_key] = f"{platform.get('platform_name') or platform_key} ({platform_key})"
        for platform_key, label in sorted(platform_labels.items(), key=lambda item: item[1].casefold()):
            self.platform_filter.addItem(label, platform_key)

        selected_platform_key = current_platform if current_platform is not None else None
        self.account_filter.clear()
        self.account_filter.addItem("All Accounts", None)
        accounts = self._account_options(selected_device_id, selected_platform_key)
        seen_account_ids: set[int] = set()
        for account in accounts:
            account_id = int(account["id"])
            if account_id in seen_account_ids:
                continue
            seen_account_ids.add(account_id)
            label = f"{account['display_name']} ({account.get('platform_name') or account.get('platform_key') or 'Account'})"
            self.account_filter.addItem(label, account_id)

        self.watcher_filter.clear()
        self.watcher_filter.addItem("All Watchers", None)
        for watcher in self.watcher_service.list_watchers():
            self.watcher_filter.addItem(watcher["name"], watcher["id"])

        self._restore_combo_selection(self.workflow_filter, current_workflow)
        self._restore_combo_selection(self.device_filter, current_device)
        self._restore_combo_selection(self.platform_filter, current_platform)
        self._restore_combo_selection(self.account_filter, current_account)
        self._restore_combo_selection(self.watcher_filter, current_watcher)

        for combo in combos:
            combo.blockSignals(False)

    def _restore_combo_selection(self, combo: QtWidgets.QComboBox, data) -> None:
        index = combo.findData(data)
        combo.setCurrentIndex(index if index >= 0 else 0)

    def _on_device_filter_changed(self) -> None:
        self.refresh_filters()
        self.load_logs()

    def _on_platform_filter_changed(self) -> None:
        self.refresh_filters()
        self.load_logs()

    def load_logs(self) -> None:
        logs = self.log_service.list_logs(
            workflow_id=self.workflow_filter.currentData(),
            device_id=self.device_filter.currentData(),
            watcher_id=self.watcher_filter.currentData(),
            status=self.status_filter.currentData(),
            platform_key=self.platform_filter.currentData(),
            account_id=self.account_filter.currentData(),
            limit=500,
        )
        self.table.setRowCount(len(logs))
        for row_index, log in enumerate(logs):
            metadata = self._parse_metadata(log.get("metadata"))
            metadata_text = json.dumps(metadata, indent=2, ensure_ascii=False)
            values = [
                log["created_at"],
                log["level"],
                log.get("workflow_name") or "-",
                log.get("device_name") or "-",
                metadata.get("platform_name") or metadata.get("platform_key") or "-",
                metadata.get("account_name") or "-",
                log.get("watcher_name") or "-",
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

        watcher_telemetry_rows = self.watcher_telemetry_service.summary(
            workflow_id=self.workflow_filter.currentData(),
            device_id=self.device_filter.currentData(),
            limit=10,
        )
        self.watcher_telemetry_table.setRowCount(len(watcher_telemetry_rows))
        for row_index, telemetry in enumerate(watcher_telemetry_rows):
            values = [
                telemetry.get("watcher_name") or telemetry["watcher_id"],
                telemetry.get("workflow_name") or "-",
                telemetry.get("device_name") or "-",
                telemetry["trigger_count"],
                telemetry["success_count"],
                telemetry["failure_count"],
                telemetry["success_rate"],
                telemetry["failure_rate"],
                telemetry.get("last_error") or "-",
            ]
            for column, value in enumerate(values):
                self.watcher_telemetry_table.setItem(row_index, column, QtWidgets.QTableWidgetItem(str(value)))
        self.watcher_telemetry_table.resizeColumnsToContents()

    def _parse_metadata(self, payload: str | None) -> dict:
        try:
            return json.loads(payload or "{}")
        except Exception:
            return {"raw": payload or ""}
