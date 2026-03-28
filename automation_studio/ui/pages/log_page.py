from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from PySide6 import QtCore, QtGui, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class LogPage(QtWidgets.QWidget):
    SEARCH_LIMIT = 1000
    SESSION_TERMINAL_STATUSES = {
        "workflow_success",
        "workflow_failed",
        "workflow_stopped",
        "schedule_success",
        "schedule_failed",
        "schedule_skipped_overlap",
        "schedule_missed_skipped",
        "schedule_missed_rescheduled",
    }

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
        self._filtered_logs: list[dict[str, Any]] = []
        self._sessions: list[dict[str, Any]] = []
        self._session_lookup: dict[str, dict[str, Any]] = {}
        self._current_session_key: str | None = None
        self._current_step_telemetry: list[dict[str, Any]] = []
        self._current_watcher_telemetry: list[dict[str, Any]] = []
        self._build_ui()
        self.refresh_filters()
        self.load_logs()

    def _build_ui(self) -> None:
        root_layout = QtWidgets.QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(12)

        title = QtWidgets.QLabel("Log")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Review workflow history, group events into runs, and inspect detailed execution timelines.")
        subtitle.setObjectName("subtitleLabel")
        root_layout.addWidget(title)
        root_layout.addWidget(subtitle)

        card = CardFrame()
        card_layout = QtWidgets.QVBoxLayout(card)
        card_layout.setContentsMargins(14, 14, 14, 14)
        card_layout.setSpacing(8)
        root_layout.addWidget(card, 1)

        filters = QtWidgets.QGridLayout()
        filters.setHorizontalSpacing(10)
        filters.setVerticalSpacing(6)
        self.workflow_filter = QtWidgets.QComboBox()
        self.device_filter = QtWidgets.QComboBox()
        self.platform_filter = QtWidgets.QComboBox()
        self.account_filter = QtWidgets.QComboBox()
        self.watcher_filter = QtWidgets.QComboBox()
        self.status_filter = QtWidgets.QComboBox()
        self.status_filter.addItem("All Status", "all")
        for label, value in (
            ("Schedule Started", "schedule_started"),
            ("Schedule Success", "schedule_success"),
            ("Schedule Failed", "schedule_failed"),
            ("Schedule Retry", "schedule_retry"),
            ("Schedule Queued", "schedule_queued"),
            ("Schedule Skipped Overlap", "schedule_skipped_overlap"),
            ("Schedule Missed Skipped", "schedule_missed_skipped"),
            ("Schedule Missed Rescheduled", "schedule_missed_rescheduled"),
            ("Schedule Paused", "schedule_paused"),
            ("Schedule Resumed", "schedule_resumed"),
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

        quick_row = QtWidgets.QHBoxLayout()
        quick_row.setSpacing(6)
        quick_row.addWidget(make_form_label("Quick Filters"))
        self.errors_only_check = QtWidgets.QCheckBox("Errors Only")
        self.schedules_only_check = QtWidgets.QCheckBox("Schedules Only")
        self.steps_only_check = QtWidgets.QCheckBox("Steps Only")
        self.watchers_only_check = QtWidgets.QCheckBox("Watchers Only")
        self.today_only_check = QtWidgets.QCheckBox("Today")
        for widget in (
            self.errors_only_check,
            self.schedules_only_check,
            self.steps_only_check,
            self.watchers_only_check,
            self.today_only_check,
        ):
            quick_row.addWidget(widget)
        quick_row.addStretch(1)
        card_layout.addLayout(quick_row)

        search_row = QtWidgets.QHBoxLayout()
        search_row.setSpacing(6)
        search_row.addWidget(make_form_label("Search"))
        self.search_input = QtWidgets.QLineEdit()
        self.search_input.setPlaceholderText("Search message, status, workflow, device, platform, account, watcher, or metadata")
        self.clear_search_button = make_button("Clear", "secondary")
        self.result_count_label = QtWidgets.QLabel("0 events / 0 sessions")
        self.result_count_label.setObjectName("subtitleLabel")
        search_row.addWidget(self.search_input, 1)
        search_row.addWidget(self.clear_search_button)
        search_row.addWidget(self.result_count_label)
        card_layout.addLayout(search_row)

        metrics_layout = QtWidgets.QGridLayout()
        metrics_layout.setHorizontalSpacing(8)
        metrics_layout.setVerticalSpacing(8)
        self.metric_labels: dict[str, QtWidgets.QLabel] = {}
        metric_titles = [
            ("sessions_today", "Runs Today"),
            ("failed_today", "Failures Today"),
            ("avg_duration", "Avg Run"),
            ("watcher_runs", "Runs With Watchers"),
            ("top_step", "Top Failing Step"),
            ("top_schedule", "Top Failing Schedule"),
        ]
        for index, (key, title_text) in enumerate(metric_titles):
            card_widget = QtWidgets.QFrame()
            card_widget.setProperty("panel", True)
            metric_card_layout = QtWidgets.QVBoxLayout(card_widget)
            metric_card_layout.setContentsMargins(10, 8, 10, 8)
            metric_card_layout.setSpacing(4)
            label = QtWidgets.QLabel(title_text)
            label.setObjectName("subtitleLabel")
            value = QtWidgets.QLabel("-")
            value.setObjectName("titleLabel")
            value.setWordWrap(True)
            metric_card_layout.addWidget(label)
            metric_card_layout.addWidget(value)
            metrics_layout.addWidget(card_widget, 0, index)
            self.metric_labels[key] = value
        card_layout.addLayout(metrics_layout)

        self.tabs = QtWidgets.QTabWidget()
        card_layout.addWidget(self.tabs, 1)

        events_tab = QtWidgets.QWidget()
        events_layout = QtWidgets.QVBoxLayout(events_tab)
        events_layout.setContentsMargins(0, 0, 0, 0)
        events_layout.setSpacing(6)

        event_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Horizontal)
        event_splitter.setChildrenCollapsible(False)
        event_splitter.setHandleWidth(8)
        events_layout.addWidget(event_splitter, 1)

        session_panel = CardFrame()
        session_layout = QtWidgets.QVBoxLayout(session_panel)
        session_layout.setContentsMargins(12, 12, 12, 12)
        session_layout.setSpacing(6)
        session_layout.addWidget(make_form_label("Run Sessions"))

        self.session_summary_label = QtWidgets.QLabel("Select a session to inspect its event timeline.")
        self.session_summary_label.setObjectName("subtitleLabel")
        self.session_summary_label.setWordWrap(True)
        session_layout.addWidget(self.session_summary_label)

        self.failed_sessions_only_check = QtWidgets.QCheckBox("Failed Runs Only")
        self.watcher_sessions_only_check = QtWidgets.QCheckBox("Runs With Watchers")
        session_filter_row = QtWidgets.QHBoxLayout()
        session_filter_row.setSpacing(6)
        session_filter_row.addWidget(self.failed_sessions_only_check)
        session_filter_row.addWidget(self.watcher_sessions_only_check)
        session_layout.addLayout(session_filter_row)

        self.session_list = QtWidgets.QListWidget()
        session_layout.addWidget(self.session_list, 1)
        event_splitter.addWidget(session_panel)

        table_panel = QtWidgets.QWidget()
        table_layout = QtWidgets.QVBoxLayout(table_panel)
        table_layout.setContentsMargins(0, 0, 0, 0)
        table_layout.setSpacing(6)

        self.timeline_summary_label = QtWidgets.QLabel("Timeline")
        self.timeline_summary_label.setObjectName("subtitleLabel")
        self.timeline_summary_label.setWordWrap(True)
        table_layout.addWidget(self.timeline_summary_label)

        self.table = QtWidgets.QTableWidget(0, 11)
        self.table.setHorizontalHeaderLabels(
            ["Time", "Elapsed", "Level", "Workflow", "Device", "Context", "Watcher", "Status", "Message", "Duration", "Metadata"]
        )
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setWordWrap(False)
        table_layout.addWidget(self.table, 1)
        event_splitter.addWidget(table_panel)

        details_panel = CardFrame()
        details_layout = QtWidgets.QVBoxLayout(details_panel)
        details_layout.setContentsMargins(12, 12, 12, 12)
        details_layout.setSpacing(6)
        details_layout.addWidget(make_form_label("Event Details"))

        self.detail_summary_label = QtWidgets.QLabel("Select an event to inspect its execution context.")
        self.detail_summary_label.setObjectName("subtitleLabel")
        self.detail_summary_label.setWordWrap(True)
        details_layout.addWidget(self.detail_summary_label)

        self.detail_info_label = QtWidgets.QLabel("-")
        self.detail_info_label.setWordWrap(True)
        details_layout.addWidget(self.detail_info_label)

        details_layout.addWidget(make_form_label("Message"))
        self.detail_message = QtWidgets.QPlainTextEdit()
        self.detail_message.setReadOnly(True)
        self.detail_message.setMaximumBlockCount(300)
        self.detail_message.setMinimumHeight(72)
        details_layout.addWidget(self.detail_message)

        details_layout.addWidget(make_form_label("Metadata"))
        self.detail_metadata = QtWidgets.QPlainTextEdit()
        self.detail_metadata.setReadOnly(True)
        self.detail_metadata.setMaximumBlockCount(1000)
        self.detail_metadata.setMinimumHeight(120)
        details_layout.addWidget(self.detail_metadata, 1)

        details_layout.addWidget(make_form_label("Artifacts"))
        self.artifact_list = QtWidgets.QListWidget()
        self.artifact_list.setMinimumHeight(72)
        details_layout.addWidget(self.artifact_list)

        action_row = QtWidgets.QHBoxLayout()
        action_row.setSpacing(6)
        self.copy_message_button = make_button("Copy Event", "secondary")
        self.copy_metadata_button = make_button("Copy Metadata", "secondary")
        self.open_artifact_button = make_button("Open Artifact", "secondary")
        self.open_artifact_folder_button = make_button("Open Folder", "secondary")
        for button in (
            self.copy_message_button,
            self.copy_metadata_button,
            self.open_artifact_button,
            self.open_artifact_folder_button,
        ):
            action_row.addWidget(button)
        details_layout.addLayout(action_row)
        event_splitter.addWidget(details_panel)
        event_splitter.setStretchFactor(0, 2)
        event_splitter.setStretchFactor(1, 4)
        event_splitter.setStretchFactor(2, 3)

        telemetry_tab = QtWidgets.QWidget()
        telemetry_layout = QtWidgets.QVBoxLayout(telemetry_tab)
        telemetry_layout.setContentsMargins(0, 0, 0, 0)
        telemetry_layout.setSpacing(8)

        telemetry_layout.addWidget(make_form_label("Step Telemetry"))
        self.telemetry_table = QtWidgets.QTableWidget(0, 8)
        self.telemetry_table.setHorizontalHeaderLabels(
            ["Step Type", "Workflow", "Device", "Success", "Failure", "Continued", "Skipped", "Failure %"]
        )
        self.telemetry_table.verticalHeader().setVisible(False)
        self.telemetry_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.telemetry_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.telemetry_table.horizontalHeader().setStretchLastSection(True)
        telemetry_layout.addWidget(self.telemetry_table)

        telemetry_layout.addWidget(make_form_label("Watcher Telemetry"))
        self.watcher_telemetry_table = QtWidgets.QTableWidget(0, 9)
        self.watcher_telemetry_table.setHorizontalHeaderLabels(
            ["Watcher", "Workflow", "Device", "Triggers", "Success", "Failure", "Success %", "Failure %", "Last Error"]
        )
        self.watcher_telemetry_table.verticalHeader().setVisible(False)
        self.watcher_telemetry_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.watcher_telemetry_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.watcher_telemetry_table.horizontalHeader().setStretchLastSection(True)
        telemetry_layout.addWidget(self.watcher_telemetry_table)

        analytics_tab = QtWidgets.QWidget()
        analytics_layout = QtWidgets.QVBoxLayout(analytics_tab)
        analytics_layout.setContentsMargins(0, 0, 0, 0)
        analytics_layout.setSpacing(8)

        analytics_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        analytics_splitter.setChildrenCollapsible(False)
        analytics_splitter.setHandleWidth(8)
        analytics_layout.addWidget(analytics_splitter, 1)

        top_card = CardFrame()
        top_layout = QtWidgets.QVBoxLayout(top_card)
        top_layout.setContentsMargins(12, 12, 12, 12)
        top_layout.setSpacing(6)

        top_tables = QtWidgets.QHBoxLayout()
        top_tables.setSpacing(8)
        self.top_steps_table = QtWidgets.QTableWidget(0, 4)
        self.top_steps_table.setHorizontalHeaderLabels(["Step Type", "Failure %", "Failures", "Workflow"])
        self.top_watchers_table = QtWidgets.QTableWidget(0, 4)
        self.top_watchers_table.setHorizontalHeaderLabels(["Watcher", "Failure %", "Failures", "Triggers"])
        self.top_schedules_table = QtWidgets.QTableWidget(0, 4)
        self.top_schedules_table.setHorizontalHeaderLabels(["Schedule", "Failures", "Successes", "Fail %"])
        for title_text, table in (
            ("Top Failing Steps", self.top_steps_table),
            ("Top Failing Watchers", self.top_watchers_table),
            ("Top Failing Schedules", self.top_schedules_table),
        ):
            column = QtWidgets.QVBoxLayout()
            column.setSpacing(4)
            column.addWidget(make_form_label(title_text))
            table.verticalHeader().setVisible(False)
            table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
            table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
            table.horizontalHeader().setStretchLastSection(True)
            column.addWidget(table, 1)
            top_tables.addLayout(column, 1)
        top_layout.addLayout(top_tables)
        analytics_splitter.addWidget(top_card)

        bottom_card = CardFrame()
        bottom_layout = QtWidgets.QVBoxLayout(bottom_card)
        bottom_layout.setContentsMargins(12, 12, 12, 12)
        bottom_layout.setSpacing(6)

        bottom_tables = QtWidgets.QHBoxLayout()
        bottom_tables.setSpacing(8)
        self.failure_reasons_table = QtWidgets.QTableWidget(0, 3)
        self.failure_reasons_table.setHorizontalHeaderLabels(["Reason", "Count", "Latest Status"])
        self.slowest_sessions_table = QtWidgets.QTableWidget(0, 4)
        self.slowest_sessions_table.setHorizontalHeaderLabels(["Run", "Duration", "Status", "Device"])
        for title_text, table in (
            ("Recent Failure Reasons", self.failure_reasons_table),
            ("Slowest Runs", self.slowest_sessions_table),
        ):
            column = QtWidgets.QVBoxLayout()
            column.setSpacing(4)
            column.addWidget(make_form_label(title_text))
            table.verticalHeader().setVisible(False)
            table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
            table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
            table.horizontalHeader().setStretchLastSection(True)
            column.addWidget(table, 1)
            bottom_tables.addLayout(column, 1)
        bottom_layout.addLayout(bottom_tables)
        analytics_splitter.addWidget(bottom_card)

        self.tabs.addTab(events_tab, "Events")
        self.tabs.addTab(telemetry_tab, "Telemetry")
        self.tabs.addTab(analytics_tab, "Analytics")

        self.refresh_button.clicked.connect(self.load_logs)
        self.clear_search_button.clicked.connect(self._clear_search)
        self.workflow_filter.currentIndexChanged.connect(self.load_logs)
        self.watcher_filter.currentIndexChanged.connect(self.load_logs)
        self.status_filter.currentIndexChanged.connect(self.load_logs)
        self.account_filter.currentIndexChanged.connect(self.load_logs)
        self.device_filter.currentIndexChanged.connect(self._on_device_filter_changed)
        self.platform_filter.currentIndexChanged.connect(self._on_platform_filter_changed)
        self.search_input.textChanged.connect(self.load_logs)
        self.session_list.itemSelectionChanged.connect(self._on_session_selected)
        self.table.itemSelectionChanged.connect(self._on_log_selected)
        self.artifact_list.itemDoubleClicked.connect(lambda *_: self._open_selected_artifact())
        self.copy_message_button.clicked.connect(self._copy_selected_event)
        self.copy_metadata_button.clicked.connect(self._copy_selected_metadata)
        self.open_artifact_button.clicked.connect(self._open_selected_artifact)
        self.open_artifact_folder_button.clicked.connect(self._open_selected_artifact_folder)
        for widget in (
            self.errors_only_check,
            self.schedules_only_check,
            self.steps_only_check,
            self.watchers_only_check,
            self.today_only_check,
            self.failed_sessions_only_check,
            self.watcher_sessions_only_check,
        ):
            widget.toggled.connect(self.load_logs)
        self._clear_detail_panel()

    def _labeled_field(self, text: str, widget: QtWidgets.QWidget) -> QtWidgets.QWidget:
        container = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
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

    def _clear_search(self) -> None:
        self.search_input.clear()

    def load_logs(self) -> None:
        raw_logs = self.log_service.list_logs(
            workflow_id=self.workflow_filter.currentData(),
            device_id=self.device_filter.currentData(),
            watcher_id=self.watcher_filter.currentData(),
            status=self.status_filter.currentData(),
            platform_key=self.platform_filter.currentData(),
            account_id=self.account_filter.currentData(),
            limit=self.SEARCH_LIMIT,
        )
        self._filtered_logs = self._apply_client_filters(raw_logs)
        self._sessions = self._build_sessions(self._filtered_logs)
        self._session_lookup = {session["key"]: session for session in self._sessions}
        self.result_count_label.setText(f"{len(self._filtered_logs)} events / {len(self._sessions)} sessions")

        self._load_telemetry_tables()
        self._update_analytics()
        self._populate_session_list()

    def _load_telemetry_tables(self) -> None:
        telemetry_rows = self.telemetry_service.summary(
            workflow_id=self.workflow_filter.currentData(),
            device_id=self.device_filter.currentData(),
            limit=10,
        )
        self._current_step_telemetry = telemetry_rows
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
        self._current_watcher_telemetry = watcher_telemetry_rows
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

    def _apply_client_filters(self, logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        search_text = self.search_input.text().strip().casefold()
        selected_categories = {
            name
            for name, enabled in (
                ("schedule", self.schedules_only_check.isChecked()),
                ("step", self.steps_only_check.isChecked()),
                ("watcher", self.watchers_only_check.isChecked()),
            )
            if enabled
        }
        today_text = datetime.now().astimezone().strftime("%Y-%m-%d")

        filtered: list[dict[str, Any]] = []
        for log in logs:
            metadata = self._parse_metadata(log.get("metadata"))
            log["_parsed_metadata"] = metadata
            status = str(log.get("status") or "")
            level = str(log.get("level") or "")

            if self.today_only_check.isChecked() and not str(log.get("created_at") or "").startswith(today_text):
                continue
            if self.errors_only_check.isChecked() and not self._is_error_event(level, status):
                continue
            if selected_categories and self._event_category(status) not in selected_categories:
                continue
            if search_text and not self._matches_search(log, metadata, search_text):
                continue
            filtered.append(log)
        return filtered

    def _matches_search(self, log: dict[str, Any], metadata: dict[str, Any], search_text: str) -> bool:
        haystack = " ".join(
            [
                str(log.get("created_at") or ""),
                str(log.get("level") or ""),
                str(log.get("workflow_name") or ""),
                str(log.get("device_name") or ""),
                str(log.get("watcher_name") or ""),
                str(log.get("status") or ""),
                str(log.get("message") or ""),
                str(metadata.get("platform_name") or metadata.get("platform_key") or ""),
                str(metadata.get("account_name") or ""),
                json.dumps(metadata, ensure_ascii=False),
            ]
        ).casefold()
        return search_text in haystack

    def _build_sessions(self, logs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        sorted_logs = sorted(logs, key=lambda item: (str(item.get("created_at") or ""), int(item.get("id") or 0)))
        sessions: dict[str, dict[str, Any]] = {}
        active_schedule_sessions: dict[int, str] = {}
        schedule_sequence = 0

        for log in sorted_logs:
            metadata = log.get("_parsed_metadata") or self._parse_metadata(log.get("metadata"))
            status = str(log.get("status") or "")
            session_key: str
            session_type: str

            run_id = str(metadata.get("run_id") or "").strip()
            schedule_id = int(metadata.get("schedule_id") or 0) or None
            if run_id:
                session_key = f"run:{run_id}"
                session_type = "workflow"
            elif status.startswith("schedule_") and schedule_id is not None:
                session_type = "schedule"
                if status == "schedule_started":
                    schedule_sequence += 1
                    session_key = f"schedule:{schedule_id}:{schedule_sequence}"
                    active_schedule_sessions[schedule_id] = session_key
                else:
                    session_key = active_schedule_sessions.get(schedule_id, f"schedule:{schedule_id}:{int(log.get('id') or 0)}")
            else:
                session_key = f"log:{int(log.get('id') or 0)}"
                session_type = "event"

            session = sessions.get(session_key)
            if session is None:
                session = self._new_session(session_key, session_type, log, metadata)
                sessions[session_key] = session

            parsed_time = self._parse_log_time(str(log.get("created_at") or ""))
            event = dict(log)
            event["_parsed_time"] = parsed_time
            event["_elapsed_ms"] = 0
            session["events"].append(event)
            session["event_count"] += 1
            session["watcher_event_count"] += 1 if status.startswith("watcher_") else 0
            session["error_count"] += 1 if self._is_error_event(str(log.get("level") or ""), status) else 0
            duration_ms = int(metadata.get("duration_ms") or 0)
            if duration_ms > 0:
                session["recorded_duration_ms"] += duration_ms
            session["start_time"] = parsed_time if session["start_time"] is None or parsed_time < session["start_time"] else session["start_time"]
            session["end_time"] = parsed_time if session["end_time"] is None or parsed_time > session["end_time"] else session["end_time"]
            session["last_status"] = status or session["last_status"]
            session["has_watchers"] = bool(session["has_watchers"] or status.startswith("watcher_"))
            session["artifact_paths"].extend(self._extract_artifact_paths(metadata))
            self._update_session_title(session, log, metadata)

            if schedule_id is not None and status in self.SESSION_TERMINAL_STATUSES and schedule_id in active_schedule_sessions:
                active_schedule_sessions.pop(schedule_id, None)

        session_rows = []
        for session in sessions.values():
            self._finalize_session(session)
            if self.failed_sessions_only_check.isChecked() and not session["is_failed"]:
                continue
            if self.watcher_sessions_only_check.isChecked() and not session["has_watchers"]:
                continue
            session_rows.append(session)
        session_rows.sort(
            key=lambda item: (
                item["start_time"] or datetime.min.astimezone(),
                item["title"].casefold(),
            ),
            reverse=True,
        )
        return session_rows

    def _new_session(self, session_key: str, session_type: str, log: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
        return {
            "key": session_key,
            "type": session_type,
            "title": str(log.get("workflow_name") or log.get("status") or "Session"),
            "subtitle": "",
            "start_time": None,
            "end_time": None,
            "last_status": str(log.get("status") or ""),
            "events": [],
            "event_count": 0,
            "watcher_event_count": 0,
            "error_count": 0,
            "recorded_duration_ms": 0,
            "has_watchers": False,
            "is_failed": False,
            "artifact_paths": [],
            "workflow_name": str(log.get("workflow_name") or "-"),
            "device_name": str(log.get("device_name") or "-"),
            "platform_name": str(metadata.get("platform_name") or metadata.get("platform_key") or "-"),
            "account_name": str(metadata.get("account_name") or "-"),
            "run_id": str(metadata.get("run_id") or ""),
            "schedule_id": int(metadata.get("schedule_id") or 0) or None,
        }

    def _update_session_title(self, session: dict[str, Any], log: dict[str, Any], metadata: dict[str, Any]) -> None:
        status = str(log.get("status") or "")
        if session["type"] == "schedule":
            schedule_name = str(metadata.get("schedule_name") or log.get("workflow_name") or "Schedule")
            session["title"] = schedule_name
            session["subtitle"] = f"{metadata.get('trigger_source') or 'timer'} / {log.get('device_name') or '-'}"
            return
        if session["type"] == "workflow":
            execution_scope = str(metadata.get("execution_scope") or "workflow")
            session["title"] = str(log.get("workflow_name") or "Workflow")
            session["subtitle"] = f"{execution_scope} / {log.get('device_name') or '-'}"
            if status.startswith("watcher_") and metadata.get("watcher_name"):
                session["subtitle"] = f"{session['subtitle']} / watcher activity"
            return
        session["title"] = str(log.get("status") or "Event")
        session["subtitle"] = str(log.get("device_name") or "-")

    def _finalize_session(self, session: dict[str, Any]) -> None:
        start_time = session.get("start_time")
        if start_time is None:
            return
        artifact_paths = []
        seen_artifacts: set[str] = set()
        for path in session["artifact_paths"]:
            if str(path) in seen_artifacts:
                continue
            seen_artifacts.add(str(path))
            artifact_paths.append(path)
        session["artifact_paths"] = artifact_paths

        for event in session["events"]:
            event["_elapsed_ms"] = int((event["_parsed_time"] - start_time).total_seconds() * 1000)
        end_time = session.get("end_time") or start_time
        session["elapsed_ms"] = int((end_time - start_time).total_seconds() * 1000)
        session["display_duration_ms"] = session["recorded_duration_ms"] or session["elapsed_ms"]
        last_status = str(session.get("last_status") or "")
        session["is_failed"] = "failed" in last_status or session["error_count"] > 0 or "validation" in last_status
        status_label = last_status.replace("_", " ").title() if last_status else "Session"
        duration_text = self._format_duration_ms(session["display_duration_ms"])
        watcher_text = f" / {session['watcher_event_count']} watcher events" if session["watcher_event_count"] else ""
        session["display_text"] = (
            f"{session['title']}\n{status_label} / {duration_text} / {session['event_count']} events{watcher_text}"
        )

    def _populate_session_list(self) -> None:
        previous_key = self._current_session_key
        self.session_list.blockSignals(True)
        self.session_list.clear()
        for session in self._sessions:
            item = QtWidgets.QListWidgetItem(str(session["display_text"]))
            item.setData(QtCore.Qt.ItemDataRole.UserRole, session["key"])
            if session["is_failed"]:
                item.setForeground(QtGui.QColor("#fca5a5"))
            elif session["has_watchers"]:
                item.setForeground(QtGui.QColor("#93c5fd"))
            self.session_list.addItem(item)
        self.session_list.blockSignals(False)

        if not self._sessions:
            self._current_session_key = None
            self.session_summary_label.setText("No sessions match the current filters.")
            self.timeline_summary_label.setText("Timeline")
            self.table.setRowCount(0)
            self._clear_detail_panel()
            return

        target_key = previous_key if previous_key in self._session_lookup else self._sessions[0]["key"]
        for row in range(self.session_list.count()):
            item = self.session_list.item(row)
            if item and item.data(QtCore.Qt.ItemDataRole.UserRole) == target_key:
                self.session_list.setCurrentRow(row)
                break

    def _update_analytics(self) -> None:
        today_text = datetime.now().astimezone().strftime("%Y-%m-%d")
        sessions_today = [session for session in self._sessions if session.get("start_time") and session["start_time"].strftime("%Y-%m-%d") == today_text]
        failed_today = [session for session in sessions_today if session.get("is_failed")]
        watcher_runs = [session for session in self._sessions if session.get("has_watchers")]
        avg_duration_ms = int(sum(int(session.get("display_duration_ms") or 0) for session in self._sessions) / len(self._sessions)) if self._sessions else 0
        top_step = self._current_step_telemetry[0]["step_type"] if self._current_step_telemetry else "-"

        schedule_stats: dict[str, dict[str, Any]] = {}
        failure_reasons: dict[str, dict[str, Any]] = {}
        for log in self._filtered_logs:
            metadata = log.get("_parsed_metadata") or self._parse_metadata(log.get("metadata"))
            status = str(log.get("status") or "")
            schedule_name = str(metadata.get("schedule_name") or "")
            if status in {"schedule_success", "schedule_failed"} and schedule_name:
                row = schedule_stats.setdefault(schedule_name, {"success": 0, "failure": 0})
                if status == "schedule_success":
                    row["success"] += 1
                else:
                    row["failure"] += 1
            if self._is_error_event(str(log.get("level") or ""), status):
                reason = str(log.get("message") or "").strip() or status
                reason = reason if len(reason) <= 80 else reason[:77] + "..."
                row = failure_reasons.setdefault(reason, {"count": 0, "status": status})
                row["count"] += 1
                row["status"] = status

        top_schedule_name = "-"
        if schedule_stats:
            top_schedule_name = max(
                schedule_stats.items(),
                key=lambda item: (int(item[1]["failure"]), -int(item[1]["success"]), item[0].casefold()),
            )[0]

        self.metric_labels["sessions_today"].setText(str(len(sessions_today)))
        self.metric_labels["failed_today"].setText(str(len(failed_today)))
        self.metric_labels["avg_duration"].setText(self._format_duration_ms(avg_duration_ms))
        self.metric_labels["watcher_runs"].setText(str(len(watcher_runs)))
        self.metric_labels["top_step"].setText(str(top_step))
        self.metric_labels["top_schedule"].setText(str(top_schedule_name))

        self._fill_table(
            self.top_steps_table,
            [
                [
                    row["step_type"],
                    row["failure_rate"],
                    int(row["failure_count"]) + int(row["continued_failure_count"]),
                    row.get("workflow_name") or "-",
                ]
                for row in self._current_step_telemetry[:8]
            ],
        )
        self._fill_table(
            self.top_watchers_table,
            [
                [
                    row.get("watcher_name") or row["watcher_id"],
                    row["failure_rate"],
                    row["failure_count"],
                    row["trigger_count"],
                ]
                for row in self._current_watcher_telemetry[:8]
            ],
        )

        schedule_rows = []
        for schedule_name, counts in sorted(
            schedule_stats.items(),
            key=lambda item: (int(item[1]["failure"]), -int(item[1]["success"]), item[0].casefold()),
            reverse=True,
        )[:8]:
            total = int(counts["success"]) + int(counts["failure"])
            failure_rate = round((int(counts["failure"]) * 100.0 / total), 2) if total else 0
            schedule_rows.append([schedule_name, counts["failure"], counts["success"], failure_rate])
        self._fill_table(self.top_schedules_table, schedule_rows)

        failure_rows = [
            [reason, info["count"], info["status"]]
            for reason, info in sorted(
                failure_reasons.items(),
                key=lambda item: (int(item[1]["count"]), item[0].casefold()),
                reverse=True,
            )[:10]
        ]
        self._fill_table(self.failure_reasons_table, failure_rows)

        slowest_rows = [
            [
                session["title"],
                self._format_duration_ms(int(session.get("display_duration_ms") or 0)),
                session.get("last_status") or "-",
                session.get("device_name") or "-",
            ]
            for session in sorted(
                self._sessions,
                key=lambda item: int(item.get("display_duration_ms") or 0),
                reverse=True,
            )[:10]
        ]
        self._fill_table(self.slowest_sessions_table, slowest_rows)

    def _fill_table(self, table: QtWidgets.QTableWidget, rows: list[list[Any]]) -> None:
        table.setRowCount(len(rows))
        for row_index, values in enumerate(rows):
            for column, value in enumerate(values):
                table.setItem(row_index, column, QtWidgets.QTableWidgetItem(str(value)))
        table.resizeColumnsToContents()

    def _on_session_selected(self) -> None:
        item = self.session_list.currentItem()
        if item is None:
            self._current_session_key = None
            self.table.setRowCount(0)
            self._clear_detail_panel()
            return
        self._current_session_key = str(item.data(QtCore.Qt.ItemDataRole.UserRole))
        session = self._session_lookup.get(self._current_session_key)
        if not session:
            return

        duration_text = self._format_duration_ms(int(session.get("display_duration_ms") or 0))
        self.session_summary_label.setText(
            f"{session['title']} / {session['last_status'] or 'session'} / {duration_text} / {session['event_count']} events"
        )
        self.timeline_summary_label.setText(
            f"{session['workflow_name']} on {session['device_name']} / {session['platform_name']} / {session['account_name']}"
        )

        self.table.setRowCount(len(session["events"]))
        for row_index, log in enumerate(session["events"]):
            metadata = log.get("_parsed_metadata") or self._parse_metadata(log.get("metadata"))
            values = [
                str(log.get("created_at") or ""),
                self._format_duration_ms(int(log.get("_elapsed_ms") or 0)),
                str(log.get("level") or ""),
                str(log.get("workflow_name") or "-"),
                str(log.get("device_name") or "-"),
                self._context_label(log, metadata),
                str(log.get("watcher_name") or metadata.get("watcher_name") or "-"),
                str(log.get("status") or ""),
                str(log.get("message") or ""),
                self._format_duration_ms(int(metadata.get("duration_ms") or 0)),
                self._metadata_preview(metadata),
            ]
            for column, value in enumerate(values):
                item_value = QtWidgets.QTableWidgetItem(str(value))
                self.table.setItem(row_index, column, item_value)
            self._style_log_row(row_index, log, metadata)
        self.table.resizeColumnsToContents()
        self.table.setColumnWidth(8, min(max(self.table.columnWidth(8), 280), 500))
        self.table.setColumnWidth(10, min(max(self.table.columnWidth(10), 280), 520))
        if session["events"]:
            self.table.selectRow(0)

    def _context_label(self, log: dict[str, Any], metadata: dict[str, Any]) -> str:
        step_name = str(metadata.get("step_name") or "")
        position = metadata.get("position")
        if step_name:
            prefix = f"{position}. " if position is not None else ""
            return f"{prefix}{step_name}"
        schedule_name = str(metadata.get("schedule_name") or "")
        if schedule_name:
            return schedule_name
        execution_scope = str(metadata.get("execution_scope") or "")
        if execution_scope:
            return execution_scope
        stage = str(metadata.get("stage") or "")
        if stage:
            return stage
        return str(log.get("status") or "-")

    def _event_category(self, status: str) -> str:
        if status.startswith("schedule_"):
            return "schedule"
        if status.startswith("watcher_"):
            return "watcher"
        return "step"

    def _is_error_event(self, level: str, status: str) -> bool:
        normalized_status = status.casefold()
        normalized_level = level.casefold()
        return (
            normalized_level in {"error", "warning"}
            or "failed" in normalized_status
            or "validation" in normalized_status
            or "safety_stop" in normalized_status
        )

    def _style_log_row(self, row_index: int, log: dict[str, Any], metadata: dict[str, Any]) -> None:
        level = str(log.get("level") or "")
        status = str(log.get("status") or "")
        if self._is_error_event(level, status):
            foreground = QtGui.QColor("#fca5a5")
            background = QtGui.QColor("#3a1620")
        elif "success" in status or status in {"workflow_stopped", "schedule_resumed"}:
            foreground = QtGui.QColor("#86efac")
            background = QtGui.QColor("#0f2f23")
        elif "retry" in status or "queued" in status or "paused" in status or "skipped" in status:
            foreground = QtGui.QColor("#fde68a")
            background = QtGui.QColor("#3a2b10")
        else:
            foreground = QtGui.QColor("#dbe7ff")
            background = QtGui.QColor("#14233b")
        for column in range(self.table.columnCount()):
            item = self.table.item(row_index, column)
            if item is None:
                continue
            item.setForeground(foreground)
            if column in {2, 7}:
                item.setBackground(background)
        if str(log.get("watcher_name") or metadata.get("watcher_name") or ""):
            watcher_item = self.table.item(row_index, 6)
            if watcher_item is not None:
                watcher_item.setForeground(QtGui.QColor("#93c5fd"))
        metadata_item = self.table.item(row_index, 10)
        if metadata_item is not None and self._extract_artifact_paths(metadata):
            metadata_item.setForeground(QtGui.QColor("#c4b5fd"))

    def _on_log_selected(self) -> None:
        session = self._session_lookup.get(self._current_session_key or "")
        row = self.table.currentRow()
        if not session or row < 0 or row >= len(session["events"]):
            self._clear_detail_panel()
            return
        log = session["events"][row]
        metadata = log.get("_parsed_metadata") or self._parse_metadata(log.get("metadata"))
        artifacts = self._extract_artifact_paths(metadata)

        self.detail_summary_label.setText(
            f"{log.get('status') or '-'} / {log.get('workflow_name') or '-'} / {log.get('device_name') or '-'}"
        )
        info_lines = [
            f"Time: {log.get('created_at') or '-'}",
            f"Elapsed: {self._format_duration_ms(int(log.get('_elapsed_ms') or 0))}",
            f"Level: {log.get('level') or '-'}",
            f"Watcher: {log.get('watcher_name') or metadata.get('watcher_name') or '-'}",
            f"Platform: {metadata.get('platform_name') or metadata.get('platform_key') or '-'}",
            f"Account: {metadata.get('account_name') or '-'}",
        ]
        self.detail_info_label.setText("\n".join(info_lines))
        self.detail_message.setPlainText(str(log.get("message") or ""))
        self.detail_metadata.setPlainText(json.dumps(metadata, indent=2, ensure_ascii=False))
        self.artifact_list.clear()
        for artifact in artifacts:
            item = QtWidgets.QListWidgetItem(str(artifact))
            item.setData(QtCore.Qt.ItemDataRole.UserRole, str(artifact))
            self.artifact_list.addItem(item)
        if self.artifact_list.count() > 0:
            self.artifact_list.setCurrentRow(0)
        self.open_artifact_button.setEnabled(self.artifact_list.count() > 0)
        self.open_artifact_folder_button.setEnabled(self.artifact_list.count() > 0)

    def _clear_detail_panel(self) -> None:
        self.detail_summary_label.setText("Select an event to inspect its execution context.")
        self.detail_info_label.setText("-")
        self.detail_message.clear()
        self.detail_metadata.clear()
        self.artifact_list.clear()
        self.open_artifact_button.setEnabled(False)
        self.open_artifact_folder_button.setEnabled(False)

    def _copy_selected_event(self) -> None:
        session = self._session_lookup.get(self._current_session_key or "")
        row = self.table.currentRow()
        if not session or row < 0 or row >= len(session["events"]):
            return
        log = session["events"][row]
        metadata = log.get("_parsed_metadata") or self._parse_metadata(log.get("metadata"))
        payload = {
            "time": log.get("created_at"),
            "elapsed_ms": int(log.get("_elapsed_ms") or 0),
            "level": log.get("level"),
            "workflow": log.get("workflow_name"),
            "device": log.get("device_name"),
            "watcher": log.get("watcher_name"),
            "status": log.get("status"),
            "message": log.get("message"),
            "metadata": metadata,
        }
        QtWidgets.QApplication.clipboard().setText(json.dumps(payload, indent=2, ensure_ascii=False))

    def _copy_selected_metadata(self) -> None:
        session = self._session_lookup.get(self._current_session_key or "")
        row = self.table.currentRow()
        if not session or row < 0 or row >= len(session["events"]):
            return
        log = session["events"][row]
        metadata = log.get("_parsed_metadata") or self._parse_metadata(log.get("metadata"))
        QtWidgets.QApplication.clipboard().setText(json.dumps(metadata, indent=2, ensure_ascii=False))

    def _selected_artifact_path(self) -> Path | None:
        item = self.artifact_list.currentItem()
        if item is None:
            return None
        return Path(str(item.data(QtCore.Qt.ItemDataRole.UserRole)))

    def _open_selected_artifact(self) -> None:
        artifact_path = self._selected_artifact_path()
        if artifact_path is None or not artifact_path.exists():
            return
        QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(artifact_path)))

    def _open_selected_artifact_folder(self) -> None:
        artifact_path = self._selected_artifact_path()
        if artifact_path is None:
            return
        folder = artifact_path.parent if artifact_path.exists() else artifact_path
        if folder.exists():
            QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(str(folder)))

    def _extract_artifact_paths(self, metadata: dict[str, Any]) -> list[Path]:
        discovered: list[Path] = []
        seen: set[str] = set()

        def visit(value: Any, key: str = "") -> None:
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    visit(nested_value, str(nested_key))
                return
            if isinstance(value, list):
                for nested_value in value:
                    visit(nested_value, key)
                return
            if not isinstance(value, str):
                return
            if not value.strip():
                return
            normalized_key = key.casefold()
            looks_like_path = any(token in normalized_key for token in ("path", "file", "artifact", "screenshot", "hierarchy"))
            candidate = Path(value)
            if not candidate.is_absolute():
                candidate = Path.cwd() / candidate
            if "artifacts" in value.replace("\\", "/") or looks_like_path or candidate.exists():
                resolved = candidate.resolve(strict=False)
                if str(resolved) not in seen:
                    seen.add(str(resolved))
                    discovered.append(resolved)

        visit(metadata)
        return discovered

    def _metadata_preview(self, metadata: dict[str, Any]) -> str:
        compact = json.dumps(metadata, ensure_ascii=False, separators=(", ", ": "))
        return compact if len(compact) <= 140 else compact[:137] + "..."

    def _parse_metadata(self, payload: str | None) -> dict:
        try:
            return json.loads(payload or "{}")
        except Exception:
            return {"raw": payload or ""}

    def _parse_log_time(self, value: str) -> datetime:
        timezone = datetime.now().astimezone().tzinfo
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone)

    def _format_duration_ms(self, value: int) -> str:
        normalized = max(int(value or 0), 0)
        if normalized < 1000:
            return f"{normalized} ms"
        seconds = normalized / 1000
        if seconds < 60:
            return f"{seconds:.1f}s"
        minutes = int(seconds // 60)
        remaining = int(seconds % 60)
        return f"{minutes}m {remaining:02d}s"
