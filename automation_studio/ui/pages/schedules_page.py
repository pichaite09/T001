from __future__ import annotations

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class ScheduleRunThread(QtCore.QThread):
    result_ready = QtCore.Signal(int, dict)

    def __init__(
        self,
        scheduler_service,
        schedule_id: int,
        *,
        trigger_source: str,
        advance_schedule: bool,
        runtime_task_id: str | None = None,
    ) -> None:
        super().__init__()
        self.scheduler_service = scheduler_service
        self.schedule_id = schedule_id
        self.trigger_source = trigger_source
        self.advance_schedule = advance_schedule
        self.runtime_task_id = runtime_task_id

    def run(self) -> None:
        result = self.scheduler_service.execute_schedule(
            self.schedule_id,
            trigger_source=self.trigger_source,
            advance_schedule=self.advance_schedule,
            runtime_task_id=self.runtime_task_id,
        )
        self.result_ready.emit(self.schedule_id, result)


class ScheduleGroupEditorDialog(QtWidgets.QDialog):
    def __init__(self, group: dict | None = None, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.group = group or {}
        self.setWindowTitle("Edit Schedule Group" if group else "New Schedule Group")
        self.resize(420, 240)
        self.setModal(True)

        root = QtWidgets.QVBoxLayout(self)
        form = QtWidgets.QFormLayout()

        self.name_input = QtWidgets.QLineEdit(str(self.group.get("name") or ""))
        self.description_input = QtWidgets.QPlainTextEdit(str(self.group.get("description") or ""))
        self.description_input.setFixedHeight(96)
        self.enabled_check = QtWidgets.QCheckBox("Enabled")
        self.enabled_check.setChecked(bool(self.group.get("is_enabled", 1)))

        form.addRow("Name", self.name_input)
        form.addRow("Description", self.description_input)
        form.addRow("", self.enabled_check)
        root.addLayout(form)

        buttons = QtWidgets.QDialogButtonBox()
        buttons.addButton("Cancel", QtWidgets.QDialogButtonBox.ButtonRole.RejectRole)
        save_button = buttons.addButton("Save Group", QtWidgets.QDialogButtonBox.ButtonRole.AcceptRole)
        root.addWidget(buttons)

        buttons.rejected.connect(self.reject)
        save_button.clicked.connect(self._save_and_accept)

    def _save_and_accept(self) -> None:
        if not self.name_input.text().strip():
            QtWidgets.QMessageBox.warning(self, "Schedule Groups", "Group name is required.")
            return
        self.accept()

    def payload(self) -> dict:
        return {
            "group_id": int(self.group.get("id") or 0) or None,
            "name": self.name_input.text().strip(),
            "description": self.description_input.toPlainText().strip(),
            "is_enabled": self.enabled_check.isChecked(),
        }


class ScheduleGroupManagerDialog(QtWidgets.QDialog):
    groups_changed = QtCore.Signal()

    def __init__(self, scheduler_service, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.scheduler_service = scheduler_service
        self.current_group_id: int | None = None
        self.setWindowTitle("Manage Schedule Groups")
        self.resize(760, 420)
        self.setModal(True)
        self._build_ui()
        self.load_groups()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        toolbar = QtWidgets.QHBoxLayout()
        self.new_button = make_button("New Group", "secondary")
        self.edit_button = make_button("Edit Selected", "secondary")
        self.toggle_button = make_button("Pause", "secondary")
        self.delete_button = make_button("Delete", "danger")
        self.refresh_button = make_button("Refresh", "secondary")
        for button in (self.new_button, self.edit_button, self.toggle_button, self.delete_button, self.refresh_button):
            toolbar.addWidget(button)
        toolbar.addStretch(1)
        root.addLayout(toolbar)

        self.group_table = QtWidgets.QTableWidget(0, 5)
        self.group_table.setHorizontalHeaderLabels(["ID", "Name", "State", "Schedules", "Description"])
        self.group_table.setColumnHidden(0, True)
        self.group_table.verticalHeader().setVisible(False)
        self.group_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.group_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.group_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.group_table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self.group_table, 1)

        self.status_label = QtWidgets.QLabel("Groups let you pause, organize, and monitor related schedules together.")
        self.status_label.setObjectName("subtitleLabel")
        self.status_label.setWordWrap(True)
        root.addWidget(self.status_label)

        self.new_button.clicked.connect(lambda: self._open_editor(None))
        self.edit_button.clicked.connect(self._edit_selected)
        self.toggle_button.clicked.connect(self._toggle_selected)
        self.delete_button.clicked.connect(self._delete_selected)
        self.refresh_button.clicked.connect(self.load_groups)
        self.group_table.itemSelectionChanged.connect(self._on_selected)
        self.group_table.itemDoubleClicked.connect(lambda *_: self._edit_selected())
        self._sync_action_state()

    def load_groups(self) -> None:
        groups = self.scheduler_service.list_groups()
        self.group_table.setRowCount(len(groups))
        for row, group in enumerate(groups):
            values = [
                int(group["id"]),
                str(group.get("name") or ""),
                "Enabled" if bool(group.get("is_enabled", 1)) else "Paused",
                f"{int(group.get('enabled_schedule_count') or 0)}/{int(group.get('schedule_count') or 0)}",
                str(group.get("description") or ""),
            ]
            for column, value in enumerate(values):
                self.group_table.setItem(row, column, QtWidgets.QTableWidgetItem(str(value)))
        self.group_table.resizeColumnsToContents()
        self._sync_action_state()

    def _selected_group(self) -> dict | None:
        if self.current_group_id is None:
            return None
        return self.scheduler_service.get_group(int(self.current_group_id))

    def _on_selected(self) -> None:
        row = self.group_table.currentRow()
        self.current_group_id = int(self.group_table.item(row, 0).text()) if row >= 0 and self.group_table.item(row, 0) else None
        group = self._selected_group()
        if group:
            self.status_label.setText(
                f"Selected group '{group['name']}' with {int(group.get('schedule_count') or 0)} schedules."
            )
        self._sync_action_state()

    def _sync_action_state(self) -> None:
        has_group = self.current_group_id is not None
        self.edit_button.setEnabled(has_group)
        self.delete_button.setEnabled(has_group)
        self.toggle_button.setEnabled(has_group)
        if not has_group:
            self.toggle_button.setText("Pause")
            return
        group = self._selected_group()
        self.toggle_button.setText("Resume" if group and not bool(group.get("is_enabled", 1)) else "Pause")

    def _open_editor(self, group_id: int | None) -> None:
        group = self.scheduler_service.get_group(group_id) if group_id else None
        dialog = ScheduleGroupEditorDialog(group=group, parent=self)
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        payload = dialog.payload()
        try:
            saved_group_id = self.scheduler_service.save_group(
                payload["group_id"],
                payload["name"],
                payload["description"],
                payload["is_enabled"],
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save group failed", str(exc))
            return
        self.load_groups()
        self._select_group(saved_group_id)
        self.groups_changed.emit()

    def _edit_selected(self) -> None:
        if self.current_group_id is None:
            QtWidgets.QMessageBox.information(self, "Schedule Groups", "Select a group first.")
            return
        self._open_editor(int(self.current_group_id))

    def _toggle_selected(self) -> None:
        if self.current_group_id is None:
            return
        group = self._selected_group()
        if not group:
            return
        try:
            self.scheduler_service.set_group_enabled(int(self.current_group_id), not bool(group.get("is_enabled", 1)))
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Schedule Groups", str(exc))
            return
        self.load_groups()
        self._select_group(int(self.current_group_id))
        self.groups_changed.emit()

    def _delete_selected(self) -> None:
        if self.current_group_id is None:
            return
        if QtWidgets.QMessageBox.question(
            self,
            "Delete Group",
            "Delete this group? Schedules will stay in place and become ungrouped.",
        ) != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.scheduler_service.delete_group(int(self.current_group_id))
        self.current_group_id = None
        self.load_groups()
        self.groups_changed.emit()

    def _select_group(self, group_id: int) -> None:
        for row in range(self.group_table.rowCount()):
            item = self.group_table.item(row, 0)
            if item and int(item.text()) == int(group_id):
                self.group_table.selectRow(row)
                return


class ScheduleEditorDialog(QtWidgets.QDialog):
    def __init__(
        self,
        scheduler_service,
        workflow_service,
        device_service,
        account_service,
        schedule: dict | None = None,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.scheduler_service = scheduler_service
        self.workflow_service = workflow_service
        self.device_service = device_service
        self.account_service = account_service
        self.schedule = schedule or {}
        self.setWindowTitle("Edit Schedule" if schedule else "New Schedule")
        self.resize(760, 840)
        self.setModal(True)
        self._build_ui()
        self._load_form()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(12, 12, 12, 12)
        root.setSpacing(12)

        scroll = QtWidgets.QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QtWidgets.QFrame.Shape.NoFrame)
        root.addWidget(scroll, 1)

        content = QtWidgets.QWidget()
        self.content_layout = QtWidgets.QVBoxLayout(content)
        self.content_layout.setContentsMargins(0, 0, 0, 0)
        self.content_layout.setSpacing(12)
        scroll.setWidget(content)

        general_card = CardFrame()
        general_layout = QtWidgets.QFormLayout(general_card)
        general_layout.setContentsMargins(16, 16, 16, 16)
        general_layout.setSpacing(8)

        self.name_input = QtWidgets.QLineEdit()
        self.workflow_combo = QtWidgets.QComboBox()
        self.group_combo = QtWidgets.QComboBox()
        self.manage_groups_button = make_button("Manage Groups", "secondary")
        group_row = QtWidgets.QHBoxLayout()
        group_row.setContentsMargins(0, 0, 0, 0)
        group_row.addWidget(self.group_combo, 1)
        group_row.addWidget(self.manage_groups_button)
        group_widget = QtWidgets.QWidget()
        group_widget.setLayout(group_row)
        self.priority_spin = QtWidgets.QSpinBox()
        self.priority_spin.setRange(1, 999)
        self.priority_spin.setValue(100)
        self.priority_spin.setToolTip("Lower numbers run first when multiple schedules are due together.")
        self.device_combo = QtWidgets.QComboBox()
        self.platform_combo = QtWidgets.QComboBox()
        self.account_combo = QtWidgets.QComboBox()
        self.use_current_account_check = QtWidgets.QCheckBox("Use current account")
        self.enabled_check = QtWidgets.QCheckBox("Enabled")
        self.enabled_check.setChecked(True)
        self.schedule_type_combo = QtWidgets.QComboBox()
        self.schedule_type_combo.addItem("Run Once", "once")
        self.schedule_type_combo.addItem("Every N Minutes", "interval")
        self.schedule_type_combo.addItem("Daily", "daily")
        self.schedule_type_combo.addItem("Weekly", "weekly")

        general_layout.addRow("Name", self.name_input)
        general_layout.addRow("Workflow", self.workflow_combo)
        general_layout.addRow("Group", group_widget)
        general_layout.addRow("Priority", self.priority_spin)
        general_layout.addRow("Device", self.device_combo)
        general_layout.addRow("Platform", self.platform_combo)
        general_layout.addRow("Account", self.account_combo)
        general_layout.addRow("", self.use_current_account_check)
        general_layout.addRow("Schedule Type", self.schedule_type_combo)
        general_layout.addRow("", self.enabled_check)
        self.content_layout.addWidget(general_card)

        schedule_card = CardFrame()
        schedule_layout = QtWidgets.QVBoxLayout(schedule_card)
        schedule_layout.setContentsMargins(16, 16, 16, 16)
        schedule_layout.setSpacing(8)
        schedule_layout.addWidget(make_form_label("Schedule"))

        self.schedule_stack = QtWidgets.QStackedWidget()

        self.once_page = QtWidgets.QWidget()
        once_layout = QtWidgets.QFormLayout(self.once_page)
        self.once_datetime = QtWidgets.QDateTimeEdit(QtCore.QDateTime.currentDateTime().addSecs(3600))
        self.once_datetime.setDisplayFormat("yyyy-MM-dd HH:mm")
        self.once_datetime.setCalendarPopup(True)
        once_layout.addRow("Run At", self.once_datetime)

        self.interval_page = QtWidgets.QWidget()
        interval_layout = QtWidgets.QFormLayout(self.interval_page)
        self.interval_spin = QtWidgets.QSpinBox()
        self.interval_spin.setRange(1, 1440)
        self.interval_spin.setValue(30)
        self.interval_spin.setSuffix(" min")
        interval_layout.addRow("Every", self.interval_spin)

        self.daily_page = QtWidgets.QWidget()
        daily_layout = QtWidgets.QFormLayout(self.daily_page)
        self.daily_time = QtWidgets.QTimeEdit(QtCore.QTime(9, 0))
        self.daily_time.setDisplayFormat("HH:mm")
        daily_layout.addRow("Time", self.daily_time)

        self.weekly_page = QtWidgets.QWidget()
        weekly_layout = QtWidgets.QVBoxLayout(self.weekly_page)
        weekly_layout.setContentsMargins(0, 0, 0, 0)
        weekly_layout.setSpacing(8)
        weekly_form = QtWidgets.QFormLayout()
        self.weekly_time = QtWidgets.QTimeEdit(QtCore.QTime(9, 0))
        self.weekly_time.setDisplayFormat("HH:mm")
        weekly_form.addRow("Time", self.weekly_time)
        weekly_layout.addLayout(weekly_form)
        weekday_row = QtWidgets.QHBoxLayout()
        self.weekday_checks: list[QtWidgets.QCheckBox] = []
        for label in ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"):
            checkbox = QtWidgets.QCheckBox(label)
            self.weekday_checks.append(checkbox)
            weekday_row.addWidget(checkbox)
        weekly_layout.addLayout(weekday_row)

        self.schedule_stack.addWidget(self.once_page)
        self.schedule_stack.addWidget(self.interval_page)
        self.schedule_stack.addWidget(self.daily_page)
        self.schedule_stack.addWidget(self.weekly_page)
        schedule_layout.addWidget(self.schedule_stack)
        self.content_layout.addWidget(schedule_card)

        policy_card = CardFrame()
        policy_layout = QtWidgets.QFormLayout(policy_card)
        policy_layout.setContentsMargins(16, 16, 16, 16)
        policy_layout.setSpacing(8)
        self.active_window_check = QtWidgets.QCheckBox("Limit to active window")
        self.window_start = QtWidgets.QTimeEdit(QtCore.QTime(9, 0))
        self.window_start.setDisplayFormat("HH:mm")
        self.window_end = QtWidgets.QTimeEdit(QtCore.QTime(18, 0))
        self.window_end.setDisplayFormat("HH:mm")
        self.jitter_spin = QtWidgets.QSpinBox()
        self.jitter_spin.setRange(0, 3600)
        self.jitter_spin.setSuffix(" sec")
        self.missed_run_combo = QtWidgets.QComboBox()
        self.missed_run_combo.addItem("Run Immediately", "run_immediately")
        self.missed_run_combo.addItem("Skip", "skip")
        self.missed_run_combo.addItem("Reschedule Next", "reschedule_next")
        self.overlap_combo = QtWidgets.QComboBox()
        self.overlap_combo.addItem("Skip If Running", "skip_if_running")
        self.overlap_combo.addItem("Queue Next", "queue_next")
        self.retry_spin = QtWidgets.QSpinBox()
        self.retry_spin.setRange(0, 10)
        self.retry_delay_spin = QtWidgets.QSpinBox()
        self.retry_delay_spin.setRange(0, 600)
        self.retry_delay_spin.setSuffix(" sec")

        policy_layout.addRow("", self.active_window_check)
        policy_layout.addRow("Window Start", self.window_start)
        policy_layout.addRow("Window End", self.window_end)
        policy_layout.addRow("Jitter", self.jitter_spin)
        policy_layout.addRow("Missed Runs", self.missed_run_combo)
        policy_layout.addRow("Overlap", self.overlap_combo)
        policy_layout.addRow("Retry Count", self.retry_spin)
        policy_layout.addRow("Retry Delay", self.retry_delay_spin)
        self.content_layout.addWidget(policy_card)

        self.summary_label = QtWidgets.QLabel("Use groups and priority to coordinate larger schedule batches.")
        self.summary_label.setObjectName("subtitleLabel")
        self.summary_label.setWordWrap(True)
        self.content_layout.addWidget(self.summary_label)

        button_box = QtWidgets.QDialogButtonBox()
        self.cancel_button = button_box.addButton("Cancel", QtWidgets.QDialogButtonBox.ButtonRole.RejectRole)
        self.save_button = button_box.addButton("Save Schedule", QtWidgets.QDialogButtonBox.ButtonRole.AcceptRole)
        root.addWidget(button_box)

        self.cancel_button.clicked.connect(self.reject)
        self.save_button.clicked.connect(self._save_and_accept)
        self.manage_groups_button.clicked.connect(self._manage_groups)
        self.schedule_type_combo.currentIndexChanged.connect(self._on_schedule_type_changed)
        self.device_combo.currentIndexChanged.connect(self._on_device_changed)
        self.platform_combo.currentIndexChanged.connect(self._on_platform_changed)
        self.use_current_account_check.toggled.connect(self._sync_account_state)
        self.active_window_check.toggled.connect(self._sync_window_state)
        self._on_schedule_type_changed()
        self._sync_account_state()
        self._sync_window_state()

    def _load_form(self) -> None:
        self._refresh_workflows()
        self._refresh_groups()
        self._refresh_devices()

        if not self.schedule:
            return

        self.name_input.setText(str(self.schedule.get("name") or ""))
        self.priority_spin.setValue(int(self.schedule.get("priority") or 100))
        self._set_combo_data(self.workflow_combo, int(self.schedule["workflow_id"]))
        self._set_combo_data(self.group_combo, int(self.schedule.get("schedule_group_id") or 0) or None)
        self._set_combo_data(self.device_combo, int(self.schedule["device_id"]))
        self._on_device_changed()
        self._set_combo_data(self.platform_combo, int(self.schedule.get("device_platform_id") or 0) or None)
        self._on_platform_changed()
        self.use_current_account_check.setChecked(bool(self.schedule.get("use_current_account", 0)))
        self._set_combo_data(self.account_combo, int(self.schedule.get("account_id") or 0) or None)
        self.enabled_check.setChecked(bool(self.schedule.get("is_enabled", 1)))

        schedule_type = str(self.schedule.get("schedule_type") or "interval")
        self._set_combo_data(self.schedule_type_combo, schedule_type)
        config = dict(self.schedule.get("schedule_config") or {})
        if schedule_type == "once":
            dt = QtCore.QDateTime.fromString(str(config.get("run_at") or ""), "yyyy-MM-dd HH:mm:ss")
            self.once_datetime.setDateTime(dt if dt.isValid() else QtCore.QDateTime.currentDateTime().addSecs(3600))
        elif schedule_type == "interval":
            self.interval_spin.setValue(int(config.get("every_minutes") or 30))
        elif schedule_type == "daily":
            self.daily_time.setTime(QtCore.QTime.fromString(str(config.get("time") or "09:00"), "HH:mm"))
        elif schedule_type == "weekly":
            self.weekly_time.setTime(QtCore.QTime.fromString(str(config.get("time") or "09:00"), "HH:mm"))
            selected_days = {int(day) for day in config.get("weekdays", [])}
            for index, checkbox in enumerate(self.weekday_checks):
                checkbox.setChecked(index in selected_days)

        self.active_window_check.setChecked(bool(config.get("active_window_enabled", False)))
        self.window_start.setTime(QtCore.QTime.fromString(str(config.get("window_start") or "09:00"), "HH:mm"))
        self.window_end.setTime(QtCore.QTime.fromString(str(config.get("window_end") or "18:00"), "HH:mm"))
        self.jitter_spin.setValue(int(config.get("jitter_seconds") or 0))
        self._set_combo_data(self.missed_run_combo, str(config.get("missed_run_policy") or "run_immediately"))
        self._set_combo_data(self.overlap_combo, str(config.get("overlap_policy") or "skip_if_running"))
        self.retry_spin.setValue(int(config.get("retry_on_failure") or 0))
        self.retry_delay_spin.setValue(int(config.get("retry_delay_seconds") or 0))
        self._sync_window_state()

    def _refresh_workflows(self) -> None:
        current_value = self.workflow_combo.currentData()
        self.workflow_combo.blockSignals(True)
        self.workflow_combo.clear()
        for workflow in self.workflow_service.list_workflows():
            self.workflow_combo.addItem(f"{workflow['name']} (ID: {workflow['id']})", int(workflow["id"]))
        self.workflow_combo.blockSignals(False)
        if current_value is not None:
            self._set_combo_data(self.workflow_combo, current_value)
        if self.workflow_combo.count() == 0:
            self.workflow_combo.addItem("No workflows", None)

    def _refresh_groups(self) -> None:
        current_value = self.group_combo.currentData()
        self.group_combo.blockSignals(True)
        self.group_combo.clear()
        self.group_combo.addItem("No group", None)
        for group in self.scheduler_service.list_groups():
            state_suffix = "" if bool(group.get("is_enabled", 1)) else " [Paused]"
            self.group_combo.addItem(f"{group['name']}{state_suffix}", int(group["id"]))
        self.group_combo.blockSignals(False)
        self._set_combo_data(self.group_combo, current_value)

    def _refresh_devices(self) -> None:
        current_value = self.device_combo.currentData()
        self.device_combo.blockSignals(True)
        self.device_combo.clear()
        for device in self.device_service.list_devices():
            self.device_combo.addItem(f"{device['name']} ({device['serial']})", int(device["id"]))
        self.device_combo.blockSignals(False)
        if current_value is not None:
            self._set_combo_data(self.device_combo, current_value)
        if self.device_combo.count() > 0 and self.device_combo.currentIndex() < 0:
            self.device_combo.setCurrentIndex(0)
        self._on_device_changed()

    def _on_schedule_type_changed(self) -> None:
        schedule_type = self.schedule_type_combo.currentData()
        page_index = {"once": 0, "interval": 1, "daily": 2, "weekly": 3}.get(schedule_type, 1)
        self.schedule_stack.setCurrentIndex(page_index)

    def _on_device_changed(self) -> None:
        current_platform = self.platform_combo.currentData()
        device_id = self.device_combo.currentData()
        self.platform_combo.blockSignals(True)
        self.platform_combo.clear()
        self.platform_combo.addItem("No platform", None)
        if device_id:
            for platform in self.account_service.list_device_platforms(int(device_id)):
                label = f"{platform['platform_name']} ({platform['platform_key']})"
                self.platform_combo.addItem(label, int(platform["id"]))
        self.platform_combo.blockSignals(False)
        self._set_combo_data(self.platform_combo, current_platform)
        self._on_platform_changed()

    def _on_platform_changed(self) -> None:
        current_account = self.account_combo.currentData()
        platform_id = self.platform_combo.currentData()
        self.account_combo.clear()
        self.account_combo.addItem("No account", None)
        if platform_id:
            for account in self.account_service.list_accounts(int(platform_id)):
                self.account_combo.addItem(str(account["display_name"]), int(account["id"]))
        self._set_combo_data(self.account_combo, current_account)
        self._sync_account_state()

    def _sync_account_state(self) -> None:
        has_platform = self.platform_combo.currentData() is not None
        self.use_current_account_check.setEnabled(has_platform)
        self.account_combo.setEnabled(has_platform and not self.use_current_account_check.isChecked())

    def _sync_window_state(self) -> None:
        enabled = self.active_window_check.isChecked()
        self.window_start.setEnabled(enabled)
        self.window_end.setEnabled(enabled)

    def _set_combo_data(self, combo: QtWidgets.QComboBox, value) -> None:
        index = combo.findData(value)
        combo.setCurrentIndex(index if index >= 0 else 0)

    def _manage_groups(self) -> None:
        dialog = ScheduleGroupManagerDialog(self.scheduler_service, parent=self)
        dialog.groups_changed.connect(self._refresh_groups)
        dialog.exec()
        self._refresh_groups()

    def payload(self) -> dict:
        schedule_type = str(self.schedule_type_combo.currentData() or "interval")
        if schedule_type == "once":
            config = {"run_at": self.once_datetime.dateTime().toString("yyyy-MM-dd HH:mm:ss")}
        elif schedule_type == "daily":
            config = {"time": self.daily_time.time().toString("HH:mm")}
        elif schedule_type == "weekly":
            config = {
                "time": self.weekly_time.time().toString("HH:mm"),
                "weekdays": [index for index, checkbox in enumerate(self.weekday_checks) if checkbox.isChecked()],
            }
        else:
            config = {"every_minutes": int(self.interval_spin.value())}
        config.update(
            {
                "active_window_enabled": self.active_window_check.isChecked(),
                "window_start": self.window_start.time().toString("HH:mm"),
                "window_end": self.window_end.time().toString("HH:mm"),
                "jitter_seconds": int(self.jitter_spin.value()),
                "missed_run_policy": str(self.missed_run_combo.currentData() or "run_immediately"),
                "overlap_policy": str(self.overlap_combo.currentData() or "skip_if_running"),
                "retry_on_failure": int(self.retry_spin.value()),
                "retry_delay_seconds": int(self.retry_delay_spin.value()),
            }
        )
        return {
            "schedule_id": int(self.schedule.get("id") or 0) or None,
            "name": self.name_input.text().strip(),
            "workflow_id": self.workflow_combo.currentData(),
            "schedule_group_id": self.group_combo.currentData(),
            "priority": int(self.priority_spin.value()),
            "device_id": self.device_combo.currentData(),
            "device_platform_id": self.platform_combo.currentData(),
            "account_id": self.account_combo.currentData(),
            "use_current_account": self.use_current_account_check.isChecked(),
            "schedule_type": schedule_type,
            "schedule_config": config,
            "is_enabled": self.enabled_check.isChecked(),
        }

    def _save_and_accept(self) -> None:
        workflow_id = self.workflow_combo.currentData()
        device_id = self.device_combo.currentData()
        if workflow_id is None:
            QtWidgets.QMessageBox.warning(self, "Schedules", "Select a workflow first.")
            return
        if device_id is None:
            QtWidgets.QMessageBox.warning(self, "Schedules", "Select a device first.")
            return
        if not self.name_input.text().strip():
            QtWidgets.QMessageBox.warning(self, "Schedules", "Schedule name is required.")
            return
        self.accept()


class SchedulesPage(QtWidgets.QWidget):
    schedules_changed = QtCore.Signal()
    logs_changed = QtCore.Signal()
    run_requested = QtCore.Signal(int)
    toggle_requested = QtCore.Signal(int, bool)

    def __init__(
        self,
        scheduler_service,
        workflow_service,
        device_service,
        account_service,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.scheduler_service = scheduler_service
        self.workflow_service = workflow_service
        self.device_service = device_service
        self.account_service = account_service
        self.current_schedule_id: int | None = None
        self.running_schedule_ids: set[int] = set()
        self.queued_schedule_ids: set[int] = set()
        self._build_ui()
        self.load_schedules()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(16)

        title = QtWidgets.QLabel("Schedules")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Phase 3 dashboard with grouped schedules, priority ordering, and live running/queued visibility.")
        subtitle.setObjectName("subtitleLabel")
        root.addWidget(title)
        root.addWidget(subtitle)

        dashboard_card = CardFrame()
        dashboard_layout = QtWidgets.QVBoxLayout(dashboard_card)
        dashboard_layout.setContentsMargins(18, 18, 18, 18)
        dashboard_layout.setSpacing(12)

        metrics_layout = QtWidgets.QGridLayout()
        self.metric_labels: dict[str, QtWidgets.QLabel] = {}
        metric_titles = [
            ("total", "Total"),
            ("enabled", "Enabled"),
            ("paused", "Paused"),
            ("running", "Running"),
            ("queued", "Queued"),
            ("due_now", "Due Now"),
            ("groups", "Groups"),
        ]
        for index, (key, title_text) in enumerate(metric_titles):
            card = QtWidgets.QFrame()
            card.setProperty("panel", True)
            card_layout = QtWidgets.QVBoxLayout(card)
            card_layout.setContentsMargins(12, 10, 12, 10)
            caption = QtWidgets.QLabel(title_text)
            caption.setObjectName("subtitleLabel")
            value = QtWidgets.QLabel("0")
            value.setObjectName("titleLabel")
            card_layout.addWidget(caption)
            card_layout.addWidget(value)
            metrics_layout.addWidget(card, index // 4, index % 4)
            self.metric_labels[key] = value
        dashboard_layout.addLayout(metrics_layout)

        lists_row = QtWidgets.QHBoxLayout()
        self.running_list = QtWidgets.QListWidget()
        self.queued_list = QtWidgets.QListWidget()
        self.next_runs_list = QtWidgets.QListWidget()
        for label_text, widget in (
            ("Running Now", self.running_list),
            ("Queued", self.queued_list),
            ("Next Up", self.next_runs_list),
        ):
            column = QtWidgets.QVBoxLayout()
            column.addWidget(make_form_label(label_text))
            column.addWidget(widget, 1)
            lists_row.addLayout(column, 1)
        dashboard_layout.addLayout(lists_row)
        root.addWidget(dashboard_card)

        splitter = QtWidgets.QSplitter()
        splitter.setChildrenCollapsible(False)
        root.addWidget(splitter, 1)

        left_card = CardFrame()
        left_layout = QtWidgets.QVBoxLayout(left_card)
        left_layout.setContentsMargins(18, 18, 18, 18)
        left_layout.setSpacing(10)

        button_row = QtWidgets.QHBoxLayout()
        self.new_button = make_button("New Schedule", "secondary")
        self.edit_button = make_button("Edit Selected", "secondary")
        self.manage_groups_button = make_button("Manage Groups", "secondary")
        self.toggle_button = make_button("Pause", "secondary")
        self.run_now_button = make_button("Run Now", "secondary")
        self.delete_button = make_button("Delete", "danger")
        self.refresh_button = make_button("Refresh", "secondary")
        for button in (
            self.new_button,
            self.edit_button,
            self.manage_groups_button,
            self.toggle_button,
            self.run_now_button,
            self.delete_button,
            self.refresh_button,
        ):
            button_row.addWidget(button)
        button_row.addStretch(1)
        left_layout.addLayout(button_row)

        self.schedule_table = QtWidgets.QTableWidget(0, 10)
        self.schedule_table.setHorizontalHeaderLabels(
            ["ID", "Name", "Group", "Priority", "Workflow", "Device", "Schedule", "Next Run", "Last Run", "Status"]
        )
        self.schedule_table.setColumnHidden(0, True)
        self.schedule_table.verticalHeader().setVisible(False)
        self.schedule_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.schedule_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.schedule_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.schedule_table.horizontalHeader().setStretchLastSection(True)
        left_layout.addWidget(self.schedule_table, 1)

        self.status_label = QtWidgets.QLabel("Create or edit schedules from the popup editor.")
        self.status_label.setObjectName("subtitleLabel")
        self.status_label.setWordWrap(True)
        left_layout.addWidget(self.status_label)
        splitter.addWidget(left_card)

        right_splitter = QtWidgets.QSplitter(QtCore.Qt.Orientation.Vertical)
        right_splitter.setChildrenCollapsible(False)

        history_card = CardFrame()
        history_layout = QtWidgets.QVBoxLayout(history_card)
        history_layout.setContentsMargins(18, 18, 18, 18)
        history_layout.setSpacing(10)
        history_layout.addWidget(make_form_label("Schedule History"))
        self.run_table = QtWidgets.QTableWidget(0, 5)
        self.run_table.setHorizontalHeaderLabels(["Started", "Finished", "Trigger", "Status", "Message"])
        self.run_table.verticalHeader().setVisible(False)
        self.run_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.run_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.run_table.horizontalHeader().setStretchLastSection(True)
        history_layout.addWidget(self.run_table, 1)
        right_splitter.addWidget(history_card)

        groups_card = CardFrame()
        groups_layout = QtWidgets.QVBoxLayout(groups_card)
        groups_layout.setContentsMargins(18, 18, 18, 18)
        groups_layout.setSpacing(10)
        groups_layout.addWidget(make_form_label("Group Overview"))
        self.group_table = QtWidgets.QTableWidget(0, 5)
        self.group_table.setHorizontalHeaderLabels(["Name", "State", "Schedules", "Running", "Queued"])
        self.group_table.verticalHeader().setVisible(False)
        self.group_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.group_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.group_table.horizontalHeader().setStretchLastSection(True)
        groups_layout.addWidget(self.group_table, 1)
        right_splitter.addWidget(groups_card)

        splitter.addWidget(right_splitter)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 2)

        self.new_button.clicked.connect(lambda: self._open_editor(None))
        self.edit_button.clicked.connect(self._edit_selected)
        self.manage_groups_button.clicked.connect(self._manage_groups)
        self.toggle_button.clicked.connect(self._toggle_schedule)
        self.run_now_button.clicked.connect(self._request_run_now)
        self.delete_button.clicked.connect(self.delete_schedule)
        self.refresh_button.clicked.connect(self._reload_all)
        self.schedule_table.itemSelectionChanged.connect(self._on_schedule_selected)
        self.schedule_table.itemDoubleClicked.connect(lambda *_: self._edit_selected())
        self._sync_action_state()

    def refresh_workflows(self) -> None:
        self.load_schedules()

    def refresh_devices(self) -> None:
        self.load_schedules()

    def set_runtime_state(self, running_ids: set[int], queued_ids: set[int]) -> None:
        self.running_schedule_ids = {int(schedule_id) for schedule_id in running_ids}
        self.queued_schedule_ids = {int(schedule_id) for schedule_id in queued_ids}
        self._refresh_dashboard()

    def load_schedules(self) -> None:
        schedules = self.scheduler_service.list_schedules()
        self.schedule_table.setRowCount(len(schedules))
        for row, schedule in enumerate(schedules):
            is_running = int(schedule["id"]) in self.running_schedule_ids
            is_queued = int(schedule["id"]) in self.queued_schedule_ids
            group_name = str(schedule.get("group_name") or "-")
            if schedule.get("group_name") and not bool(schedule.get("group_is_enabled", 1)):
                group_name = f"{group_name} [Paused]"
            status_parts = [str(schedule.get("last_status") or "idle")]
            if not bool(schedule.get("is_enabled", 1)):
                status_parts.append("disabled")
            if is_running:
                status_parts.append("running")
            elif is_queued:
                status_parts.append("queued")
            values = [
                int(schedule["id"]),
                str(schedule["name"]),
                group_name,
                int(schedule.get("priority") or 100),
                str(schedule.get("workflow_name") or "-"),
                str(schedule.get("device_name") or "-"),
                str(schedule.get("schedule_summary") or "-"),
                str(schedule.get("next_run_at") or "-"),
                str(schedule.get("last_run_at") or "-"),
                " / ".join(status_parts),
            ]
            for column, value in enumerate(values):
                self.schedule_table.setItem(row, column, QtWidgets.QTableWidgetItem(str(value)))
        self.schedule_table.resizeColumnsToContents()
        self._sync_action_state()
        self._refresh_dashboard()

    def load_runs(self) -> None:
        self.run_table.setRowCount(0)
        if not self.current_schedule_id:
            return
        runs = self.scheduler_service.list_runs(self.current_schedule_id, limit=100)
        self.run_table.setRowCount(len(runs))
        for row, run in enumerate(runs):
            values = [
                str(run.get("started_at") or "-"),
                str(run.get("finished_at") or "-"),
                str(run.get("trigger_source") or "-"),
                str(run.get("status") or "-"),
                str(run.get("message") or ""),
            ]
            for column, value in enumerate(values):
                self.run_table.setItem(row, column, QtWidgets.QTableWidgetItem(value))
        self.run_table.resizeColumnsToContents()

    def notify_schedule_result(self, schedule_id: int, result: dict) -> None:
        status = "completed" if result.get("success") else "failed"
        self.status_label.setText(f"Schedule #{schedule_id} {status}: {result.get('message') or '-'}")
        self.load_schedules()
        self.load_runs()
        self.logs_changed.emit()

    def _reload_all(self) -> None:
        self.load_schedules()
        self.load_runs()

    def _refresh_dashboard(self) -> None:
        snapshot = self.scheduler_service.dashboard_snapshot(
            running_schedule_ids=self.running_schedule_ids,
            queued_schedule_ids=self.queued_schedule_ids,
        )
        for key, label in self.metric_labels.items():
            label.setText(str(snapshot["counts"].get(key, 0)))

        self.running_list.clear()
        self.queued_list.clear()
        self.next_runs_list.clear()
        for schedule in snapshot["running"]:
            self.running_list.addItem(f"#{schedule['id']} {schedule['name']}")
        for schedule in snapshot["queued"]:
            self.queued_list.addItem(f"#{schedule['id']} {schedule['name']}")
        for schedule in snapshot["next_runs"]:
            self.next_runs_list.addItem(
                f"#{schedule['id']}  P{int(schedule.get('priority') or 100)}  {schedule['name']}  @ {schedule.get('next_run_at') or '-'}"
            )

        groups = snapshot["groups"]
        self.group_table.setRowCount(len(groups))
        for row, group in enumerate(groups):
            values = [
                str(group.get("name") or ""),
                "Enabled" if bool(group.get("is_enabled", 1)) else "Paused",
                f"{int(group.get('enabled_schedule_count') or 0)}/{int(group.get('schedule_count') or 0)}",
                int(group.get("running_count") or 0),
                int(group.get("queued_count") or 0),
            ]
            for column, value in enumerate(values):
                self.group_table.setItem(row, column, QtWidgets.QTableWidgetItem(str(value)))
        self.group_table.resizeColumnsToContents()

    def _on_schedule_selected(self) -> None:
        row = self.schedule_table.currentRow()
        if row < 0:
            self.current_schedule_id = None
            self.run_table.setRowCount(0)
            self._sync_action_state()
            return
        self.current_schedule_id = int(self.schedule_table.item(row, 0).text())
        schedule = self.scheduler_service.get_schedule(self.current_schedule_id)
        if schedule:
            group_text = f" / Group {schedule.get('group_name')}" if schedule.get("group_name") else ""
            self.status_label.setText(
                f"Selected '{schedule['name']}' on {schedule.get('device_name') or '-'}"
                f"{group_text} running {schedule.get('schedule_summary') or '-'}"
            )
        self.load_runs()
        self._sync_action_state()

    def _sync_action_state(self) -> None:
        has_schedule = self.current_schedule_id is not None
        self.edit_button.setEnabled(has_schedule)
        self.delete_button.setEnabled(has_schedule)
        self.run_now_button.setEnabled(has_schedule)
        self.toggle_button.setEnabled(has_schedule)
        if has_schedule:
            current_row = self.schedule_table.currentRow()
            status_text = self.schedule_table.item(current_row, 9).text() if current_row >= 0 and self.schedule_table.item(current_row, 9) else ""
            self.toggle_button.setText("Resume" if "disabled" in status_text or "paused" in status_text else "Pause")
        else:
            self.toggle_button.setText("Pause")

    def _request_run_now(self) -> None:
        if not self.current_schedule_id:
            return
        self.status_label.setText(f"Running schedule #{self.current_schedule_id} now...")
        self.run_requested.emit(int(self.current_schedule_id))

    def _toggle_schedule(self) -> None:
        if not self.current_schedule_id:
            return
        should_enable = self.toggle_button.text() == "Resume"
        self.toggle_requested.emit(int(self.current_schedule_id), should_enable)

    def _edit_selected(self) -> None:
        if not self.current_schedule_id:
            QtWidgets.QMessageBox.information(self, "Schedules", "Select a schedule first.")
            return
        self._open_editor(int(self.current_schedule_id))

    def _manage_groups(self) -> None:
        dialog = ScheduleGroupManagerDialog(self.scheduler_service, parent=self)
        dialog.groups_changed.connect(self._reload_all)
        dialog.exec()
        self.load_schedules()
        self.schedules_changed.emit()

    def _open_editor(self, schedule_id: int | None) -> None:
        schedule = self.scheduler_service.get_schedule(schedule_id) if schedule_id else None
        dialog = ScheduleEditorDialog(
            self.scheduler_service,
            self.workflow_service,
            self.device_service,
            self.account_service,
            schedule=schedule,
            parent=self,
        )
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        payload = dialog.payload()
        try:
            saved_schedule_id = self.scheduler_service.save_schedule(
                payload["schedule_id"],
                payload["name"],
                int(payload["workflow_id"]),
                int(payload["device_id"]),
                int(payload["device_platform_id"] or 0) or None,
                int(payload["account_id"] or 0) or None,
                bool(payload["use_current_account"]),
                str(payload["schedule_type"]),
                dict(payload["schedule_config"]),
                bool(payload["is_enabled"]),
                schedule_group_id=int(payload["schedule_group_id"] or 0) or None,
                priority=int(payload["priority"]),
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save schedule failed", str(exc))
            return
        self.status_label.setText("Schedule saved.")
        self.load_schedules()
        self._select_schedule_row(saved_schedule_id)
        self.schedules_changed.emit()

    def delete_schedule(self) -> None:
        if not self.current_schedule_id:
            return
        if QtWidgets.QMessageBox.question(self, "Delete Schedule", "Delete this schedule and its run history?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.scheduler_service.delete_schedule(int(self.current_schedule_id))
        self.current_schedule_id = None
        self.status_label.setText("Schedule deleted.")
        self.run_table.setRowCount(0)
        self.load_schedules()
        self.schedules_changed.emit()

    def _select_schedule_row(self, schedule_id: int) -> None:
        for row in range(self.schedule_table.rowCount()):
            item = self.schedule_table.item(row, 0)
            if item and int(item.text()) == int(schedule_id):
                self.schedule_table.selectRow(row)
                break
