from __future__ import annotations

import json

from PySide6 import QtCore, QtGui, QtWidgets

from automation_studio.models import definition_for
from automation_studio.ui.step_editor import StepEditorDialog
from automation_studio.ui.watcher_profile_dialog import WorkflowProfileAttachDialog
from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class WorkflowRunner(QtCore.QThread):
    result_ready = QtCore.Signal(dict)

    def __init__(
        self,
        workflow_service,
        workflow_id: int,
        device_id: int,
        step_ids: list[int] | None = None,
        device_platform_id: int | None = None,
        account_id: int | None = None,
        use_current_account: bool = False,
    ) -> None:
        super().__init__()
        self.workflow_service = workflow_service
        self.workflow_id = workflow_id
        self.device_id = device_id
        self.step_ids = list(step_ids or [])
        self.device_platform_id = device_platform_id
        self.account_id = account_id
        self.use_current_account = use_current_account

    def run(self) -> None:
        if len(self.step_ids) == 1:
            result = self.workflow_service.execute_step(
                self.workflow_id,
                self.step_ids[0],
                self.device_id,
                device_platform_id=self.device_platform_id,
                account_id=self.account_id,
                use_current_account=self.use_current_account,
            )
        else:
            result = self.workflow_service.execute_workflow(
                self.workflow_id,
                self.device_id,
                device_platform_id=self.device_platform_id,
                account_id=self.account_id,
                use_current_account=self.use_current_account,
            )
        self.result_ready.emit(result)


class ReorderableStepTable(QtWidgets.QTableWidget):
    order_changed = QtCore.Signal(list, int)
    MIME_TYPE = "application/x-automation-studio-step-row"
    ID_COLUMN = 0
    DRAG_COLUMN = 1
    POSITION_COLUMN = 2

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self._drag_row: int | None = None
        self.setDragEnabled(True)
        self.setAcceptDrops(True)
        self.viewport().setAcceptDrops(True)
        self.setDragDropMode(QtWidgets.QAbstractItemView.DragDropMode.DragDrop)
        self.setDefaultDropAction(QtCore.Qt.DropAction.MoveAction)
        self.setDropIndicatorShown(True)
        self.setDragDropOverwriteMode(False)

    def dragEnterEvent(self, event: QtGui.QDragEnterEvent) -> None:
        if event.source() is self and event.mimeData().hasFormat(self.MIME_TYPE):
            event.acceptProposedAction()
            return
        event.ignore()

    def dragMoveEvent(self, event: QtGui.QDragMoveEvent) -> None:
        if event.source() is self and event.mimeData().hasFormat(self.MIME_TYPE):
            event.acceptProposedAction()
            return
        event.ignore()

    def dropEvent(self, event) -> None:
        try:
            if event.source() is not self or not event.mimeData().hasFormat(self.MIME_TYPE):
                event.ignore()
                return

            source_row = self._drag_row if self._drag_row is not None else self.currentRow()
            if source_row < 0:
                event.ignore()
                return

            target_row = self._target_row_from_event(event)
            if target_row < 0:
                event.ignore()
                return

            row_count = self.rowCount()
            if target_row > row_count:
                target_row = row_count
            if target_row == source_row or target_row == source_row + 1:
                event.accept()
                return

            moved_step_id = self.move_row(source_row, target_row)
            if moved_step_id is not None:
                event.acceptProposedAction()
                self.order_changed.emit(self.current_step_order(), moved_step_id)
            else:
                event.accept()
        finally:
            self._drag_row = None

    def startDrag(self, supported_actions: QtCore.Qt.DropActions) -> None:
        self._drag_row = self.currentRow()
        if self._drag_row < 0:
            return

        mime_data = QtCore.QMimeData()
        mime_data.setData(self.MIME_TYPE, str(self._drag_row).encode("ascii"))

        drag = QtGui.QDrag(self)
        drag.setMimeData(mime_data)
        drag.exec(QtCore.Qt.DropAction.MoveAction)
        self._drag_row = None

    def move_row(self, source_row: int, target_row: int) -> int | None:
        row_count = self.rowCount()
        if row_count <= 1 or source_row < 0 or source_row >= row_count:
            return None

        target_row = max(0, min(target_row, row_count))
        if target_row == source_row or target_row == source_row + 1:
            return None

        rows = self._row_snapshots()
        moved_row = rows.pop(source_row)
        if source_row < target_row:
            target_row -= 1
        rows.insert(target_row, moved_row)

        moved_step_id = int(moved_row[0].text())
        self._restore_rows(rows)
        self.selectRow(target_row)
        self._refresh_position_column()
        return moved_step_id

    def current_step_order(self) -> list[int]:
        return [self._step_id_at_row(row) for row in range(self.rowCount())]

    def _target_row_from_event(self, event) -> int:
        pos = event.position().toPoint()
        index = self.indexAt(pos)
        if not index.isValid():
            return self.rowCount()
        rect = self.visualRect(index)
        return index.row() + 1 if pos.y() >= rect.center().y() else index.row()

    def _row_snapshots(self) -> list[list[QtWidgets.QTableWidgetItem]]:
        rows: list[list[QtWidgets.QTableWidgetItem]] = []
        for row in range(self.rowCount()):
            rows.append(
                [
                    self.item(row, column).clone()
                    if self.item(row, column)
                    else QtWidgets.QTableWidgetItem("")
                    for column in range(self.columnCount())
                ]
            )
        return rows

    def _restore_rows(self, rows: list[list[QtWidgets.QTableWidgetItem]]) -> None:
        self.setRowCount(0)
        for row_items in rows:
            row = self.rowCount()
            self.insertRow(row)
            self._set_row(row, row_items)

    def _set_row(self, row: int, items: list[QtWidgets.QTableWidgetItem]) -> None:
        for column, item in enumerate(items):
            self.setItem(row, column, item)

    def _refresh_position_column(self) -> None:
        for row in range(self.rowCount()):
            item = self.item(row, self.POSITION_COLUMN)
            if item:
                item.setText(str(row + 1))

    def _step_id_at_row(self, row: int) -> int:
        return int(self.item(row, self.ID_COLUMN).text())


class WorkflowPage(QtWidgets.QWidget):
    workflows_changed = QtCore.Signal()
    logs_changed = QtCore.Signal()
    ACCOUNT_USE_CURRENT = "__current__"

    def __init__(
        self,
        workflow_service,
        device_service,
        watcher_service,
        account_service,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.workflow_service = workflow_service
        self.device_service = device_service
        self.watcher_service = watcher_service
        self.account_service = account_service
        self.current_workflow_id: int | None = None
        self.current_step_id: int | None = None
        self.current_steps: list[dict] = []
        self.runner: WorkflowRunner | None = None
        self._active_run_metadata: dict | None = None
        self._build_ui()
        self.refresh_devices()
        self.load_workflows()

    def _build_ui(self) -> None:
        root_layout = QtWidgets.QVBoxLayout(self)
        root_layout.setContentsMargins(0, 0, 0, 0)
        root_layout.setSpacing(16)

        header = QtWidgets.QHBoxLayout()
        title_box = QtWidgets.QVBoxLayout()
        title = QtWidgets.QLabel("Workflow")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Phase 1 editor: form-based step editing, preset, preview และจัดการ step ได้ครบในหน้าเดียว")
        subtitle.setObjectName("subtitleLabel")
        subtitle.setText(
            "Phase 3 editor with presets, flow control, variables, conditional jump, and workflow JSON import/export."
        )
        subtitle.setWordWrap(True)
        title_box.addWidget(title)
        title_box.addWidget(subtitle)
        header.addLayout(title_box)
        header.addStretch(1)
        root_layout.addLayout(header)

        content = QtWidgets.QSplitter()
        content.setChildrenCollapsible(False)
        root_layout.addWidget(content, 1)

        workflow_card = CardFrame()
        workflow_layout = QtWidgets.QVBoxLayout(workflow_card)
        workflow_layout.setContentsMargins(18, 18, 18, 18)
        workflow_layout.setSpacing(12)

        workflow_actions = QtWidgets.QHBoxLayout()
        self.workflow_new_button = make_button("New", "secondary")
        self.workflow_save_button = make_button("Save Workflow")
        self.workflow_export_button = make_button("Export JSON", "secondary")
        self.workflow_import_button = make_button("Import JSON", "secondary")
        self.workflow_delete_button = make_button("Delete", "danger")
        workflow_actions.addWidget(self.workflow_new_button)
        workflow_actions.addWidget(self.workflow_save_button)
        workflow_actions.addWidget(self.workflow_export_button)
        workflow_actions.addWidget(self.workflow_import_button)
        workflow_actions.addWidget(self.workflow_delete_button)
        workflow_layout.addLayout(workflow_actions)

        workflow_layout.addWidget(make_form_label("Workflow Name"))
        self.workflow_name_input = QtWidgets.QLineEdit()
        self.workflow_name_input.setPlaceholderText("Login And Screenshot")
        workflow_layout.addWidget(self.workflow_name_input)

        workflow_layout.addWidget(make_form_label("Description"))
        self.workflow_description_input = QtWidgets.QTextEdit()
        self.workflow_description_input.setFixedHeight(90)
        workflow_layout.addWidget(self.workflow_description_input)

        workflow_layout.addWidget(make_form_label("Linked Watchers"))
        self.watcher_list = QtWidgets.QListWidget()
        self.watcher_list.setMinimumHeight(72)
        self.watcher_list.setMaximumHeight(110)
        workflow_layout.addWidget(self.watcher_list)

        profile_header = QtWidgets.QHBoxLayout()
        profile_header.addWidget(make_form_label("Watcher Profiles"))
        profile_header.addStretch(1)
        self.manage_profiles_button = make_button("Profiles", "secondary")
        profile_header.addWidget(self.manage_profiles_button)
        workflow_layout.addLayout(profile_header)

        self.profile_list = QtWidgets.QListWidget()
        self.profile_list.setMinimumHeight(72)
        self.profile_list.setMaximumHeight(110)
        workflow_layout.addWidget(self.profile_list)

        workflow_layout.addWidget(make_form_label("Workflows"))
        self.workflow_list = QtWidgets.QListWidget()
        self.workflow_list.setMinimumHeight(180)
        self.workflow_list.setSizePolicy(
            QtWidgets.QSizePolicy.Policy.Preferred,
            QtWidgets.QSizePolicy.Policy.Expanding,
        )
        workflow_layout.addWidget(self.workflow_list, 1)
        content.addWidget(workflow_card)

        steps_card = CardFrame()
        steps_layout = QtWidgets.QVBoxLayout(steps_card)
        steps_layout.setContentsMargins(18, 18, 18, 18)
        steps_layout.setSpacing(12)

        top_actions = QtWidgets.QHBoxLayout()
        top_actions.setSpacing(8)
        self.device_combo = QtWidgets.QComboBox()
        self.platform_combo = QtWidgets.QComboBox()
        self.account_combo = QtWidgets.QComboBox()
        self.device_combo.setToolTip("Select the device that will run this workflow.")
        self.platform_combo.setToolTip("Optionally inject a platform context before the workflow starts.")
        self.account_combo.setToolTip("Use a specific account or the platform's current account.")
        top_actions.addWidget(self.device_combo, 2)
        top_actions.addWidget(self.platform_combo, 1)
        top_actions.addWidget(self.account_combo, 1)
        steps_layout.addLayout(top_actions)

        run_toolbar = QtWidgets.QHBoxLayout()
        run_toolbar.setSpacing(8)
        self.run_step_button = make_button("Run Step", "secondary")
        self.run_button = make_button("Run")
        run_toolbar.addWidget(self.run_step_button)
        run_toolbar.addWidget(self.run_button)
        run_toolbar.addStretch(1)
        steps_layout.addLayout(run_toolbar)

        step_toolbar = QtWidgets.QHBoxLayout()
        step_toolbar.setSpacing(8)
        self.add_step_button = make_button("Add Step")
        self.duplicate_step_button = make_button("Duplicate", "secondary")
        self.toggle_step_button = make_button("Disable", "secondary")
        self.move_up_button = make_button("Move Up", "secondary")
        self.move_down_button = make_button("Move Down", "secondary")
        self.edit_step_button = make_button("Edit", "secondary")
        self.delete_step_button = make_button("Delete", "danger")
        step_toolbar.addWidget(self.add_step_button)
        step_toolbar.addWidget(self.duplicate_step_button)
        step_toolbar.addWidget(self.edit_step_button)
        step_toolbar.addWidget(self.delete_step_button)
        step_toolbar.addStretch(1)
        steps_layout.addLayout(step_toolbar)

        step_toolbar_secondary = QtWidgets.QHBoxLayout()
        step_toolbar_secondary.setSpacing(8)
        step_toolbar_secondary.addWidget(self.toggle_step_button)
        step_toolbar_secondary.addWidget(self.move_up_button)
        step_toolbar_secondary.addWidget(self.move_down_button)
        step_toolbar_secondary.addStretch(1)
        steps_layout.addLayout(step_toolbar_secondary)

        self.steps_table = ReorderableStepTable()
        self.steps_table.setColumnCount(7)
        self.steps_table.setHorizontalHeaderLabels(["ID", "Drag", "Pos", "Name", "Type", "State", "Parameters"])
        self.steps_table.verticalHeader().setVisible(False)
        self.steps_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.steps_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.steps_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.steps_table.setColumnHidden(ReorderableStepTable.ID_COLUMN, True)
        self.steps_table.setAlternatingRowColors(True)
        steps_layout.addWidget(self.steps_table, 1)

        hint = QtWidgets.QLabel("ลากจากคอลัมน์ Drag เพื่อจัดลำดับ, ใช้ Duplicate/Enable/Disable ได้ และ Step Editor จะสร้าง JSON ให้แบบ preview")
        hint.setObjectName("subtitleLabel")
        hint.setText(
            "Drag rows to reorder, use Step Editor for presets plus flow control, and import/export workflows as JSON when you want to reuse or back up step sets."
        )
        hint.setWordWrap(True)
        steps_layout.addWidget(hint)

        self.run_status_label = QtWidgets.QLabel("เลือก workflow แล้วเพิ่ม steps ได้ทันที")
        self.run_status_label.setObjectName("subtitleLabel")
        self.run_status_label.setWordWrap(True)
        steps_layout.addWidget(self.run_status_label)

        content.addWidget(steps_card)
        content.setStretchFactor(0, 2)
        content.setStretchFactor(1, 5)

        self.workflow_new_button.clicked.connect(self.clear_workflow_form)
        self.workflow_save_button.clicked.connect(self.save_workflow)
        self.workflow_export_button.clicked.connect(self.export_workflow_json)
        self.workflow_import_button.clicked.connect(self.import_workflow_json)
        self.workflow_delete_button.clicked.connect(self.delete_workflow)
        self.manage_profiles_button.clicked.connect(self.manage_workflow_profiles)
        self.workflow_list.itemSelectionChanged.connect(self._on_workflow_selected)

        self.add_step_button.clicked.connect(self.open_new_step_dialog)
        self.duplicate_step_button.clicked.connect(self.duplicate_selected_step)
        self.toggle_step_button.clicked.connect(self.toggle_selected_step)
        self.move_up_button.clicked.connect(self.move_selected_step_up)
        self.move_down_button.clicked.connect(self.move_selected_step_down)
        self.edit_step_button.clicked.connect(self.open_edit_step_dialog)
        self.delete_step_button.clicked.connect(self.delete_step)
        self.steps_table.itemSelectionChanged.connect(self._on_step_selected)
        self.steps_table.itemDoubleClicked.connect(lambda *_: self.open_edit_step_dialog())
        self.steps_table.order_changed.connect(self._persist_step_order)
        self.run_step_button.clicked.connect(self.run_selected_step)
        self.run_button.clicked.connect(self.run_workflow)
        self.device_combo.currentIndexChanged.connect(self._on_device_changed)
        self.platform_combo.currentIndexChanged.connect(self._on_platform_changed)

        self._sync_step_actions()

    def refresh_devices(self) -> None:
        current = self.device_combo.currentData()
        self.device_combo.clear()
        for device in self.device_service.list_devices():
            label = f"{device['name']} ({device['serial']})"
            self.device_combo.addItem(label, device["id"])
        if current is not None:
            index = self.device_combo.findData(current)
            if index >= 0:
                self.device_combo.setCurrentIndex(index)
        self.refresh_runtime_targets()

    def refresh_runtime_targets(self) -> None:
        current_platform_id = self.platform_combo.currentData()
        current_account_data = self.account_combo.currentData()
        device_id = self.device_combo.currentData()
        platforms = self.account_service.list_device_platforms(int(device_id)) if device_id is not None else []

        self.platform_combo.blockSignals(True)
        self.platform_combo.clear()
        self.platform_combo.addItem("Platform: none", None)
        for platform in platforms:
            current_label = f" / Current: {platform.get('current_account_name')}" if platform.get("current_account_name") else ""
            label = f"{platform['platform_name']} ({platform['platform_key']}){current_label}"
            self.platform_combo.addItem(label, int(platform["id"]))
        platform_index = self.platform_combo.findData(current_platform_id)
        self.platform_combo.setCurrentIndex(platform_index if platform_index >= 0 else 0)
        self.platform_combo.blockSignals(False)

        self._refresh_account_targets(current_account_data)

    def _refresh_account_targets(self, preferred_data=None) -> None:
        self.account_combo.blockSignals(True)
        self.account_combo.clear()
        device_platform_id = self.platform_combo.currentData()
        if device_platform_id is None:
            self.account_combo.addItem("Account: none", None)
            self.account_combo.setCurrentIndex(0)
            self.account_combo.setEnabled(False)
            self.account_combo.blockSignals(False)
            return

        platform = self.account_service.get_device_platform(int(device_platform_id))
        current_name = platform.get("current_account_name") if platform else ""
        current_label = f"Use Current Account ({current_name})" if current_name else "Use Current Account"
        self.account_combo.addItem(current_label, self.ACCOUNT_USE_CURRENT)
        for account in self.account_service.list_accounts(int(device_platform_id)):
            suffix = " [Current]" if bool(account.get("is_current")) else ""
            self.account_combo.addItem(f"{account['display_name']}{suffix}", int(account["id"]))
        self.account_combo.setEnabled(True)
        account_index = self.account_combo.findData(preferred_data)
        self.account_combo.setCurrentIndex(account_index if account_index >= 0 else 0)
        self.account_combo.blockSignals(False)

    def _on_device_changed(self) -> None:
        self.refresh_runtime_targets()

    def _on_platform_changed(self) -> None:
        self._refresh_account_targets()

    def load_workflows(self) -> None:
        workflows = self.workflow_service.list_workflows()
        self.workflow_list.clear()
        for workflow in workflows:
            item = QtWidgets.QListWidgetItem(workflow["name"])
            item.setData(QtCore.Qt.ItemDataRole.UserRole, workflow["id"])
            self.workflow_list.addItem(item)
        self.workflows_changed.emit()

    def _on_workflow_selected(self) -> None:
        item = self.workflow_list.currentItem()
        if not item:
            return
        workflow_id = item.data(QtCore.Qt.ItemDataRole.UserRole)
        workflow = self.workflow_service.get_workflow(workflow_id)
        if not workflow:
            return
        self.current_workflow_id = workflow_id
        self.workflow_name_input.setText(workflow["name"])
        self.workflow_description_input.setPlainText(workflow.get("description", ""))
        self.load_linked_watchers()
        self.load_steps()
        self.run_status_label.setText("พร้อมรัน workflow หรือเพิ่ม step ใหม่")

    def clear_workflow_form(self) -> None:
        self.current_workflow_id = None
        self.current_step_id = None
        self.current_steps = []
        self.workflow_list.clearSelection()
        self.workflow_name_input.clear()
        self.workflow_description_input.clear()
        self.watcher_list.clear()
        self.profile_list.clear()
        self.steps_table.setRowCount(0)
        self._sync_step_actions()
        self.run_status_label.setText("พร้อมสร้าง workflow ใหม่")

    def save_workflow(self) -> None:
        name = self.workflow_name_input.text().strip()
        description = self.workflow_description_input.toPlainText().strip()
        if not name:
            QtWidgets.QMessageBox.warning(self, "Missing name", "กรุณากรอกชื่อ workflow")
            return
        workflow_id = self.workflow_service.save_workflow(self.current_workflow_id, name, description, True)
        self.current_workflow_id = workflow_id
        self.load_workflows()
        self._select_workflow_item(workflow_id)
        self.load_linked_watchers()
        self.run_status_label.setText("บันทึก workflow เรียบร้อย")

    def _select_workflow_item(self, workflow_id: int) -> None:
        for index in range(self.workflow_list.count()):
            item = self.workflow_list.item(index)
            if item.data(QtCore.Qt.ItemDataRole.UserRole) == workflow_id:
                self.workflow_list.setCurrentItem(item)
                break

    def export_workflow_json(self) -> None:
        if not self.current_workflow_id:
            QtWidgets.QMessageBox.warning(self, "Missing workflow", "Please select a workflow to export.")
            return
        workflow = self.workflow_service.get_workflow(self.current_workflow_id)
        default_name = f"{workflow['name'].strip().replace(' ', '_') or 'workflow'}.json"
        file_path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Export Workflow JSON",
            default_name,
            "JSON Files (*.json)",
        )
        if not file_path:
            return
        try:
            payload = self.workflow_service.export_workflow_definition(self.current_workflow_id)
            with open(file_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, ensure_ascii=False)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Export failed", str(exc))
            return
        self.run_status_label.setText("Workflow JSON exported successfully")

    def import_workflow_json(self) -> None:
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Import Workflow JSON",
            "",
            "JSON Files (*.json)",
        )
        if not file_path:
            return
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            workflow_id = self.workflow_service.import_workflow_definition(payload)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Import failed", str(exc))
            return
        self.load_workflows()
        self._select_workflow_item(workflow_id)
        self.run_status_label.setText("Workflow JSON imported successfully")

    def delete_workflow(self) -> None:
        if not self.current_workflow_id:
            return
        confirmation = QtWidgets.QMessageBox.question(
            self,
            "Delete workflow",
            "ต้องการลบ workflow นี้รวมถึง steps หรือไม่?",
        )
        if confirmation != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.workflow_service.delete_workflow(self.current_workflow_id)
        self.clear_workflow_form()
        self.load_workflows()

    def load_steps(self) -> None:
        self.current_step_id = None
        self.current_steps = []
        if not self.current_workflow_id:
            self.steps_table.setRowCount(0)
            self._sync_step_actions()
            return
        self.current_steps = self.workflow_service.list_steps(self.current_workflow_id)
        self.steps_table.setRowCount(len(self.current_steps))
        for row_index, step in enumerate(self.current_steps):
            compact_params = self._format_step_summary(step["parameters"])
            values = [
                str(step["id"]),
                ":::",
                str(step["position"]),
                step["name"],
                definition_for(step["step_type"]).label,
                "Enabled" if step["is_enabled"] else "Disabled",
                compact_params,
            ]
            for column, value in enumerate(values):
                item = QtWidgets.QTableWidgetItem(value)
                if column == ReorderableStepTable.DRAG_COLUMN:
                    item.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
                self.steps_table.setItem(row_index, column, item)
        self.steps_table.clearSelection()
        self._configure_steps_table_columns()
        self._sync_step_actions()

    def _configure_steps_table_columns(self) -> None:
        header = self.steps_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(ReorderableStepTable.DRAG_COLUMN, QtWidgets.QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(ReorderableStepTable.POSITION_COLUMN, QtWidgets.QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(3, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(6, QtWidgets.QHeaderView.ResizeMode.Stretch)
        self.steps_table.setColumnWidth(ReorderableStepTable.DRAG_COLUMN, 52)
        self.steps_table.setColumnWidth(ReorderableStepTable.POSITION_COLUMN, 56)
        self.steps_table.setColumnWidth(4, 120)
        self.steps_table.setColumnWidth(5, 90)

    def load_linked_watchers(self) -> None:
        self.watcher_list.clear()
        self.profile_list.clear()
        if not self.current_workflow_id:
            return
        attached_profiles = self.watcher_service.list_profiles_for_workflow(self.current_workflow_id)
        if not attached_profiles:
            self.profile_list.addItem("No watcher profile templates attached.")
        else:
            for profile in attached_profiles:
                state_label = "Active" if profile["is_active"] else "Inactive"
                self.profile_list.addItem(
                    f"{profile['name']} ({int(profile.get('watcher_count', 0) or 0)} watchers, {state_label})"
                )
        linked_watchers = self.watcher_service.list_watchers_for_workflow(self.current_workflow_id)
        if not linked_watchers:
            self.watcher_list.addItem("No global or workflow watchers linked yet.")
            return
        for watcher in linked_watchers:
            if watcher.get("source") == "profile":
                scope_label = f"Profile: {watcher.get('profile_name') or watcher.get('profile_id')}"
            else:
                scope_label = "Global" if watcher["scope_type"] == "global" else "Workflow"
            state_label = "Enabled" if watcher["is_enabled"] else "Disabled"
            self.watcher_list.addItem(
                f"[{scope_label}] {watcher['name']} - {watcher['condition_type']} -> {watcher['action_type']} ({state_label})"
            )

    def manage_workflow_profiles(self) -> None:
        if not self.current_workflow_id:
            QtWidgets.QMessageBox.information(self, "Workflow Profiles", "Select a workflow first.")
            return
        dialog = WorkflowProfileAttachDialog(
            profiles=self.watcher_service.list_profiles(),
            selected_profile_ids=[
                profile["id"] for profile in self.watcher_service.list_profiles_for_workflow(self.current_workflow_id)
            ],
            parent=self,
        )
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        try:
            self.watcher_service.save_workflow_profiles(self.current_workflow_id, dialog.selected_ids())
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save workflow profiles failed", str(exc))
            return
        self.load_linked_watchers()
        self.logs_changed.emit()
        self.run_status_label.setText("Workflow watcher profiles updated")

    def _selected_step(self) -> dict | None:
        if not self.current_step_id:
            return None
        return next((item for item in self.current_steps if item["id"] == self.current_step_id), None)

    def _on_step_selected(self) -> None:
        row = self.steps_table.currentRow()
        self.current_step_id = (
            int(self.steps_table.item(row, ReorderableStepTable.ID_COLUMN).text())
            if row >= 0
            else None
        )
        self._sync_step_actions()

    def _sync_step_actions(self) -> None:
        has_workflow = self.current_workflow_id is not None
        step = self._selected_step()
        has_step = step is not None
        current_row = self.steps_table.currentRow()
        self.add_step_button.setEnabled(has_workflow)
        self.workflow_export_button.setEnabled(has_workflow)
        self.workflow_import_button.setEnabled(True)
        self.run_button.setEnabled(has_workflow and self.runner is None)
        self.run_step_button.setEnabled(has_step and self.runner is None)
        self.duplicate_step_button.setEnabled(has_step)
        self.toggle_step_button.setEnabled(has_step)
        self.move_up_button.setEnabled(has_step and current_row > 0)
        self.move_down_button.setEnabled(has_step and 0 <= current_row < self.steps_table.rowCount() - 1)
        self.edit_step_button.setEnabled(has_step)
        self.delete_step_button.setEnabled(has_step)
        self.manage_profiles_button.setEnabled(has_workflow)
        self.toggle_step_button.setText("Disable" if has_step and step["is_enabled"] else "Enable")

    def _persist_step_order(self, ordered_step_ids: list[int], moved_step_id: int) -> None:
        if not self.current_workflow_id or not ordered_step_ids:
            return
        try:
            self.workflow_service.reorder_steps(self.current_workflow_id, ordered_step_ids)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Reorder failed", str(exc))
            self.load_steps()
            return
        self.load_steps()
        self._select_step_row(moved_step_id)
        self.run_status_label.setText("อัปเดตลำดับ steps เรียบร้อยแล้ว")

    def move_selected_step_up(self) -> None:
        row = self.steps_table.currentRow()
        if row <= 0:
            return
        moved_step_id = self.steps_table.move_row(row, row - 1)
        if moved_step_id is not None:
            self._persist_step_order(self.steps_table.current_step_order(), moved_step_id)

    def move_selected_step_down(self) -> None:
        row = self.steps_table.currentRow()
        if row < 0 or row >= self.steps_table.rowCount() - 1:
            return
        moved_step_id = self.steps_table.move_row(row, row + 2)
        if moved_step_id is not None:
            self._persist_step_order(self.steps_table.current_step_order(), moved_step_id)

    def open_new_step_dialog(self) -> None:
        if not self.current_workflow_id:
            QtWidgets.QMessageBox.warning(self, "Missing workflow", "กรุณาบันทึก workflow ก่อนเพิ่ม step")
            return
        self._open_step_dialog(None)

    def open_edit_step_dialog(self) -> None:
        step = self._selected_step()
        if not step:
            QtWidgets.QMessageBox.warning(self, "Missing step", "กรุณาเลือก step ที่ต้องการแก้ไข")
            return
        self._open_step_dialog(step)

    def _open_step_dialog(self, step: dict | None) -> None:
        dialog = StepEditorDialog(
            self,
            step_data=step,
            default_position=max(self.steps_table.rowCount(), 0) + 1,
            workflow_choices=[
                (int(workflow["id"]), str(workflow["name"]))
                for workflow in self.workflow_service.list_workflows()
            ],
        )
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return

        payload = dialog.payload()
        try:
            step_id = self.workflow_service.save_step(
                step["id"] if step else None,
                self.current_workflow_id,
                int(payload["position"]),
                str(payload["name"]),
                str(payload["step_type"]),
                json.dumps(payload["parameters"], ensure_ascii=False),
                bool(payload["is_enabled"]),
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save step failed", str(exc))
            return
        self.load_steps()
        self._select_step_row(step_id)
        self.run_status_label.setText("บันทึก step เรียบร้อย")

    def _select_step_row(self, step_id: int) -> None:
        for row in range(self.steps_table.rowCount()):
            if int(self.steps_table.item(row, ReorderableStepTable.ID_COLUMN).text()) == step_id:
                self.steps_table.selectRow(row)
                self.current_step_id = step_id
                break
        self._sync_step_actions()

    def duplicate_selected_step(self) -> None:
        step = self._selected_step()
        if not step:
            return
        try:
            new_step_id = self.workflow_service.save_step(
                None,
                self.current_workflow_id,
                int(step["position"]) + 1,
                f"{step['name']} Copy",
                step["step_type"],
                step["parameters"],
                bool(step["is_enabled"]),
            )
            ordered_ids = [item["id"] for item in self.current_steps]
            insert_index = ordered_ids.index(step["id"]) + 1
            ordered_ids.insert(insert_index, new_step_id)
            self.workflow_service.reorder_steps(self.current_workflow_id, ordered_ids)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Duplicate failed", str(exc))
            return

        self.load_steps()
        self._select_step_row(new_step_id)
        self.run_status_label.setText("สร้างสำเนา step เรียบร้อย")

    def toggle_selected_step(self) -> None:
        step = self._selected_step()
        if not step:
            return
        try:
            step_id = self.workflow_service.save_step(
                step["id"],
                self.current_workflow_id,
                step["position"],
                step["name"],
                step["step_type"],
                step["parameters"],
                not bool(step["is_enabled"]),
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Update failed", str(exc))
            return

        self.load_steps()
        self._select_step_row(step_id)
        self.run_status_label.setText("อัปเดตสถานะ step เรียบร้อย")

    def delete_step(self) -> None:
        step = self._selected_step()
        if not step:
            return
        confirmation = QtWidgets.QMessageBox.question(
            self,
            "Delete step",
            f"ต้องการลบ step '{step['name']}' หรือไม่?",
        )
        if confirmation != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.workflow_service.delete_step(step["id"])
        self.load_steps()
        self.run_status_label.setText("ลบ step เรียบร้อย")

    def run_workflow(self) -> None:
        if not self.current_workflow_id:
            QtWidgets.QMessageBox.warning(self, "Missing workflow", "กรุณาเลือก workflow")
            return
        device_id = self.device_combo.currentData()
        if device_id is None:
            QtWidgets.QMessageBox.warning(self, "Missing device", "กรุณาเพิ่มอุปกรณ์ก่อนรัน workflow")
            return
        device_platform_id, account_id, use_current_account = self._execution_target()
        self._start_runner(
            device_id,
            None,
            "Running workflow on selected device...",
            device_platform_id=device_platform_id,
            account_id=account_id,
            use_current_account=use_current_account,
        )

    def run_selected_step(self) -> None:
        if not self.current_workflow_id:
            QtWidgets.QMessageBox.warning(self, "Missing workflow", "Please select a workflow first.")
            return
        step = self._selected_step()
        if not step:
            QtWidgets.QMessageBox.warning(self, "Missing step", "Please select a step to run.")
            return
        device_id = self.device_combo.currentData()
        if device_id is None:
            QtWidgets.QMessageBox.warning(self, "Missing device", "Please add a device before running a step.")
            return
        device_platform_id, account_id, use_current_account = self._execution_target()
        self._start_runner(
            device_id,
            [int(step["id"])],
            f"Running selected step: {step['name']}...",
            device_platform_id=device_platform_id,
            account_id=account_id,
            use_current_account=use_current_account,
        )

    def _start_runner(
        self,
        device_id: int,
        step_ids: list[int] | None,
        status_text: str,
        *,
        device_platform_id: int | None = None,
        account_id: int | None = None,
        use_current_account: bool = False,
    ) -> None:
        self.run_button.setDisabled(True)
        self.run_step_button.setDisabled(True)
        self.run_status_label.setText(status_text)
        workflow_name = self.workflow_name_input.text().strip() or "Workflow"
        device_name = self.device_combo.currentText().strip() or f"Device {device_id}"
        self._active_run_metadata = {
            "task_id": "workflow-page-run",
            "workflow_id": int(self.current_workflow_id or 0),
            "workflow_name": workflow_name,
            "device_id": int(device_id),
            "device_name": device_name,
            "scope": "step" if step_ids else "workflow",
            "step_ids": list(step_ids or []),
            "started_at": QtCore.QDateTime.currentDateTime().toString("yyyy-MM-dd HH:mm:ss"),
            "status": "running",
            "detail": status_text,
        }
        self.runner = WorkflowRunner(
            self.workflow_service,
            self.current_workflow_id,
            device_id,
            step_ids,
            device_platform_id=device_platform_id,
            account_id=account_id,
            use_current_account=use_current_account,
        )
        self.runner.result_ready.connect(self._on_workflow_finished)
        self.runner.finished.connect(self._on_runner_finished)
        self.runner.start()

    def _execution_target(self) -> tuple[int | None, int | None, bool]:
        device_platform_id = self.platform_combo.currentData()
        if device_platform_id is None:
            return None, None, False
        account_data = self.account_combo.currentData()
        if account_data == self.ACCOUNT_USE_CURRENT:
            return int(device_platform_id), None, True
        if isinstance(account_data, int):
            return int(device_platform_id), int(account_data), False
        return int(device_platform_id), None, False

    def _on_runner_finished(self) -> None:
        self.runner = None
        self._active_run_metadata = None
        self._sync_step_actions()

    def _on_workflow_finished(self, result: dict) -> None:
        self.run_status_label.setText(result["message"])
        self.logs_changed.emit()
        if result.get("success"):
            QtWidgets.QMessageBox.information(self, "Workflow completed", result["message"])
        else:
            QtWidgets.QMessageBox.warning(self, "Workflow failed", result["message"])

    def runtime_snapshot(self) -> list[dict]:
        if not self.runner or not self.runner.isRunning() or not self._active_run_metadata:
            return []
        return [dict(self._active_run_metadata)]

    def request_stop_active_run(self) -> bool:
        if not self.runner or not self.runner.isRunning() or not self._active_run_metadata:
            return False
        device_id = int(self._active_run_metadata.get("device_id") or 0)
        if device_id <= 0:
            return False
        self.workflow_service.request_stop_for_devices([device_id], reason="Stopped from Runtime page")
        self.run_status_label.setText("Stop requested for active workflow run.")
        self._active_run_metadata["status"] = "stopping"
        self._active_run_metadata["detail"] = "Stop requested from Runtime page"
        return True

    def _format_step_summary(self, parameters_text: str) -> str:
        try:
            parameters = json.loads(parameters_text or "{}")
        except json.JSONDecodeError:
            compact_params = " ".join((parameters_text or "").split())
            return compact_params[:120] + ("..." if len(compact_params) > 120 else "")

        flow_flags: list[str] = []
        if str(parameters.get("run_if_expression", "")).strip():
            flow_flags.append("if")
        if int(parameters.get("repeat_times", 1) or 1) > 1:
            flow_flags.append(f"x{int(parameters['repeat_times'])}")
        if str(parameters.get("result_variable", "")).strip():
            flow_flags.append(f"->{parameters['result_variable']}")

        compact_params = " ".join(json.dumps(parameters, ensure_ascii=False).split())
        compact_params = compact_params[:120] + ("..." if len(compact_params) > 120 else "")
        return f"[{' '.join(flow_flags)}] {compact_params}" if flow_flags else compact_params
