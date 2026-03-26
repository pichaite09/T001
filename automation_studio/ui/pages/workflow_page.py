from __future__ import annotations

import json

from PySide6 import QtCore, QtGui, QtWidgets

from automation_studio.models import STEP_DEFINITIONS, STEP_DEFINITION_MAP
from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class WorkflowRunner(QtCore.QThread):
    result_ready = QtCore.Signal(dict)

    def __init__(self, workflow_service, workflow_id: int, device_id: int) -> None:
        super().__init__()
        self.workflow_service = workflow_service
        self.workflow_id = workflow_id
        self.device_id = device_id

    def run(self) -> None:
        result = self.workflow_service.execute_workflow(self.workflow_id, self.device_id)
        self.result_ready.emit(result)


class StepEditorDialog(QtWidgets.QDialog):
    def __init__(
        self,
        parent: QtWidgets.QWidget | None = None,
        step_data: dict | None = None,
        default_position: int = 1,
    ) -> None:
        super().__init__(parent)
        self.step_data = step_data
        self.setWindowTitle("Step Editor")
        self.setModal(True)
        self.resize(760, 620)
        self._build_ui()
        self._load_data(default_position)

    def _build_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(12)

        title = QtWidgets.QLabel("Step Editor")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("แก้ไขรายละเอียด step ใน popup เพื่อให้หน้า workflow มีพื้นที่มากขึ้น")
        subtitle.setObjectName("subtitleLabel")
        layout.addWidget(title)
        layout.addWidget(subtitle)

        grid = QtWidgets.QGridLayout()
        grid.setHorizontalSpacing(12)
        grid.setVerticalSpacing(10)

        grid.addWidget(make_form_label("Position"), 0, 0)
        self.step_position_input = QtWidgets.QSpinBox()
        self.step_position_input.setMinimum(1)
        self.step_position_input.setMaximum(999)
        grid.addWidget(self.step_position_input, 0, 1)

        grid.addWidget(make_form_label("Name"), 1, 0)
        self.step_name_input = QtWidgets.QLineEdit()
        grid.addWidget(self.step_name_input, 1, 1)

        grid.addWidget(make_form_label("Step Type"), 2, 0)
        self.step_type_combo = QtWidgets.QComboBox()
        for definition in STEP_DEFINITIONS:
            self.step_type_combo.addItem(f"{definition.label} ({definition.key})", definition.key)
        grid.addWidget(self.step_type_combo, 2, 1)
        layout.addLayout(grid)

        options_row = QtWidgets.QHBoxLayout()
        self.template_button = make_button("Apply Template", "secondary")
        self.step_enabled_check = QtWidgets.QCheckBox("Enabled")
        self.step_enabled_check.setChecked(True)
        options_row.addWidget(self.template_button)
        options_row.addWidget(self.step_enabled_check)
        options_row.addStretch(1)
        layout.addLayout(options_row)

        layout.addWidget(make_form_label("Parameters (JSON)"))
        self.parameters_input = QtWidgets.QPlainTextEdit()
        self.parameters_input.setPlaceholderText('{\n  "text": "Login"\n}')
        self.parameters_input.setMinimumHeight(260)
        layout.addWidget(self.parameters_input, 1)

        self.step_hint_label = QtWidgets.QLabel()
        self.step_hint_label.setObjectName("subtitleLabel")
        self.step_hint_label.setWordWrap(True)
        layout.addWidget(self.step_hint_label)

        actions = QtWidgets.QHBoxLayout()
        self.cancel_button = make_button("Cancel", "secondary")
        self.save_button = make_button("Save Step")
        actions.addStretch(1)
        actions.addWidget(self.cancel_button)
        actions.addWidget(self.save_button)
        layout.addLayout(actions)

        self.template_button.clicked.connect(self.apply_step_template)
        self.step_type_combo.currentIndexChanged.connect(self._on_step_type_changed)
        self.cancel_button.clicked.connect(self.reject)
        self.save_button.clicked.connect(self._validate_and_accept)

    def _load_data(self, default_position: int) -> None:
        if self.step_data:
            self.step_position_input.setValue(int(self.step_data["position"]))
            self.step_name_input.setText(self.step_data["name"])
            index = self.step_type_combo.findData(self.step_data["step_type"])
            if index >= 0:
                self.step_type_combo.setCurrentIndex(index)
            self.parameters_input.setPlainText(self.step_data["parameters"])
            self.step_enabled_check.setChecked(bool(self.step_data["is_enabled"]))
        else:
            self.step_position_input.setValue(default_position)
            self.step_enabled_check.setChecked(True)
            self.apply_step_template(force=True)
        self._update_step_hint()

    def _on_step_type_changed(self) -> None:
        self._update_step_hint()
        if not self.step_data and not self.step_name_input.text().strip():
            self.step_name_input.setText(STEP_DEFINITION_MAP[self.step_type_combo.currentData()].label)

    def _update_step_hint(self) -> None:
        definition = STEP_DEFINITION_MAP[self.step_type_combo.currentData()]
        self.step_hint_label.setText(f"{definition.description}\nSuggested JSON: {definition.template_json()}")

    def apply_step_template(self, force: bool = False) -> None:
        definition = STEP_DEFINITION_MAP[self.step_type_combo.currentData()]
        self.parameters_input.setPlainText(definition.template_json())
        if force or not self.step_name_input.text().strip():
            self.step_name_input.setText(definition.label)
        self._update_step_hint()

    def _validate_and_accept(self) -> None:
        if not self.step_name_input.text().strip():
            QtWidgets.QMessageBox.warning(self, "Missing name", "กรุณากรอกชื่อ step")
            return
        try:
            json.loads(self.parameters_input.toPlainText().strip() or "{}")
        except json.JSONDecodeError as exc:
            QtWidgets.QMessageBox.warning(self, "Invalid JSON", str(exc))
            return
        self.accept()

    def payload(self) -> dict:
        return {
            "position": self.step_position_input.value(),
            "name": self.step_name_input.text().strip(),
            "step_type": self.step_type_combo.currentData(),
            "parameters": self.parameters_input.toPlainText().strip() or "{}",
            "is_enabled": self.step_enabled_check.isChecked(),
        }


class ReorderableStepTable(QtWidgets.QTableWidget):
    order_changed = QtCore.Signal(list, int)
    MIME_TYPE = "application/x-automation-studio-step-row"

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
            item = self.item(row, 1)
            if item:
                item.setText(str(row + 1))

    def _step_id_at_row(self, row: int) -> int:
        return int(self.item(row, 0).text())


class WorkflowPage(QtWidgets.QWidget):
    workflows_changed = QtCore.Signal()
    logs_changed = QtCore.Signal()

    def __init__(self, workflow_service, device_service, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.workflow_service = workflow_service
        self.device_service = device_service
        self.current_workflow_id: int | None = None
        self.current_step_id: int | None = None
        self.runner: WorkflowRunner | None = None
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
        subtitle = QtWidgets.QLabel("สร้าง flow การ automation, แก้ไข step แบบ popup และสั่งรันกับอุปกรณ์ที่เลือก")
        subtitle.setObjectName("subtitleLabel")
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
        self.workflow_delete_button = make_button("Delete", "danger")
        workflow_actions.addWidget(self.workflow_new_button)
        workflow_actions.addWidget(self.workflow_save_button)
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

        workflow_layout.addWidget(make_form_label("Workflows"))
        self.workflow_list = QtWidgets.QListWidget()
        workflow_layout.addWidget(self.workflow_list, 1)
        content.addWidget(workflow_card)

        steps_card = CardFrame()
        steps_layout = QtWidgets.QVBoxLayout(steps_card)
        steps_layout.setContentsMargins(18, 18, 18, 18)
        steps_layout.setSpacing(12)

        top_actions = QtWidgets.QHBoxLayout()
        self.device_combo = QtWidgets.QComboBox()
        self.run_button = make_button("Run Workflow")
        top_actions.addWidget(self.device_combo, 1)
        top_actions.addWidget(self.run_button)
        steps_layout.addLayout(top_actions)

        step_toolbar = QtWidgets.QHBoxLayout()
        self.add_step_button = make_button("Add Step")
        self.move_up_button = make_button("Move Up", "secondary")
        self.move_down_button = make_button("Move Down", "secondary")
        self.edit_step_button = make_button("Edit Selected", "secondary")
        self.delete_step_button = make_button("Delete Step", "danger")
        step_toolbar.addWidget(self.add_step_button)
        step_toolbar.addWidget(self.move_up_button)
        step_toolbar.addWidget(self.move_down_button)
        step_toolbar.addWidget(self.edit_step_button)
        step_toolbar.addWidget(self.delete_step_button)
        step_toolbar.addStretch(1)
        steps_layout.addLayout(step_toolbar)

        self.steps_table = ReorderableStepTable()
        self.steps_table.setColumnCount(5)
        self.steps_table.setHorizontalHeaderLabels(["ID", "Pos", "Name", "Type", "Parameters"])
        self.steps_table.verticalHeader().setVisible(False)
        self.steps_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.steps_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.steps_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.steps_table.horizontalHeader().setStretchLastSection(True)
        self.steps_table.setColumnHidden(0, True)
        self.steps_table.setAlternatingRowColors(True)
        steps_layout.addWidget(self.steps_table, 1)

        hint = QtWidgets.QLabel("ลากแถวเพื่อจัดลำดับได้ หรือใช้ Move Up / Move Down เป็นทางสำรอง")
        hint.setObjectName("subtitleLabel")
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
        self.workflow_delete_button.clicked.connect(self.delete_workflow)
        self.workflow_list.itemSelectionChanged.connect(self._on_workflow_selected)

        self.add_step_button.clicked.connect(self.open_new_step_dialog)
        self.move_up_button.clicked.connect(self.move_selected_step_up)
        self.move_down_button.clicked.connect(self.move_selected_step_down)
        self.edit_step_button.clicked.connect(self.open_edit_step_dialog)
        self.delete_step_button.clicked.connect(self.delete_step)
        self.steps_table.itemSelectionChanged.connect(self._on_step_selected)
        self.steps_table.itemDoubleClicked.connect(lambda *_: self.open_edit_step_dialog())
        self.steps_table.order_changed.connect(self._persist_step_order)
        self.run_button.clicked.connect(self.run_workflow)

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
        self.load_steps()
        self.run_status_label.setText("พร้อมรัน workflow หรือเพิ่ม step ใหม่")

    def clear_workflow_form(self) -> None:
        self.current_workflow_id = None
        self.current_step_id = None
        self.workflow_list.clearSelection()
        self.workflow_name_input.clear()
        self.workflow_description_input.clear()
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
        self.run_status_label.setText("บันทึก workflow เรียบร้อย")

    def _select_workflow_item(self, workflow_id: int) -> None:
        for index in range(self.workflow_list.count()):
            item = self.workflow_list.item(index)
            if item.data(QtCore.Qt.ItemDataRole.UserRole) == workflow_id:
                self.workflow_list.setCurrentItem(item)
                break

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
        if not self.current_workflow_id:
            self.steps_table.setRowCount(0)
            self._sync_step_actions()
            return
        steps = self.workflow_service.list_steps(self.current_workflow_id)
        self.steps_table.setRowCount(len(steps))
        for row_index, step in enumerate(steps):
            compact_params = " ".join(step["parameters"].split())
            values = [
                str(step["id"]),
                str(step["position"]),
                step["name"],
                step["step_type"],
                compact_params[:100] + ("..." if len(compact_params) > 100 else ""),
            ]
            for column, value in enumerate(values):
                self.steps_table.setItem(row_index, column, QtWidgets.QTableWidgetItem(value))
        self.steps_table.clearSelection()
        self.steps_table.resizeColumnsToContents()
        self._sync_step_actions()

    def _selected_step(self) -> dict | None:
        if not self.current_workflow_id or not self.current_step_id:
            return None
        return next(
            (
                item
                for item in self.workflow_service.list_steps(self.current_workflow_id)
                if item["id"] == self.current_step_id
            ),
            None,
        )

    def _on_step_selected(self) -> None:
        row = self.steps_table.currentRow()
        self.current_step_id = int(self.steps_table.item(row, 0).text()) if row >= 0 else None
        self._sync_step_actions()

    def _sync_step_actions(self) -> None:
        has_workflow = self.current_workflow_id is not None
        has_step = self.current_step_id is not None
        current_row = self.steps_table.currentRow()
        self.add_step_button.setEnabled(has_workflow)
        self.move_up_button.setEnabled(has_step and current_row > 0)
        self.move_down_button.setEnabled(has_step and 0 <= current_row < self.steps_table.rowCount() - 1)
        self.edit_step_button.setEnabled(has_step)
        self.delete_step_button.setEnabled(has_step)

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
        )
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return

        payload = dialog.payload()
        step_id = self.workflow_service.save_step(
            step["id"] if step else None,
            self.current_workflow_id,
            payload["position"],
            payload["name"],
            payload["step_type"],
            payload["parameters"],
            payload["is_enabled"],
        )
        self.load_steps()
        self._select_step_row(step_id)
        self.run_status_label.setText("บันทึก step เรียบร้อย")

    def _select_step_row(self, step_id: int) -> None:
        for row in range(self.steps_table.rowCount()):
            if int(self.steps_table.item(row, 0).text()) == step_id:
                self.steps_table.selectRow(row)
                self.current_step_id = step_id
                break
        self._sync_step_actions()

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
        self.run_button.setDisabled(True)
        self.run_status_label.setText("กำลังรัน workflow บนอุปกรณ์ที่เลือก...")
        self.runner = WorkflowRunner(self.workflow_service, self.current_workflow_id, device_id)
        self.runner.result_ready.connect(self._on_workflow_finished)
        self.runner.finished.connect(lambda: self.run_button.setDisabled(False))
        self.runner.start()

    def _on_workflow_finished(self, result: dict) -> None:
        self.run_status_label.setText(result["message"])
        self.logs_changed.emit()
        if result.get("success"):
            QtWidgets.QMessageBox.information(self, "Workflow completed", result["message"])
        else:
            QtWidgets.QMessageBox.warning(self, "Workflow failed", result["message"])
