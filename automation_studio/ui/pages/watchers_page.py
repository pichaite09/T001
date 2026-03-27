from __future__ import annotations

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.watcher_editor import WatcherEditorDialog
from automation_studio.ui.widgets import CardFrame, make_button


class WatchersPage(QtWidgets.QWidget):
    watchers_changed = QtCore.Signal()
    logs_changed = QtCore.Signal()

    def __init__(self, watcher_service, workflow_service, device_service, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.watcher_service = watcher_service
        self.workflow_service = workflow_service
        self.device_service = device_service
        self._build_ui()
        self.load_watchers()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(16)

        title = QtWidgets.QLabel("Watchers")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Runtime guards for popup handling, app recovery, and automatic safety actions while workflows run.")
        subtitle.setObjectName("subtitleLabel")
        root.addWidget(title)
        root.addWidget(subtitle)

        card = CardFrame()
        card_layout = QtWidgets.QVBoxLayout(card)
        card_layout.setContentsMargins(18, 18, 18, 18)
        card_layout.setSpacing(12)

        actions = QtWidgets.QHBoxLayout()
        self.new_button = make_button("New Watcher")
        self.edit_button = make_button("Edit Selected", "secondary")
        self.delete_button = make_button("Delete", "danger")
        self.refresh_button = make_button("Refresh", "secondary")
        actions.addWidget(self.new_button)
        actions.addWidget(self.edit_button)
        actions.addWidget(self.delete_button)
        actions.addStretch(1)
        actions.addWidget(self.refresh_button)
        card_layout.addLayout(actions)

        self.table = QtWidgets.QTableWidget(0, 7)
        self.table.setHorizontalHeaderLabels(["ID", "Name", "Scope", "Condition", "Action", "Priority", "State"])
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setStretchLastSection(True)
        card_layout.addWidget(self.table)
        root.addWidget(card, 1)

        self.new_button.clicked.connect(self._create_watcher)
        self.edit_button.clicked.connect(self._edit_selected_watcher)
        self.delete_button.clicked.connect(self._delete_selected_watcher)
        self.refresh_button.clicked.connect(self.load_watchers)
        self.table.itemDoubleClicked.connect(lambda _: self._edit_selected_watcher())

    def load_watchers(self) -> None:
        workflows = {workflow["id"]: workflow["name"] for workflow in self.workflow_service.list_workflows()}
        devices = {device["id"]: device["name"] for device in self.device_service.list_devices()}
        watchers = self.watcher_service.list_watchers()
        self.table.setRowCount(len(watchers))

        for row_index, watcher in enumerate(watchers):
            scope_type = watcher["scope_type"]
            scope_id = watcher.get("scope_id")
            if scope_type == "global":
                scope_label = "Global"
            elif scope_type == "workflow":
                scope_label = f"Workflow: {workflows.get(scope_id, scope_id)}"
            else:
                scope_label = f"Device: {devices.get(scope_id, scope_id)}"

            values = [
                watcher["id"],
                watcher["name"],
                scope_label,
                watcher["condition_type"],
                watcher["action_type"],
                watcher["priority"],
                "Enabled" if watcher["is_enabled"] else "Disabled",
            ]
            for column, value in enumerate(values):
                self.table.setItem(row_index, column, QtWidgets.QTableWidgetItem(str(value)))
        self.table.resizeColumnsToContents()

    def _selected_watcher_id(self) -> int | None:
        current_row = self.table.currentRow()
        if current_row < 0:
            return None
        item = self.table.item(current_row, 0)
        if not item:
            return None
        return int(item.text())

    def _open_editor(self, watcher_data: dict | None = None) -> None:
        dialog = WatcherEditorDialog(
            workflows=self.workflow_service.list_workflows(),
            devices=self.device_service.list_devices(),
            watcher_service=self.watcher_service,
            parent=self,
            watcher_data=watcher_data,
        )
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return

        payload = dialog.payload()
        self.watcher_service.save_watcher(
            watcher_id=payload["id"],
            name=payload["name"],
            scope_type=payload["scope_type"],
            scope_id=payload["scope_id"],
            condition_type=payload["condition_type"],
            condition_text=payload["condition_text"],
            action_type=payload["action_type"],
            action_text=payload["action_text"],
            policy_text=payload["policy_text"],
            is_enabled=payload["is_enabled"],
            priority=payload["priority"],
        )
        self.load_watchers()
        self.watchers_changed.emit()
        self.logs_changed.emit()

    def _create_watcher(self) -> None:
        self._open_editor(None)

    def _edit_selected_watcher(self) -> None:
        watcher_id = self._selected_watcher_id()
        if not watcher_id:
            QtWidgets.QMessageBox.information(self, "Watchers", "Select a watcher first.")
            return
        watcher = self.watcher_service.get_watcher(watcher_id)
        if not watcher:
            QtWidgets.QMessageBox.warning(self, "Watchers", "Watcher not found.")
            return
        self._open_editor(watcher)

    def _delete_selected_watcher(self) -> None:
        watcher_id = self._selected_watcher_id()
        if not watcher_id:
            QtWidgets.QMessageBox.information(self, "Watchers", "Select a watcher first.")
            return
        if QtWidgets.QMessageBox.question(self, "Delete Watcher", "Delete selected watcher?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.watcher_service.delete_watcher(watcher_id)
        self.load_watchers()
        self.watchers_changed.emit()
        self.logs_changed.emit()
