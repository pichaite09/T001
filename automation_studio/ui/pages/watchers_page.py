from __future__ import annotations

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.watcher_profile_dialog import WatcherProfileEditorDialog
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
        self.refresh_all()

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

        profiles_card = CardFrame()
        profiles_layout = QtWidgets.QVBoxLayout(profiles_card)
        profiles_layout.setContentsMargins(18, 18, 18, 18)
        profiles_layout.setSpacing(12)

        profiles_header = QtWidgets.QHBoxLayout()
        profiles_title = QtWidgets.QLabel("Watcher Profile Templates")
        profiles_title.setObjectName("subtitleLabel")
        profiles_header.addWidget(profiles_title)
        profiles_header.addStretch(1)
        self.new_profile_button = make_button("New Profile")
        self.edit_profile_button = make_button("Edit Profile", "secondary")
        self.delete_profile_button = make_button("Delete Profile", "danger")
        self.refresh_profiles_button = make_button("Refresh Profiles", "secondary")
        profiles_header.addWidget(self.new_profile_button)
        profiles_header.addWidget(self.edit_profile_button)
        profiles_header.addWidget(self.delete_profile_button)
        profiles_header.addWidget(self.refresh_profiles_button)
        profiles_layout.addLayout(profiles_header)

        self.profile_table = QtWidgets.QTableWidget(0, 5)
        self.profile_table.setHorizontalHeaderLabels(["ID", "Name", "Watchers", "State", "Description"])
        self.profile_table.verticalHeader().setVisible(False)
        self.profile_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.profile_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.profile_table.horizontalHeader().setStretchLastSection(True)
        profiles_layout.addWidget(self.profile_table)
        root.addWidget(profiles_card, 1)

        self.new_button.clicked.connect(self._create_watcher)
        self.edit_button.clicked.connect(self._edit_selected_watcher)
        self.delete_button.clicked.connect(self._delete_selected_watcher)
        self.refresh_button.clicked.connect(self.refresh_all)
        self.table.itemDoubleClicked.connect(lambda _: self._edit_selected_watcher())
        self.new_profile_button.clicked.connect(self._create_profile)
        self.edit_profile_button.clicked.connect(self._edit_selected_profile)
        self.delete_profile_button.clicked.connect(self._delete_selected_profile)
        self.refresh_profiles_button.clicked.connect(self.load_profiles)
        self.profile_table.itemDoubleClicked.connect(lambda _: self._edit_selected_profile())

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

    def load_profiles(self) -> None:
        profiles = self.watcher_service.list_profiles()
        self.profile_table.setRowCount(len(profiles))
        for row_index, profile in enumerate(profiles):
            values = [
                profile["id"],
                profile["name"],
                int(profile.get("watcher_count", 0) or 0),
                "Active" if profile["is_active"] else "Inactive",
                profile.get("description") or "",
            ]
            for column, value in enumerate(values):
                self.profile_table.setItem(row_index, column, QtWidgets.QTableWidgetItem(str(value)))
        self.profile_table.resizeColumnsToContents()

    def refresh_all(self) -> None:
        self.load_watchers()
        self.load_profiles()

    def _selected_watcher_id(self) -> int | None:
        current_row = self.table.currentRow()
        if current_row < 0:
            return None
        item = self.table.item(current_row, 0)
        if not item:
            return None
        return int(item.text())

    def _selected_profile_id(self) -> int | None:
        current_row = self.profile_table.currentRow()
        if current_row < 0:
            return None
        item = self.profile_table.item(current_row, 0)
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
        self.refresh_all()
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
        self.refresh_all()
        self.watchers_changed.emit()
        self.logs_changed.emit()

    def _open_profile_editor(self, profile_data: dict | None = None) -> None:
        available_watchers = self.watcher_service.list_watchers()
        if not profile_data and not available_watchers:
            QtWidgets.QMessageBox.information(
                self,
                "Watcher Profiles",
                "Create at least one watcher before creating a watcher profile template.",
            )
            return
        selected_watcher_ids = (
            [watcher["id"] for watcher in self.watcher_service.list_profile_watchers(profile_data["id"])]
            if profile_data
            else []
        )
        dialog = WatcherProfileEditorDialog(
            watchers=available_watchers,
            parent=self,
            profile_data=profile_data,
            selected_watcher_ids=selected_watcher_ids,
        )
        if dialog.exec() != QtWidgets.QDialog.DialogCode.Accepted:
            return
        payload = dialog.payload()
        try:
            self.watcher_service.save_profile(
                profile_id=payload["id"],
                name=payload["name"],
                description=payload["description"],
                watcher_ids=payload["watcher_ids"],
                is_active=payload["is_active"],
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save profile failed", str(exc))
            return
        self.load_profiles()
        self.watchers_changed.emit()
        self.logs_changed.emit()

    def _create_profile(self) -> None:
        self._open_profile_editor(None)

    def _edit_selected_profile(self) -> None:
        profile_id = self._selected_profile_id()
        if not profile_id:
            QtWidgets.QMessageBox.information(self, "Watcher Profiles", "Select a profile first.")
            return
        profile = self.watcher_service.get_profile(profile_id)
        if not profile:
            QtWidgets.QMessageBox.warning(self, "Watcher Profiles", "Profile not found.")
            return
        self._open_profile_editor(profile)

    def _delete_selected_profile(self) -> None:
        profile_id = self._selected_profile_id()
        if not profile_id:
            QtWidgets.QMessageBox.information(self, "Watcher Profiles", "Select a profile first.")
            return
        if QtWidgets.QMessageBox.question(self, "Delete Profile", "Delete selected watcher profile?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.watcher_service.delete_profile(profile_id)
        self.load_profiles()
        self.watchers_changed.emit()
        self.logs_changed.emit()
