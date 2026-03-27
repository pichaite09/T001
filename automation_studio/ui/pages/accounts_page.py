from __future__ import annotations

import json

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class AccountsPage(QtWidgets.QWidget):
    accounts_changed = QtCore.Signal()

    def __init__(self, account_service, device_service, workflow_service, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.account_service = account_service
        self.device_service = device_service
        self.workflow_service = workflow_service
        self.current_device_id: int | None = None
        self.current_device_platform_id: int | None = None
        self.current_account_id: int | None = None
        self._build_ui()
        self.refresh_devices()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(16)

        title = QtWidgets.QLabel("Accounts")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Map each device to multiple platforms and keep multiple accounts per platform.")
        subtitle.setObjectName("subtitleLabel")
        root.addWidget(title)
        root.addWidget(subtitle)

        device_row = QtWidgets.QHBoxLayout()
        device_row.addWidget(make_form_label("Device"))
        self.device_combo = QtWidgets.QComboBox()
        self.refresh_button = make_button("Refresh", "secondary")
        device_row.addWidget(self.device_combo, 1)
        device_row.addWidget(self.refresh_button)
        root.addLayout(device_row)

        content = QtWidgets.QSplitter()
        content.setChildrenCollapsible(False)
        root.addWidget(content, 1)

        platform_card = CardFrame()
        platform_layout = QtWidgets.QVBoxLayout(platform_card)
        platform_layout.setContentsMargins(18, 18, 18, 18)
        platform_layout.setSpacing(10)
        platform_layout.addWidget(make_form_label("Platforms On Device"))

        self.platform_table = QtWidgets.QTableWidget(0, 6)
        self.platform_table.setHorizontalHeaderLabels(["ID", "Key", "Platform", "Package", "Switch Workflow", "Current Account"])
        self.platform_table.verticalHeader().setVisible(False)
        self.platform_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.platform_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.platform_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.platform_table.setColumnHidden(0, True)
        self.platform_table.horizontalHeader().setStretchLastSection(True)
        platform_layout.addWidget(self.platform_table, 1)

        platform_form = QtWidgets.QFormLayout()
        self.platform_key_input = QtWidgets.QLineEdit()
        self.platform_name_input = QtWidgets.QLineEdit()
        self.package_name_input = QtWidgets.QLineEdit()
        self.switch_workflow_combo = QtWidgets.QComboBox()
        self.platform_enabled_check = QtWidgets.QCheckBox("Enabled")
        self.platform_enabled_check.setChecked(True)
        platform_form.addRow("Platform Key", self.platform_key_input)
        platform_form.addRow("Platform Name", self.platform_name_input)
        platform_form.addRow("Package Name", self.package_name_input)
        platform_form.addRow("Switch Workflow", self.switch_workflow_combo)
        platform_form.addRow("", self.platform_enabled_check)
        platform_layout.addLayout(platform_form)

        platform_actions = QtWidgets.QHBoxLayout()
        self.new_platform_button = make_button("New Platform", "secondary")
        self.save_platform_button = make_button("Save Platform")
        self.delete_platform_button = make_button("Delete Platform", "danger")
        platform_actions.addWidget(self.new_platform_button)
        platform_actions.addWidget(self.save_platform_button)
        platform_actions.addWidget(self.delete_platform_button)
        platform_layout.addLayout(platform_actions)
        content.addWidget(platform_card)

        account_card = CardFrame()
        account_layout = QtWidgets.QVBoxLayout(account_card)
        account_layout.setContentsMargins(18, 18, 18, 18)
        account_layout.setSpacing(10)
        account_layout.addWidget(make_form_label("Accounts In Platform"))

        self.account_table = QtWidgets.QTableWidget(0, 7)
        self.account_table.setHorizontalHeaderLabels(["ID", "Display Name", "Username", "Login ID", "Aliases", "State", "Current"])
        self.account_table.verticalHeader().setVisible(False)
        self.account_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.account_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.account_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.account_table.setColumnHidden(0, True)
        self.account_table.horizontalHeader().setStretchLastSection(True)
        account_layout.addWidget(self.account_table, 1)

        account_form = QtWidgets.QFormLayout()
        self.display_name_input = QtWidgets.QLineEdit()
        self.username_input = QtWidgets.QLineEdit()
        self.login_id_input = QtWidgets.QLineEdit()
        self.aliases_input = QtWidgets.QPlainTextEdit()
        self.aliases_input.setPlaceholderText("@alias-one, alias-two")
        self.aliases_input.setFixedHeight(70)
        self.metadata_input = QtWidgets.QPlainTextEdit()
        self.metadata_input.setFixedHeight(90)
        self.notes_input = QtWidgets.QPlainTextEdit()
        self.notes_input.setFixedHeight(90)
        self.account_enabled_check = QtWidgets.QCheckBox("Enabled")
        self.account_enabled_check.setChecked(True)
        account_form.addRow("Display Name", self.display_name_input)
        account_form.addRow("Username", self.username_input)
        account_form.addRow("Login ID", self.login_id_input)
        account_form.addRow("Aliases", self.aliases_input)
        account_form.addRow("Metadata JSON", self.metadata_input)
        account_form.addRow("Notes", self.notes_input)
        account_form.addRow("", self.account_enabled_check)
        account_layout.addLayout(account_form)

        account_actions = QtWidgets.QHBoxLayout()
        self.new_account_button = make_button("New Account", "secondary")
        self.set_current_button = make_button("Set Current", "secondary")
        self.save_account_button = make_button("Save Account")
        self.delete_account_button = make_button("Delete Account", "danger")
        account_actions.addWidget(self.new_account_button)
        account_actions.addWidget(self.set_current_button)
        account_actions.addWidget(self.save_account_button)
        account_actions.addWidget(self.delete_account_button)
        account_layout.addLayout(account_actions)
        content.addWidget(account_card)

        content.setStretchFactor(0, 3)
        content.setStretchFactor(1, 4)

        self.status_label = QtWidgets.QLabel("Select a device to manage its platforms and accounts.")
        self.status_label.setObjectName("subtitleLabel")
        self.status_label.setWordWrap(True)
        root.addWidget(self.status_label)

        self.device_combo.currentIndexChanged.connect(self._on_device_changed)
        self.refresh_button.clicked.connect(self._refresh_current_device)
        self.platform_table.itemSelectionChanged.connect(self._on_platform_selected)
        self.account_table.itemSelectionChanged.connect(self._on_account_selected)
        self.new_platform_button.clicked.connect(self._clear_platform_form)
        self.save_platform_button.clicked.connect(self.save_platform)
        self.delete_platform_button.clicked.connect(self.delete_platform)
        self.new_account_button.clicked.connect(self._clear_account_form)
        self.set_current_button.clicked.connect(self.set_current_account)
        self.save_account_button.clicked.connect(self.save_account)
        self.delete_account_button.clicked.connect(self.delete_account)
        self._sync_actions()

    def refresh_devices(self) -> None:
        current_device = self.device_combo.currentData()
        self.device_combo.blockSignals(True)
        self.device_combo.clear()
        for device in self.device_service.list_devices():
            self.device_combo.addItem(f"{device['name']} ({device['serial']})", device["id"])
        self.device_combo.blockSignals(False)
        if current_device is not None:
            index = self.device_combo.findData(current_device)
            if index >= 0:
                self.device_combo.setCurrentIndex(index)
            elif self.device_combo.count() > 0:
                self.device_combo.setCurrentIndex(0)
        elif self.device_combo.count() > 0:
            self.device_combo.setCurrentIndex(0)
        else:
            self.current_device_id = None
            self.load_platforms()
        self.refresh_workflows()

    def refresh_workflows(self) -> None:
        current_workflow = self.switch_workflow_combo.currentData()
        self.switch_workflow_combo.clear()
        self.switch_workflow_combo.addItem("None", None)
        for workflow in self.workflow_service.list_workflows():
            self.switch_workflow_combo.addItem(workflow["name"], workflow["id"])
        if current_workflow is not None:
            index = self.switch_workflow_combo.findData(current_workflow)
            if index >= 0:
                self.switch_workflow_combo.setCurrentIndex(index)

    def _refresh_current_device(self) -> None:
        self.refresh_devices()
        if self.current_device_id:
            self.load_platforms()

    def _on_device_changed(self) -> None:
        self.current_device_id = self.device_combo.currentData()
        self.current_device_platform_id = None
        self.current_account_id = None
        self._clear_platform_form()
        self._clear_account_form()
        self.load_platforms()

    def load_platforms(self) -> None:
        self.platform_table.setRowCount(0)
        self.account_table.setRowCount(0)
        if not self.current_device_id:
            self._sync_actions()
            return
        platforms = self.account_service.list_device_platforms(int(self.current_device_id))
        self.platform_table.setRowCount(len(platforms))
        for row, platform in enumerate(platforms):
            values = [
                platform["id"],
                platform["platform_key"],
                platform["platform_name"],
                platform.get("package_name") or "-",
                platform.get("switch_workflow_name") or "-",
                platform.get("current_account_name") or "-",
            ]
            for column, value in enumerate(values):
                self.platform_table.setItem(row, column, QtWidgets.QTableWidgetItem(str(value)))
        self.platform_table.resizeColumnsToContents()
        self._sync_actions()

    def load_accounts(self) -> None:
        self.account_table.setRowCount(0)
        if not self.current_device_platform_id:
            self._sync_actions()
            return
        accounts = self.account_service.list_accounts(int(self.current_device_platform_id))
        self.account_table.setRowCount(len(accounts))
        for row, account in enumerate(accounts):
            values = [
                account["id"],
                account["display_name"],
                account.get("username") or "",
                account.get("login_id") or "",
                str(account.get("alias_names") or "").replace("\n", ", "),
                "Enabled" if account["is_enabled"] else "Disabled",
                "Current" if int(account.get("is_current", 0) or 0) else "",
            ]
            for column, value in enumerate(values):
                self.account_table.setItem(row, column, QtWidgets.QTableWidgetItem(str(value)))
        self.account_table.resizeColumnsToContents()
        self._sync_actions()

    def _on_platform_selected(self) -> None:
        row = self.platform_table.currentRow()
        if row < 0:
            self.current_device_platform_id = None
            self._clear_platform_form()
            self.load_accounts()
            return
        self.current_device_platform_id = int(self.platform_table.item(row, 0).text())
        platform = self.account_service.get_device_platform(self.current_device_platform_id)
        if not platform:
            return
        self.platform_key_input.setText(str(platform.get("platform_key") or ""))
        self.platform_name_input.setText(str(platform.get("platform_name") or ""))
        self.package_name_input.setText(str(platform.get("package_name") or ""))
        self.platform_enabled_check.setChecked(bool(platform.get("is_enabled", 1)))
        workflow_index = self.switch_workflow_combo.findData(platform.get("switch_workflow_id"))
        self.switch_workflow_combo.setCurrentIndex(workflow_index if workflow_index >= 0 else 0)
        self.current_account_id = None
        self._clear_account_form()
        self.load_accounts()

    def _on_account_selected(self) -> None:
        row = self.account_table.currentRow()
        if row < 0:
            self.current_account_id = None
            self._clear_account_form()
            return
        self.current_account_id = int(self.account_table.item(row, 0).text())
        account = self.account_service.get_account(self.current_account_id)
        if not account:
            return
        self.display_name_input.setText(str(account.get("display_name") or ""))
        self.username_input.setText(str(account.get("username") or ""))
        self.login_id_input.setText(str(account.get("login_id") or ""))
        self.aliases_input.setPlainText(str(account.get("alias_names") or ""))
        self.notes_input.setPlainText(str(account.get("notes") or ""))
        self.metadata_input.setPlainText(str(account.get("metadata_json") or "{}"))
        self.account_enabled_check.setChecked(bool(account.get("is_enabled", 1)))
        self._sync_actions()

    def _clear_platform_form(self) -> None:
        self.current_device_platform_id = None
        self.platform_table.clearSelection()
        self.platform_key_input.clear()
        self.platform_name_input.clear()
        self.package_name_input.clear()
        self.platform_enabled_check.setChecked(True)
        self.switch_workflow_combo.setCurrentIndex(0)
        self.account_table.setRowCount(0)
        self._clear_account_form()
        self._sync_actions()

    def _clear_account_form(self) -> None:
        self.current_account_id = None
        self.account_table.clearSelection()
        self.display_name_input.clear()
        self.username_input.clear()
        self.login_id_input.clear()
        self.aliases_input.clear()
        self.metadata_input.setPlainText("{}")
        self.notes_input.clear()
        self.account_enabled_check.setChecked(True)
        self._sync_actions()

    def _selected_account_id(self) -> int | None:
        row = self.account_table.currentRow()
        if row < 0:
            return None
        return int(self.account_table.item(row, 0).text())

    def _sync_actions(self) -> None:
        has_device = self.current_device_id is not None
        has_platform = self.current_device_platform_id is not None
        has_account = self._selected_account_id() is not None or self.current_account_id is not None
        self.save_platform_button.setEnabled(has_device)
        self.delete_platform_button.setEnabled(has_platform)
        self.save_account_button.setEnabled(has_platform)
        self.new_account_button.setEnabled(has_platform)
        self.set_current_button.setEnabled(has_account)
        self.delete_account_button.setEnabled(has_account)

    def save_platform(self) -> None:
        if not self.current_device_id:
            QtWidgets.QMessageBox.warning(self, "Accounts", "Select a device first.")
            return
        try:
            device_platform_id = self.account_service.save_device_platform(
                self.current_device_platform_id,
                int(self.current_device_id),
                self.platform_key_input.text().strip(),
                self.platform_name_input.text().strip(),
                self.package_name_input.text().strip(),
                self.switch_workflow_combo.currentData(),
                self.platform_enabled_check.isChecked(),
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save platform failed", str(exc))
            return
        self.status_label.setText("Platform saved.")
        self.load_platforms()
        self._select_table_row(self.platform_table, device_platform_id)
        self.accounts_changed.emit()

    def delete_platform(self) -> None:
        if not self.current_device_platform_id:
            return
        if QtWidgets.QMessageBox.question(self, "Delete Platform", "Delete this platform and all its accounts?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.account_service.delete_device_platform(int(self.current_device_platform_id))
        self.current_device_platform_id = None
        self._clear_platform_form()
        self.load_platforms()
        self.load_accounts()
        self.status_label.setText("Platform deleted.")
        self.accounts_changed.emit()

    def save_account(self) -> None:
        if not self.current_device_platform_id:
            QtWidgets.QMessageBox.warning(self, "Accounts", "Select or create a platform first.")
            return
        try:
            account_id = self.account_service.save_account(
                self.current_account_id,
                int(self.current_device_platform_id),
                self.display_name_input.text().strip(),
                self.username_input.text().strip(),
                self.login_id_input.text().strip(),
                self.notes_input.toPlainText().strip(),
                self.metadata_input.toPlainText().strip(),
                self.account_enabled_check.isChecked(),
                aliases_text=self.aliases_input.toPlainText().strip(),
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save account failed", str(exc))
            return
        self.status_label.setText("Account saved.")
        self.load_accounts()
        self._select_table_row(self.account_table, account_id)
        self.load_platforms()
        self.accounts_changed.emit()

    def delete_account(self) -> None:
        account_id = self._selected_account_id()
        if not account_id:
            return
        if QtWidgets.QMessageBox.question(self, "Delete Account", "Delete this account?") != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.account_service.delete_account(account_id)
        self._clear_account_form()
        self.load_accounts()
        self.load_platforms()
        self.status_label.setText("Account deleted.")
        self.accounts_changed.emit()

    def set_current_account(self) -> None:
        account_id = self._selected_account_id()
        if not self.current_device_platform_id or not account_id:
            QtWidgets.QMessageBox.information(self, "Accounts", "Select an account first.")
            return
        self.account_service.set_current_account(int(self.current_device_platform_id), account_id)
        self.load_accounts()
        self.load_platforms()
        self.status_label.setText("Current account updated.")
        self.accounts_changed.emit()

    def _select_table_row(self, table: QtWidgets.QTableWidget, object_id: int) -> None:
        for row in range(table.rowCount()):
            if int(table.item(row, 0).text()) == int(object_id):
                table.selectRow(row)
                break
