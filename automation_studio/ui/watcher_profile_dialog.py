from __future__ import annotations

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button, make_form_label


class WatcherProfileEditorDialog(QtWidgets.QDialog):
    def __init__(
        self,
        watchers: list[dict],
        parent: QtWidgets.QWidget | None = None,
        profile_data: dict | None = None,
        selected_watcher_ids: list[int] | None = None,
    ) -> None:
        super().__init__(parent)
        self.watchers = watchers
        self.profile_data = profile_data
        self.selected_watcher_ids = {int(item) for item in (selected_watcher_ids or [])}
        self.setWindowTitle("Watcher Profile")
        self.setModal(True)
        self.resize(720, 640)
        self._build_ui()
        self._load_data()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title = QtWidgets.QLabel("Watcher Profile")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Create a reusable watcher template by grouping one or more watchers.")
        subtitle.setObjectName("subtitleLabel")
        root.addWidget(title)
        root.addWidget(subtitle)

        card = CardFrame()
        layout = QtWidgets.QVBoxLayout(card)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)

        layout.addWidget(make_form_label("Profile Name"))
        self.name_input = QtWidgets.QLineEdit()
        layout.addWidget(self.name_input)

        layout.addWidget(make_form_label("Description"))
        self.description_input = QtWidgets.QPlainTextEdit()
        self.description_input.setFixedHeight(96)
        layout.addWidget(self.description_input)

        self.active_check = QtWidgets.QCheckBox("Active")
        self.active_check.setChecked(True)
        layout.addWidget(self.active_check)

        layout.addWidget(make_form_label("Watchers In Profile"))
        self.watcher_list = QtWidgets.QListWidget()
        self.watcher_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.NoSelection)
        layout.addWidget(self.watcher_list, 1)
        root.addWidget(card, 1)

        actions = QtWidgets.QHBoxLayout()
        self.validation_label = QtWidgets.QLabel()
        self.validation_label.setObjectName("subtitleLabel")
        self.validation_label.setWordWrap(True)
        self.cancel_button = make_button("Cancel", "secondary")
        self.save_button = make_button("Save Profile")
        actions.addWidget(self.validation_label, 1)
        actions.addWidget(self.cancel_button)
        actions.addWidget(self.save_button)
        root.addLayout(actions)

        self.cancel_button.clicked.connect(self.reject)
        self.save_button.clicked.connect(self._validate_and_accept)
        self.name_input.textChanged.connect(self._clear_validation)

    def _load_data(self) -> None:
        if self.profile_data:
            self.name_input.setText(str(self.profile_data.get("name") or ""))
            self.description_input.setPlainText(str(self.profile_data.get("description") or ""))
            self.active_check.setChecked(bool(self.profile_data.get("is_active", 1)))

        if not self.watchers:
            item = QtWidgets.QListWidgetItem("Create at least one watcher first, then add it to a profile template.")
            item.setFlags(QtCore.Qt.ItemFlag.NoItemFlags)
            self.watcher_list.addItem(item)
            self.validation_label.setText("No watchers available yet")
            self.save_button.setEnabled(False)
            return

        self.save_button.setEnabled(True)
        for watcher in self.watchers:
            self._add_checkbox_row(
                self.watcher_list,
                f"{watcher['name']} [{watcher['condition_type']} -> {watcher['action_type']}]",
                watcher["id"],
                int(watcher["id"]) in self.selected_watcher_ids,
            )

    def _validate_and_accept(self) -> None:
        if not self.name_input.text().strip():
            self.validation_label.setText("Profile name is required")
            return
        if not self.selected_ids():
            self.validation_label.setText("Select at least one watcher")
            return
        self.accept()

    def _clear_validation(self) -> None:
        if self.watchers:
            self.validation_label.clear()

    def _add_checkbox_row(
        self,
        list_widget: QtWidgets.QListWidget,
        label: str,
        object_id: int,
        checked: bool,
    ) -> None:
        item = QtWidgets.QListWidgetItem()
        checkbox = QtWidgets.QCheckBox(label)
        checkbox.setChecked(checked)
        checkbox.setProperty("object_id", int(object_id))
        checkbox.toggled.connect(self._clear_validation)
        item.setSizeHint(checkbox.sizeHint())
        list_widget.addItem(item)
        list_widget.setItemWidget(item, checkbox)

    def selected_ids(self) -> list[int]:
        watcher_ids: list[int] = []
        for index in range(self.watcher_list.count()):
            item = self.watcher_list.item(index)
            checkbox = self.watcher_list.itemWidget(item)
            if isinstance(checkbox, QtWidgets.QCheckBox) and checkbox.isChecked():
                watcher_ids.append(int(checkbox.property("object_id")))
        return watcher_ids

    def payload(self) -> dict:
        return {
            "id": self.profile_data.get("id") if self.profile_data else None,
            "name": self.name_input.text().strip(),
            "description": self.description_input.toPlainText().strip(),
            "is_active": self.active_check.isChecked(),
            "watcher_ids": self.selected_ids(),
        }


class WorkflowProfileAttachDialog(QtWidgets.QDialog):
    def __init__(
        self,
        profiles: list[dict],
        selected_profile_ids: list[int],
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.profiles = profiles
        self.selected_profile_ids = {int(item) for item in selected_profile_ids}
        self.setWindowTitle("Attach Watcher Profiles")
        self.setModal(True)
        self.resize(640, 520)
        self._build_ui()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(18, 18, 18, 18)
        root.setSpacing(12)

        title = QtWidgets.QLabel("Attach Watcher Profiles")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Choose reusable watcher profile templates for the selected workflow.")
        subtitle.setObjectName("subtitleLabel")
        root.addWidget(title)
        root.addWidget(subtitle)

        card = CardFrame()
        layout = QtWidgets.QVBoxLayout(card)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(10)
        self.profile_list = QtWidgets.QListWidget()
        self.profile_list.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.NoSelection)
        for profile in self.profiles:
            self._add_checkbox_row(
                self.profile_list,
                f"{profile['name']} ({int(profile.get('watcher_count', 0) or 0)} watchers)",
                profile["id"],
                int(profile["id"]) in self.selected_profile_ids,
            )
        layout.addWidget(self.profile_list)
        root.addWidget(card, 1)

        actions = QtWidgets.QHBoxLayout()
        self.cancel_button = make_button("Cancel", "secondary")
        self.save_button = make_button("Save Profiles")
        actions.addStretch(1)
        actions.addWidget(self.cancel_button)
        actions.addWidget(self.save_button)
        root.addLayout(actions)

        self.cancel_button.clicked.connect(self.reject)
        self.save_button.clicked.connect(self.accept)

    def selected_ids(self) -> list[int]:
        profile_ids: list[int] = []
        for index in range(self.profile_list.count()):
            item = self.profile_list.item(index)
            checkbox = self.profile_list.itemWidget(item)
            if isinstance(checkbox, QtWidgets.QCheckBox) and checkbox.isChecked():
                profile_ids.append(int(checkbox.property("object_id")))
        return profile_ids

    def _add_checkbox_row(
        self,
        list_widget: QtWidgets.QListWidget,
        label: str,
        object_id: int,
        checked: bool,
    ) -> None:
        item = QtWidgets.QListWidgetItem()
        checkbox = QtWidgets.QCheckBox(label)
        checkbox.setChecked(checked)
        checkbox.setProperty("object_id", int(object_id))
        item.setSizeHint(checkbox.sizeHint())
        list_widget.addItem(item)
        list_widget.setItemWidget(item, checkbox)
