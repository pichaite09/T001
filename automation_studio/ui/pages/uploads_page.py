from __future__ import annotations

import json
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button


class UploadRunThread(QtCore.QThread):
    result_ready = QtCore.Signal(int, dict)

    def __init__(self, upload_service, upload_job_id: int) -> None:
        super().__init__()
        self.upload_service = upload_service
        self.upload_job_id = upload_job_id

    def run(self) -> None:
        result = self.upload_service.execute_upload_job(self.upload_job_id)
        self.result_ready.emit(self.upload_job_id, result)


class UploadBatchRunThread(QtCore.QThread):
    result_ready = QtCore.Signal(dict)

    def __init__(self, upload_service, upload_job_ids: list[int], *, continue_on_error: bool = True) -> None:
        super().__init__()
        self.upload_service = upload_service
        self.upload_job_ids = [int(upload_job_id) for upload_job_id in upload_job_ids]
        self.continue_on_error = continue_on_error

    def run(self) -> None:
        result = self.upload_service.execute_upload_jobs(
            self.upload_job_ids,
            continue_on_error=self.continue_on_error,
        )
        self.result_ready.emit(result)


class UploadsPage(QtWidgets.QWidget):
    uploads_changed = QtCore.Signal()
    logs_changed = QtCore.Signal()

    def __init__(
        self,
        upload_service,
        workflow_service,
        device_service,
        account_service,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.upload_service = upload_service
        self.workflow_service = workflow_service
        self.device_service = device_service
        self.account_service = account_service
        self.current_upload_job_id: int | None = None
        self.current_device_id: int | None = None
        self._run_thread: UploadRunThread | None = None
        self._batch_run_thread: UploadBatchRunThread | None = None
        self._jobs_by_id: dict[int, dict] = {}
        self._templates_by_id: dict[int, dict] = {}
        self._settings = QtCore.QSettings("AutomationStudio", "UploadsPage")
        self._auto_run_timer = QtCore.QTimer(self)
        self._auto_run_timer.timeout.connect(self._check_auto_run_draft_jobs)
        self._build_ui()
        self._restore_auto_run_settings()
        self.refresh_devices()
        self.refresh_workflows()
        self.refresh_templates()
        self.load_upload_jobs()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(16)

        title = QtWidgets.QLabel("Uploads")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel(
            "Manage reusable upload templates, queue video jobs, and run platform workflows with full upload context."
        )
        subtitle.setObjectName("subtitleLabel")
        subtitle.setWordWrap(True)
        root.addWidget(title)
        root.addWidget(subtitle)

        summary_layout = QtWidgets.QHBoxLayout()
        summary_layout.setSpacing(10)
        self.summary_cards: dict[str, QtWidgets.QLabel] = {}
        for key, label in (
            ("total_jobs", "Upload Jobs"),
            ("draft_count", "Draft"),
            ("running_count", "Running"),
            ("success_count", "Success"),
            ("failed_count", "Failed"),
            ("template_count", "Templates"),
        ):
            card = CardFrame()
            card_layout = QtWidgets.QVBoxLayout(card)
            card_layout.setContentsMargins(14, 12, 14, 12)
            card_layout.setSpacing(4)
            caption = QtWidgets.QLabel(label)
            caption.setObjectName("subtitleLabel")
            value = QtWidgets.QLabel("0")
            value.setObjectName("titleLabel")
            value.setStyleSheet("font-size: 24px;")
            card_layout.addWidget(caption)
            card_layout.addWidget(value)
            summary_layout.addWidget(card, 1)
            self.summary_cards[key] = value
        root.addLayout(summary_layout)

        self.summary_insights_label = QtWidgets.QLabel("Top workflow: - | Top platform: - | Top account: -")
        self.summary_insights_label.setObjectName("subtitleLabel")
        self.summary_insights_label.setWordWrap(True)
        root.addWidget(self.summary_insights_label)

        splitter = QtWidgets.QSplitter()
        splitter.setChildrenCollapsible(False)
        splitter.setHandleWidth(8)
        root.addWidget(splitter, 1)

        left_card = CardFrame()
        left_layout = QtWidgets.QVBoxLayout(left_card)
        left_layout.setContentsMargins(18, 18, 18, 18)
        left_layout.setSpacing(10)

        template_card = CardFrame()
        template_layout = QtWidgets.QHBoxLayout(template_card)
        template_layout.setContentsMargins(14, 12, 14, 12)
        template_layout.setSpacing(10)
        template_label = QtWidgets.QLabel("Upload Template")
        template_label.setObjectName("subtitleLabel")
        self.template_combo = QtWidgets.QComboBox()
        self.template_combo.setMinimumWidth(240)
        self.apply_template_button = make_button("Apply Template", "secondary")
        self.save_template_button = make_button("Save Template", "secondary")
        self.delete_template_button = make_button("Delete Template", "danger")
        template_layout.addWidget(template_label)
        template_layout.addWidget(self.template_combo, 1)
        template_layout.addWidget(self.apply_template_button)
        template_layout.addWidget(self.save_template_button)
        template_layout.addWidget(self.delete_template_button)
        left_layout.addWidget(template_card)

        auto_card = CardFrame()
        auto_layout = QtWidgets.QHBoxLayout(auto_card)
        auto_layout.setContentsMargins(14, 12, 14, 12)
        auto_layout.setSpacing(10)
        auto_label = QtWidgets.QLabel("Auto Draft Runner")
        auto_label.setObjectName("subtitleLabel")
        self.auto_run_checkbox = QtWidgets.QCheckBox("Auto Run Draft Jobs")
        self.auto_run_interval_combo = QtWidgets.QComboBox()
        self.auto_run_interval_combo.addItem("5 sec", 5)
        self.auto_run_interval_combo.addItem("10 sec", 10)
        self.auto_run_interval_combo.addItem("30 sec", 30)
        self.auto_run_interval_combo.addItem("60 sec", 60)
        self.auto_run_state_label = QtWidgets.QLabel("Auto runner: off")
        self.auto_run_state_label.setObjectName("subtitleLabel")
        self.auto_run_state_label.setWordWrap(True)
        auto_layout.addWidget(auto_label)
        auto_layout.addWidget(self.auto_run_checkbox)
        auto_layout.addWidget(QtWidgets.QLabel("Check every"))
        auto_layout.addWidget(self.auto_run_interval_combo)
        auto_layout.addStretch(1)
        auto_layout.addWidget(self.auto_run_state_label, 1)
        left_layout.addWidget(auto_card)

        toolbar = QtWidgets.QHBoxLayout()
        self.new_button = make_button("New Upload", "secondary")
        self.save_button = make_button("Save Upload")
        self.run_now_button = make_button("Run Now", "secondary")
        self.run_selected_button = make_button("Run Selected", "secondary")
        self.run_all_button = make_button("Run All", "secondary")
        self.import_button = make_button("Import JSON", "secondary")
        self.export_button = make_button("Export JSON", "secondary")
        self.refresh_button = make_button("Refresh", "secondary")
        self.delete_button = make_button("Delete", "danger")
        for button in (
            self.new_button,
            self.save_button,
            self.run_now_button,
            self.run_selected_button,
            self.run_all_button,
            self.import_button,
            self.export_button,
            self.refresh_button,
            self.delete_button,
        ):
            toolbar.addWidget(button)
        toolbar.addStretch(1)
        left_layout.addLayout(toolbar)

        self.upload_table = QtWidgets.QTableWidget(0, 8)
        self.upload_table.setHorizontalHeaderLabels(
            ["ID", "Device", "Platform", "Account", "Workflow", "Code Product", "Title", "Status"]
        )
        self.upload_table.setColumnHidden(0, True)
        self.upload_table.verticalHeader().setVisible(False)
        self.upload_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.upload_table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection)
        self.upload_table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        self.upload_table.horizontalHeader().setStretchLastSection(True)
        left_layout.addWidget(self.upload_table, 1)

        self.status_label = QtWidgets.QLabel("Create an upload job, then run it through a reusable workflow.")
        self.status_label.setObjectName("subtitleLabel")
        self.status_label.setWordWrap(True)
        left_layout.addWidget(self.status_label)
        splitter.addWidget(left_card)

        right_card = CardFrame()
        right_card.setMinimumWidth(360)
        right_card.setMaximumWidth(520)
        form_layout = QtWidgets.QVBoxLayout(right_card)
        form_layout.setContentsMargins(18, 18, 18, 18)
        form_layout.setSpacing(10)

        form = QtWidgets.QFormLayout()
        self.device_combo = QtWidgets.QComboBox()
        self.platform_combo = QtWidgets.QComboBox()
        self.account_combo = QtWidgets.QComboBox()
        self.workflow_combo = QtWidgets.QComboBox()
        self.code_product_input = QtWidgets.QLineEdit()
        self.link_product_input = QtWidgets.QLineEdit()
        self.title_input = QtWidgets.QLineEdit()
        self.description_input = QtWidgets.QPlainTextEdit()
        self.description_input.setFixedHeight(96)
        self.tags_input = QtWidgets.QPlainTextEdit()
        self.tags_input.setFixedHeight(68)
        self.tags_input.setPlaceholderText("tag1, tag2, tag3")
        self.video_url_input = QtWidgets.QLineEdit()
        self.video_url_input.setPlaceholderText("https://example.com/video.mp4")
        self.cover_url_input = QtWidgets.QLineEdit()
        self.cover_url_input.setPlaceholderText("https://example.com/cover.jpg")
        self.local_video_path_input = QtWidgets.QLineEdit()
        self.local_video_path_input.setPlaceholderText("D:/videos/post.mp4")
        self.metadata_input = QtWidgets.QPlainTextEdit()
        self.metadata_input.setFixedHeight(96)
        self.metadata_input.setPlaceholderText('{\n  "campaign": "spring-launch"\n}')
        self.job_status_label = QtWidgets.QLabel("draft")
        self.job_status_label.setObjectName("subtitleLabel")

        form.addRow("Device", self.device_combo)
        form.addRow("Platform", self.platform_combo)
        form.addRow("Account", self.account_combo)
        form.addRow("Workflow", self.workflow_combo)
        form.addRow("Code Product", self.code_product_input)
        form.addRow("Link Product", self.link_product_input)
        form.addRow("Title", self.title_input)
        form.addRow("Description", self.description_input)
        form.addRow("Tags", self.tags_input)
        form.addRow("Video URL", self.video_url_input)
        form.addRow("Cover URL", self.cover_url_input)
        form.addRow("Local Video Path", self.local_video_path_input)
        form.addRow("Metadata JSON", self.metadata_input)
        form.addRow("Current Status", self.job_status_label)
        form_layout.addLayout(form)
        form_layout.addStretch(1)
        splitter.addWidget(right_card)
        splitter.setStretchFactor(0, 5)
        splitter.setStretchFactor(1, 2)
        splitter.setSizes([980, 420])

        self.new_button.clicked.connect(self.clear_form)
        self.save_button.clicked.connect(self.save_upload_job)
        self.run_now_button.clicked.connect(self.run_now)
        self.run_selected_button.clicked.connect(self.run_selected)
        self.run_all_button.clicked.connect(self.run_all)
        self.import_button.clicked.connect(self.import_upload_jobs)
        self.export_button.clicked.connect(self.export_upload_jobs)
        self.refresh_button.clicked.connect(self._reload_all)
        self.delete_button.clicked.connect(self.delete_upload_job)
        self.apply_template_button.clicked.connect(self.apply_selected_template)
        self.save_template_button.clicked.connect(self.save_template)
        self.delete_template_button.clicked.connect(self.delete_template)
        self.upload_table.itemSelectionChanged.connect(self._on_upload_selected)
        self.upload_table.itemDoubleClicked.connect(lambda *_: self.run_now())
        self.device_combo.currentIndexChanged.connect(self._on_device_changed)
        self.platform_combo.currentIndexChanged.connect(self._on_platform_changed)
        self.auto_run_checkbox.toggled.connect(self._on_auto_run_toggled)
        self.auto_run_interval_combo.currentIndexChanged.connect(self._on_auto_run_interval_changed)
        self._sync_actions()

    def refresh_devices(self) -> None:
        current_device = self.device_combo.currentData()
        self.device_combo.blockSignals(True)
        self.device_combo.clear()
        self.device_combo.addItem("Select Device", None)
        for device in self.device_service.list_devices():
            self.device_combo.addItem(f"{device['name']} ({device['serial']})", int(device["id"]))
        self.device_combo.blockSignals(False)
        if current_device is not None:
            index = self.device_combo.findData(current_device)
            self.device_combo.setCurrentIndex(index if index >= 0 else 0)
        self._on_device_changed()
        self.refresh_templates()

    def refresh_workflows(self) -> None:
        current_workflow = self.workflow_combo.currentData()
        self.workflow_combo.clear()
        self.workflow_combo.addItem("Select Workflow", None)
        for workflow in self.workflow_service.list_workflows():
            self.workflow_combo.addItem(str(workflow["name"]), int(workflow["id"]))
        if current_workflow is not None:
            index = self.workflow_combo.findData(current_workflow)
            self.workflow_combo.setCurrentIndex(index if index >= 0 else 0)
        self.refresh_templates()

    def refresh_templates(self) -> None:
        current_template = self.template_combo.currentData()
        templates = self.upload_service.list_upload_templates()
        self._templates_by_id = {int(template["id"]): template for template in templates}
        self.template_combo.blockSignals(True)
        self.template_combo.clear()
        self.template_combo.addItem("No Template", None)
        for template in templates:
            label = str(template.get("name") or "Untitled Template")
            workflow_name = str(template.get("workflow_name") or "").strip()
            if workflow_name:
                label = f"{label} | {workflow_name}"
            self.template_combo.addItem(label, int(template["id"]))
        self.template_combo.blockSignals(False)
        if current_template is not None:
            index = self.template_combo.findData(current_template)
            self.template_combo.setCurrentIndex(index if index >= 0 else 0)
        self._sync_actions()
        self._refresh_summary()

    def load_upload_jobs(self) -> None:
        jobs = self.upload_service.list_upload_jobs()
        previous_id = self.current_upload_job_id
        self._jobs_by_id = {int(job["id"]): job for job in jobs}
        self.upload_table.setRowCount(len(jobs))
        for row, job in enumerate(jobs):
            values = [
                int(job["id"]),
                str(job.get("device_name") or "-"),
                str(job.get("platform_name") or job.get("platform_key") or "-"),
                str(job.get("account_name") or "-"),
                str(job.get("workflow_name") or "-"),
                str(job.get("code_product") or ""),
                str(job.get("title") or ""),
                str(job.get("status") or "draft"),
            ]
            for column, value in enumerate(values):
                self.upload_table.setItem(row, column, QtWidgets.QTableWidgetItem(str(value)))
        self.upload_table.resizeColumnsToContents()
        if previous_id is not None:
            self._select_upload_row(previous_id)
        self._refresh_summary()
        self._sync_actions()

    def _reload_all(self) -> None:
        self.refresh_devices()
        self.refresh_workflows()
        self.refresh_templates()
        self.load_upload_jobs()

    def _refresh_summary(self) -> None:
        summary = self.upload_service.upload_summary()
        for key, label in self.summary_cards.items():
            label.setText(str(summary.get(key, 0)))
        self.summary_insights_label.setText(
            f"Top workflow: {summary.get('top_workflow', '-')} | "
            f"Top platform: {summary.get('top_platform', '-')} | "
            f"Top account: {summary.get('top_account', '-')}"
        )

    def _on_device_changed(self) -> None:
        self.current_device_id = self.device_combo.currentData()
        current_platform = self.platform_combo.currentData()
        self.platform_combo.blockSignals(True)
        self.platform_combo.clear()
        self.platform_combo.addItem("None", None)
        if self.current_device_id:
            for platform in self.account_service.list_device_platforms(int(self.current_device_id)):
                self.platform_combo.addItem(
                    f"{platform['platform_name']} ({platform['platform_key']})",
                    int(platform["id"]),
                )
        self.platform_combo.blockSignals(False)
        if current_platform is not None:
            index = self.platform_combo.findData(current_platform)
            self.platform_combo.setCurrentIndex(index if index >= 0 else 0)
        self._on_platform_changed()

    def _on_platform_changed(self) -> None:
        platform_id = self.platform_combo.currentData()
        current_account = self.account_combo.currentData()
        self.account_combo.clear()
        self.account_combo.addItem("None", None)
        if platform_id:
            for account in self.account_service.list_accounts(int(platform_id)):
                self.account_combo.addItem(str(account["display_name"]), int(account["id"]))
        if current_account is not None:
            index = self.account_combo.findData(current_account)
            self.account_combo.setCurrentIndex(index if index >= 0 else 0)

    def _on_upload_selected(self) -> None:
        row = self.upload_table.currentRow()
        if row < 0 or not self.upload_table.item(row, 0):
            self.current_upload_job_id = None
            self._sync_actions()
            return
        self.current_upload_job_id = int(self.upload_table.item(row, 0).text())
        upload_job = self.upload_service.get_upload_job(self.current_upload_job_id)
        if not upload_job:
            return
        self._populate_form(upload_job)
        self._sync_actions()

    def _populate_form(self, upload_job: dict) -> None:
        self._set_combo_data(self.device_combo, int(upload_job["device_id"]))
        self._on_device_changed()
        self._set_combo_data(self.platform_combo, int(upload_job.get("device_platform_id") or 0) or None)
        self._on_platform_changed()
        self._set_combo_data(self.account_combo, int(upload_job.get("account_id") or 0) or None)
        self._set_combo_data(self.workflow_combo, int(upload_job["workflow_id"]))
        self.code_product_input.setText(str(upload_job.get("code_product") or ""))
        self.link_product_input.setText(str(upload_job.get("link_product") or ""))
        self.title_input.setText(str(upload_job.get("title") or ""))
        self.description_input.setPlainText(str(upload_job.get("description") or ""))
        self.tags_input.setPlainText(self.upload_service.tags_to_text(upload_job.get("tags_json")))
        self.video_url_input.setText(str(upload_job.get("video_url") or ""))
        self.cover_url_input.setText(str(upload_job.get("cover_url") or ""))
        self.local_video_path_input.setText(str(upload_job.get("local_video_path") or ""))
        self.metadata_input.setPlainText(self.upload_service.metadata_to_text(upload_job.get("metadata_json")))
        self.job_status_label.setText(str(upload_job.get("status") or "draft"))
        self.status_label.setText(
            f"Selected upload job #{upload_job['id']} for {upload_job.get('device_name') or '-'} / {upload_job.get('workflow_name') or '-'}."
        )

    def clear_form(self) -> None:
        self.current_upload_job_id = None
        self.upload_table.clearSelection()
        if self.device_combo.count():
            self.device_combo.setCurrentIndex(0)
        if self.workflow_combo.count():
            self.workflow_combo.setCurrentIndex(0)
        self.code_product_input.clear()
        self.link_product_input.clear()
        self.title_input.clear()
        self.description_input.clear()
        self.tags_input.clear()
        self.video_url_input.clear()
        self.cover_url_input.clear()
        self.local_video_path_input.clear()
        self.metadata_input.setPlainText("{}")
        self.job_status_label.setText("draft")
        self.status_label.setText("Ready to create a new upload job.")
        self._sync_actions()

    def save_upload_job(self) -> None:
        try:
            upload_job_id = self.upload_service.save_upload_job(
                self.current_upload_job_id,
                device_id=int(self.device_combo.currentData() or 0),
                device_platform_id=self.platform_combo.currentData(),
                account_id=self.account_combo.currentData(),
                workflow_id=int(self.workflow_combo.currentData() or 0),
                code_product=self.code_product_input.text().strip(),
                link_product=self.link_product_input.text().strip(),
                title=self.title_input.text().strip(),
                description=self.description_input.toPlainText().strip(),
                tags_text=self.tags_input.toPlainText().strip(),
                video_url=self.video_url_input.text().strip(),
                cover_url=self.cover_url_input.text().strip(),
                local_video_path=self.local_video_path_input.text().strip(),
                metadata_text=self.metadata_input.toPlainText().strip(),
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save upload failed", str(exc))
            return
        self.current_upload_job_id = upload_job_id
        self.status_label.setText(f"Upload job #{upload_job_id} saved.")
        self.load_upload_jobs()
        self._select_upload_row(upload_job_id)
        self.uploads_changed.emit()

    def save_template(self) -> None:
        selected_template_id = self.template_combo.currentData()
        default_name = ""
        if selected_template_id:
            template = self._templates_by_id.get(int(selected_template_id))
            default_name = str(template.get("name") or "") if template else ""
        if not default_name:
            default_name = self.title_input.text().strip() or "Upload Template"
        name, accepted = QtWidgets.QInputDialog.getText(
            self,
            "Save Upload Template",
            "Template name",
            text=default_name,
        )
        if not accepted:
            return
        try:
            template_id = self.upload_service.save_upload_template(
                int(selected_template_id) if selected_template_id else None,
                name=name,
                description=f"Saved from Uploads page on '{self.title_input.text().strip() or 'untitled'}'",
                device_id=self.device_combo.currentData(),
                device_platform_id=self.platform_combo.currentData(),
                account_id=self.account_combo.currentData(),
                workflow_id=self.workflow_combo.currentData(),
                code_product=self.code_product_input.text().strip(),
                link_product=self.link_product_input.text().strip(),
                title=self.title_input.text().strip(),
                upload_description=self.description_input.toPlainText().strip(),
                tags_text=self.tags_input.toPlainText().strip(),
                video_url=self.video_url_input.text().strip(),
                cover_url=self.cover_url_input.text().strip(),
                local_video_path=self.local_video_path_input.text().strip(),
                metadata_text=self.metadata_input.toPlainText().strip(),
            )
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Save template failed", str(exc))
            return
        self.refresh_templates()
        self._set_combo_data(self.template_combo, template_id)
        self.status_label.setText(f"Template saved as #{template_id}.")

    def apply_selected_template(self) -> None:
        template_id = self.template_combo.currentData()
        if not template_id:
            QtWidgets.QMessageBox.information(self, "Uploads", "Select a template first.")
            return
        template = self.upload_service.get_upload_template(int(template_id))
        if not template:
            QtWidgets.QMessageBox.warning(self, "Uploads", "Template not found.")
            self.refresh_templates()
            return
        self._set_combo_data(self.device_combo, int(template.get("device_id") or 0) or None)
        self._on_device_changed()
        self._set_combo_data(self.platform_combo, int(template.get("device_platform_id") or 0) or None)
        self._on_platform_changed()
        self._set_combo_data(self.account_combo, int(template.get("account_id") or 0) or None)
        self._set_combo_data(self.workflow_combo, int(template.get("workflow_id") or 0) or None)
        self.code_product_input.setText(str(template.get("code_product") or ""))
        self.link_product_input.setText(str(template.get("link_product") or ""))
        self.title_input.setText(str(template.get("title") or ""))
        self.description_input.setPlainText(str(template.get("description_template") or ""))
        self.tags_input.setPlainText(self.upload_service.tags_to_text(template.get("tags_json")))
        self.video_url_input.setText(str(template.get("video_url") or ""))
        self.cover_url_input.setText(str(template.get("cover_url") or ""))
        self.local_video_path_input.setText(str(template.get("local_video_path") or ""))
        self.metadata_input.setPlainText(self.upload_service.metadata_to_text(template.get("metadata_json")))
        self.status_label.setText(f"Applied template '{template.get('name') or '-'}'.")

    def delete_template(self) -> None:
        template_id = self.template_combo.currentData()
        if not template_id:
            return
        if QtWidgets.QMessageBox.question(
            self,
            "Delete Upload Template",
            "Delete this upload template?",
        ) != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.upload_service.delete_upload_template(int(template_id))
        self.refresh_templates()
        self.status_label.setText("Upload template deleted.")

    def delete_upload_job(self) -> None:
        if not self.current_upload_job_id:
            return
        if QtWidgets.QMessageBox.question(
            self,
            "Delete Upload",
            "Delete this upload job?",
        ) != QtWidgets.QMessageBox.StandardButton.Yes:
            return
        self.upload_service.delete_upload_job(int(self.current_upload_job_id))
        self.clear_form()
        self.load_upload_jobs()
        self.status_label.setText("Upload job deleted.")
        self.uploads_changed.emit()

    def run_now(self) -> None:
        if not self.current_upload_job_id:
            QtWidgets.QMessageBox.information(self, "Uploads", "Select an upload job first.")
            return
        self._start_single_run(int(self.current_upload_job_id), source_label="manual")

    def run_selected(self) -> None:
        selected_ids = self._selected_upload_job_ids()
        if not selected_ids:
            QtWidgets.QMessageBox.information(self, "Uploads", "Select one or more upload jobs first.")
            return
        self._start_batch_run(selected_ids, label="selected upload jobs")

    def run_all(self) -> None:
        upload_job_ids = [int(job_id) for job_id in self._jobs_by_id.keys()]
        if not upload_job_ids:
            QtWidgets.QMessageBox.information(self, "Uploads", "There are no upload jobs to run.")
            return
        self._start_batch_run(upload_job_ids, label="all upload jobs")

    def _start_batch_run(self, upload_job_ids: list[int], *, label: str) -> None:
        if self._run_thread and self._run_thread.isRunning():
            self.status_label.setText("A single upload run is already in progress.")
            return
        if self._batch_run_thread and self._batch_run_thread.isRunning():
            self.status_label.setText("A batch upload run is already in progress.")
            return
        self.status_label.setText(f"Running {label} ({len(upload_job_ids)})...")
        self._batch_run_thread = UploadBatchRunThread(self.upload_service, upload_job_ids, continue_on_error=True)
        self._batch_run_thread.result_ready.connect(self._on_batch_run_result)
        self._batch_run_thread.finished.connect(self._on_batch_run_finished)
        self._batch_run_thread.start()
        self._sync_actions()

    def _start_single_run(self, upload_job_id: int, *, source_label: str = "manual") -> bool:
        if self._run_thread and self._run_thread.isRunning():
            self.status_label.setText("Upload run is already in progress.")
            return False
        if self._batch_run_thread and self._batch_run_thread.isRunning():
            self.status_label.setText("Batch upload run is already in progress.")
            return False
        try:
            self.upload_service.mark_upload_job_queued(int(upload_job_id))
        except Exception as exc:
            self.status_label.setText(str(exc))
            return False
        self.load_upload_jobs()
        if source_label == "auto":
            self.status_label.setText(f"Auto runner queued upload job #{upload_job_id}.")
        else:
            self.status_label.setText(f"Queued upload job #{upload_job_id} for execution.")
        self._run_thread = UploadRunThread(self.upload_service, int(upload_job_id))
        self._run_thread.result_ready.connect(self._on_run_result)
        self._run_thread.finished.connect(self._on_run_finished)
        self._run_thread.start()
        self._sync_actions()
        return True

    def _on_run_result(self, upload_job_id: int, result: dict) -> None:
        status = "completed" if result.get("success") else "failed"
        self.load_upload_jobs()
        self._select_upload_row(upload_job_id)
        self.status_label.setText(f"Upload job #{upload_job_id} {status}: {result.get('message') or '-'}")
        self.logs_changed.emit()

    def _on_run_finished(self) -> None:
        self._run_thread = None
        self._sync_actions()
        if self.auto_run_checkbox.isChecked():
            QtCore.QTimer.singleShot(0, self._check_auto_run_draft_jobs)

    def _on_batch_run_result(self, result: dict) -> None:
        self.load_upload_jobs()
        self.logs_changed.emit()
        self.status_label.setText(
            f"Batch upload finished {int(result.get('success_count') or 0)}/{int(result.get('total') or 0)} jobs"
        )

    def _on_batch_run_finished(self) -> None:
        self._batch_run_thread = None
        self._sync_actions()
        if self.auto_run_checkbox.isChecked():
            QtCore.QTimer.singleShot(0, self._check_auto_run_draft_jobs)

    def import_upload_jobs(self) -> None:
        path_text, _ = QtWidgets.QFileDialog.getOpenFileName(
            self,
            "Import Upload Jobs",
            str(Path.cwd()),
            "JSON Files (*.json)",
        )
        if not path_text:
            return
        try:
            payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
            imported_ids = self.upload_service.import_upload_jobs(payload)
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Import uploads failed", str(exc))
            return
        self.load_upload_jobs()
        if imported_ids:
            self._select_upload_row(imported_ids[-1])
        self.status_label.setText(f"Imported {len(imported_ids)} upload job(s).")
        self.uploads_changed.emit()

    def export_upload_jobs(self) -> None:
        selected_ids = self._selected_upload_job_ids()
        payload = self.upload_service.export_upload_jobs(selected_ids or None)
        path_text, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "Export Upload Jobs",
            str(Path.cwd() / "upload-jobs.json"),
            "JSON Files (*.json)",
        )
        if not path_text:
            return
        try:
            Path(path_text).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            QtWidgets.QMessageBox.warning(self, "Export uploads failed", str(exc))
            return
        scope = f"{len(selected_ids)} selected" if selected_ids else "all"
        self.status_label.setText(f"Exported {scope} upload job(s) to {path_text}.")

    def _sync_actions(self) -> None:
        has_job = self.current_upload_job_id is not None
        selected_ids = self._selected_upload_job_ids()
        has_any_jobs = bool(self._jobs_by_id)
        has_template = self.template_combo.currentData() is not None
        is_running = bool(self._run_thread and self._run_thread.isRunning())
        is_batch_running = bool(self._batch_run_thread and self._batch_run_thread.isRunning())
        busy = is_running or is_batch_running
        self.save_button.setEnabled(not busy)
        self.new_button.setEnabled(not busy)
        self.refresh_button.setEnabled(not busy)
        self.import_button.setEnabled(not busy)
        self.export_button.setEnabled(has_any_jobs and not busy)
        self.delete_button.setEnabled(has_job and not busy)
        self.run_now_button.setEnabled(has_job and not busy)
        self.run_selected_button.setEnabled(bool(selected_ids) and not busy)
        self.run_all_button.setEnabled(has_any_jobs and not busy)
        self.apply_template_button.setEnabled(has_template and not busy)
        self.save_template_button.setEnabled(not busy)
        self.delete_template_button.setEnabled(has_template and not busy)
        self.auto_run_interval_combo.setEnabled(not busy)
        self.auto_run_checkbox.setEnabled(True)
        self._update_auto_run_state_label(busy=busy)

    def _set_combo_data(self, combo: QtWidgets.QComboBox, value) -> None:
        index = combo.findData(value)
        combo.setCurrentIndex(index if index >= 0 else 0)

    def _select_upload_row(self, upload_job_id: int) -> None:
        for row in range(self.upload_table.rowCount()):
            item = self.upload_table.item(row, 0)
            if item and int(item.text()) == int(upload_job_id):
                self.upload_table.selectRow(row)
                break

    def _selected_upload_job_ids(self) -> list[int]:
        ids: list[int] = []
        selection_model = self.upload_table.selectionModel()
        if not selection_model:
            return ids
        for item in selection_model.selectedRows():
            row = item.row()
            cell = self.upload_table.item(row, 0)
            if cell is None:
                continue
            ids.append(int(cell.text()))
        return ids

    def _restore_auto_run_settings(self) -> None:
        enabled = str(self._settings.value("auto_run_enabled", "false")).strip().lower() in {"1", "true", "yes", "on"}
        interval_seconds = int(self._settings.value("auto_run_interval_seconds", 10) or 10)
        index = self.auto_run_interval_combo.findData(interval_seconds)
        self.auto_run_interval_combo.setCurrentIndex(index if index >= 0 else 1)
        self.auto_run_checkbox.setChecked(enabled)
        self._apply_auto_run_timer_state()

    def _store_auto_run_settings(self) -> None:
        self._settings.setValue("auto_run_enabled", self.auto_run_checkbox.isChecked())
        self._settings.setValue("auto_run_interval_seconds", int(self.auto_run_interval_combo.currentData() or 10))
        self._settings.sync()

    def _on_auto_run_toggled(self) -> None:
        self._store_auto_run_settings()
        self._apply_auto_run_timer_state()
        self._sync_actions()
        if self.auto_run_checkbox.isChecked():
            QtCore.QTimer.singleShot(0, self._check_auto_run_draft_jobs)

    def _on_auto_run_interval_changed(self) -> None:
        self._store_auto_run_settings()
        self._apply_auto_run_timer_state()
        self._sync_actions()

    def _apply_auto_run_timer_state(self) -> None:
        interval_seconds = int(self.auto_run_interval_combo.currentData() or 10)
        self._auto_run_timer.setInterval(max(interval_seconds, 1) * 1000)
        if self.auto_run_checkbox.isChecked():
            self._auto_run_timer.start()
        else:
            self._auto_run_timer.stop()

    def _update_auto_run_state_label(self, *, busy: bool) -> None:
        interval_seconds = int(self.auto_run_interval_combo.currentData() or 10)
        if not self.auto_run_checkbox.isChecked():
            self.auto_run_state_label.setText("Auto runner: off")
            return
        if busy:
            self.auto_run_state_label.setText(f"Auto runner: waiting for current run to finish ({interval_seconds} sec)")
            return
        next_job_id = self._next_draft_upload_job_id()
        if next_job_id is None:
            self.auto_run_state_label.setText(f"Auto runner: on, checking every {interval_seconds} sec")
        else:
            self.auto_run_state_label.setText(
                f"Auto runner: draft job #{next_job_id} ready, checking every {interval_seconds} sec"
            )

    def _next_draft_upload_job_id(self) -> int | None:
        draft_ids = [
            int(job["id"])
            for job in self._jobs_by_id.values()
            if str(job.get("status") or "draft") == "draft"
        ]
        return min(draft_ids) if draft_ids else None

    def _check_auto_run_draft_jobs(self) -> None:
        if not self.auto_run_checkbox.isChecked():
            return
        if self._run_thread and self._run_thread.isRunning():
            self._sync_actions()
            return
        if self._batch_run_thread and self._batch_run_thread.isRunning():
            self._sync_actions()
            return
        self.load_upload_jobs()
        next_job_id = self._next_draft_upload_job_id()
        if next_job_id is None:
            self._sync_actions()
            return
        self._start_single_run(next_job_id, source_label="auto")
