from __future__ import annotations

from typing import Callable

from PySide6 import QtCore, QtWidgets

from automation_studio.ui.widgets import CardFrame, make_button


class RuntimePage(QtWidgets.QWidget):
    def __init__(
        self,
        *,
        workflow_provider: Callable[[], list[dict]],
        upload_provider: Callable[[], list[dict]],
        schedule_provider: Callable[[], list[dict]],
        stop_workflow_handler: Callable[[str], bool],
        stop_upload_handler: Callable[[str], bool],
        cancel_upload_handler: Callable[[str], bool],
        stop_schedule_handler: Callable[[str], bool],
        cancel_schedule_handler: Callable[[str], bool],
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.workflow_provider = workflow_provider
        self.upload_provider = upload_provider
        self.schedule_provider = schedule_provider
        self.stop_workflow_handler = stop_workflow_handler
        self.stop_upload_handler = stop_upload_handler
        self.cancel_upload_handler = cancel_upload_handler
        self.stop_schedule_handler = stop_schedule_handler
        self.cancel_schedule_handler = cancel_schedule_handler

        self._build_ui()
        self._timer = QtCore.QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self.refresh_runtime)
        self._timer.start()
        self.refresh_runtime()

    def _build_ui(self) -> None:
        root = QtWidgets.QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(12)

        title = QtWidgets.QLabel("Runtime")
        title.setObjectName("titleLabel")
        subtitle = QtWidgets.QLabel("Monitor active workflow runs, upload jobs, and schedules. Stop or cancel work from one place.")
        subtitle.setObjectName("subtitleLabel")
        subtitle.setWordWrap(True)
        root.addWidget(title)
        root.addWidget(subtitle)

        self.summary_label = QtWidgets.QLabel("No active runtime tasks.")
        self.summary_label.setObjectName("subtitleLabel")
        root.addWidget(self.summary_label)

        self.workflow_table = self._build_table(["Workflow", "Device", "Scope", "Started", "Status", "Detail"])
        self.upload_table = self._build_table(["Task", "Device", "Workflow", "Started", "Status", "Detail"])
        self.schedule_table = self._build_table(["Schedule", "Device", "Workflow", "Mode", "Status", "Detail"])

        root.addWidget(
            self._build_section(
                "Workflow Runs",
                self.workflow_table,
                [
                    ("Refresh", self.refresh_runtime, "secondary"),
                    ("Stop Selected", self._stop_selected_workflow, "danger"),
                ],
            ),
            1,
        )
        root.addWidget(
            self._build_section(
                "Upload Jobs",
                self.upload_table,
                [
                    ("Refresh", self.refresh_runtime, "secondary"),
                    ("Stop Selected", self._stop_selected_upload, "danger"),
                    ("Cancel Selected", self._cancel_selected_upload, "secondary"),
                ],
            ),
            1,
        )
        root.addWidget(
            self._build_section(
                "Schedule Runs",
                self.schedule_table,
                [
                    ("Refresh", self.refresh_runtime, "secondary"),
                    ("Stop Selected", self._stop_selected_schedule, "danger"),
                    ("Cancel Selected", self._cancel_selected_schedule, "secondary"),
                ],
            ),
            1,
        )

        self.status_label = QtWidgets.QLabel("")
        self.status_label.setObjectName("subtitleLabel")
        root.addWidget(self.status_label)

    def _build_section(
        self,
        title_text: str,
        table: QtWidgets.QTableWidget,
        buttons: list[tuple[str, Callable[[], None], str]],
    ) -> QtWidgets.QWidget:
        card = CardFrame()
        layout = QtWidgets.QVBoxLayout(card)
        layout.setContentsMargins(12, 10, 12, 12)
        layout.setSpacing(8)

        header = QtWidgets.QHBoxLayout()
        title = QtWidgets.QLabel(title_text)
        title.setObjectName("subtitleLabel")
        header.addWidget(title)
        header.addStretch(1)
        for label, handler, style in buttons:
            button = make_button(label, style)
            button.setMinimumHeight(28)
            button.clicked.connect(handler)
            header.addWidget(button)
        layout.addLayout(header)
        layout.addWidget(table, 1)
        return card

    def _build_table(self, headers: list[str]) -> QtWidgets.QTableWidget:
        table = QtWidgets.QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        table.setEditTriggers(QtWidgets.QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.horizontalHeader().setStretchLastSection(True)
        table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeMode.ResizeToContents)
        return table

    def refresh_runtime(self) -> None:
        workflow_tasks = self.workflow_provider()
        upload_tasks = self.upload_provider()
        schedule_tasks = self.schedule_provider()

        self._populate_table(
            self.workflow_table,
            workflow_tasks,
            ("workflow_name", "device_name", "scope", "started_at", "status", "detail"),
        )
        self._populate_table(
            self.upload_table,
            upload_tasks,
            ("task_name", "device_name", "workflow_name", "started_at", "status", "detail"),
        )
        self._populate_table(
            self.schedule_table,
            schedule_tasks,
            ("schedule_name", "device_name", "workflow_name", "mode", "status", "detail"),
        )

        total = len(workflow_tasks) + len(upload_tasks) + len(schedule_tasks)
        if total:
            self.summary_label.setText(
                f"Active runtime tasks: workflows {len(workflow_tasks)} | uploads {len(upload_tasks)} | schedules {len(schedule_tasks)}"
            )
        else:
            self.summary_label.setText("No active runtime tasks.")

    def _populate_table(
        self,
        table: QtWidgets.QTableWidget,
        rows: list[dict],
        fields: tuple[str, ...],
    ) -> None:
        table.setRowCount(len(rows))
        for row_index, row in enumerate(rows):
            for column_index, field in enumerate(fields):
                item = QtWidgets.QTableWidgetItem(str(row.get(field) or "-"))
                if column_index == 0:
                    item.setData(QtCore.Qt.ItemDataRole.UserRole, str(row.get("task_id") or ""))
                table.setItem(row_index, column_index, item)
        if rows:
            table.resizeColumnsToContents()

    def _selected_task_id(self, table: QtWidgets.QTableWidget) -> str:
        row = table.currentRow()
        if row < 0:
            return ""
        item = table.item(row, 0)
        return str(item.data(QtCore.Qt.ItemDataRole.UserRole) or "") if item else ""

    def _stop_selected_workflow(self) -> None:
        task_id = self._selected_task_id(self.workflow_table)
        if not task_id:
            self.status_label.setText("Select a workflow task first.")
            return
        self.status_label.setText("Workflow stop requested." if self.stop_workflow_handler(task_id) else "Unable to stop workflow task.")
        self.refresh_runtime()

    def _stop_selected_upload(self) -> None:
        task_id = self._selected_task_id(self.upload_table)
        if not task_id:
            self.status_label.setText("Select an upload task first.")
            return
        self.status_label.setText("Upload stop requested." if self.stop_upload_handler(task_id) else "Unable to stop upload task.")
        self.refresh_runtime()

    def _cancel_selected_upload(self) -> None:
        task_id = self._selected_task_id(self.upload_table)
        if not task_id:
            self.status_label.setText("Select a queued upload task first.")
            return
        self.status_label.setText("Upload cancelled." if self.cancel_upload_handler(task_id) else "Unable to cancel upload task.")
        self.refresh_runtime()

    def _stop_selected_schedule(self) -> None:
        task_id = self._selected_task_id(self.schedule_table)
        if not task_id:
            self.status_label.setText("Select a schedule task first.")
            return
        self.status_label.setText("Schedule stop requested." if self.stop_schedule_handler(task_id) else "Unable to stop schedule task.")
        self.refresh_runtime()

    def _cancel_selected_schedule(self) -> None:
        task_id = self._selected_task_id(self.schedule_table)
        if not task_id:
            self.status_label.setText("Select a queued schedule first.")
            return
        self.status_label.setText("Schedule cancelled." if self.cancel_schedule_handler(task_id) else "Unable to cancel schedule task.")
        self.refresh_runtime()
