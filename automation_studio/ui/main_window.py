from __future__ import annotations

import sys
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    AccountRepository,
    DeviceRepository,
    LogRepository,
    ScheduleGroupRepository,
    ScheduleRepository,
    ScheduleRunRepository,
    TelemetryRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)
from automation_studio.services import (
    AccountService,
    DeviceService,
    LogService,
    SchedulerService,
    TelemetryService,
    WatcherService,
    WatcherTelemetryService,
    WorkflowService,
)
from automation_studio.ui.pages.accounts_page import AccountsPage
from automation_studio.ui.pages.devices_page import DevicesPage
from automation_studio.ui.pages.log_page import LogPage
from automation_studio.ui.pages.schedules_page import ScheduleRunThread, SchedulesPage
from automation_studio.ui.pages.watchers_page import WatchersPage
from automation_studio.ui.pages.workflow_page import WorkflowPage
from automation_studio.ui.theme import APP_STYLESHEET
from automation_studio.ui.widgets import make_button


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Android Automation Studio")
        self.resize(1500, 920)
        self._schedule_runners: dict[int, ScheduleRunThread] = {}
        self._queued_schedule_runs: dict[int, tuple[str, bool]] = {}
        self._scheduler_startup_recovery_pending = True
        self._init_services()
        self._build_ui()
        self._init_scheduler_timer()

    def _init_services(self) -> None:
        db_path = Path.cwd() / "automation_studio.db"
        self.db = DatabaseManager(db_path)
        self.db.init_schema()

        self.device_repository = DeviceRepository(self.db)
        self.account_repository = AccountRepository(self.db)
        self.workflow_repository = WorkflowRepository(self.db)
        self.schedule_group_repository = ScheduleGroupRepository(self.db)
        self.schedule_repository = ScheduleRepository(self.db)
        self.schedule_run_repository = ScheduleRunRepository(self.db)
        self.log_repository = LogRepository(self.db)
        self.telemetry_repository = TelemetryRepository(self.db)
        self.watcher_repository = WatcherRepository(self.db)
        self.watcher_telemetry_repository = WatcherTelemetryRepository(self.db)

        self.device_service = DeviceService(self.device_repository)
        self.account_service = AccountService(
            self.account_repository,
            self.device_repository,
            self.workflow_repository,
        )
        self.log_service = LogService(self.log_repository)
        self.telemetry_service = TelemetryService(self.telemetry_repository)
        self.watcher_telemetry_service = WatcherTelemetryService(self.watcher_telemetry_repository)
        self.watcher_service = WatcherService(
            self.watcher_repository,
            self.device_repository,
            self.device_service,
            self.log_service,
            self.watcher_telemetry_service,
        )
        self.workflow_service = WorkflowService(
            self.workflow_repository,
            self.device_repository,
            self.device_service,
            self.log_service,
            self.telemetry_service,
            self.watcher_service,
            self.watcher_telemetry_service,
            self.account_service,
        )
        self.scheduler_service = SchedulerService(
            self.schedule_repository,
            self.schedule_run_repository,
            self.workflow_repository,
            self.device_repository,
            self.workflow_service,
            self.log_service,
            self.account_service,
            schedule_group_repository=self.schedule_group_repository,
        )

    def _build_ui(self) -> None:
        root = QtWidgets.QFrame()
        root.setObjectName("rootFrame")
        layout = QtWidgets.QHBoxLayout(root)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self.setCentralWidget(root)

        nav = QtWidgets.QFrame()
        nav.setProperty("navigation", True)
        nav_layout = QtWidgets.QVBoxLayout(nav)
        nav_layout.setContentsMargins(18, 24, 18, 24)
        nav_layout.setSpacing(14)

        brand = QtWidgets.QLabel("Automation\nStudio")
        brand.setObjectName("titleLabel")
        nav_layout.addWidget(brand)

        self.nav_list = QtWidgets.QListWidget()
        self.nav_list.setObjectName("navList")
        self.nav_list.addItems(["Devices", "Accounts", "Workflow", "Schedules", "Watchers", "Log"])
        self.nav_list.setCurrentRow(0)
        nav_layout.addWidget(self.nav_list, 1)

        self.open_screen_wall_button = make_button("Open Screen Wall")
        self.open_screen_wall_button.setMinimumHeight(36)
        nav_layout.addWidget(self.open_screen_wall_button, 0, QtCore.Qt.AlignmentFlag.AlignLeft)

        footer = QtWidgets.QLabel("PySide6 + uiautomator2 + sqlite")
        footer.setObjectName("subtitleLabel")
        footer.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignBottom)
        nav_layout.addWidget(footer)
        layout.addWidget(nav)

        content = QtWidgets.QWidget()
        content_layout = QtWidgets.QVBoxLayout(content)
        content_layout.setContentsMargins(24, 24, 24, 24)
        content_layout.setSpacing(0)

        self.stack = QtWidgets.QStackedWidget()
        self.devices_page = DevicesPage(self.device_service, self.log_service)
        self.accounts_page = AccountsPage(
            self.account_service,
            self.device_service,
            self.workflow_service,
        )
        self.workflow_page = WorkflowPage(
            self.workflow_service,
            self.device_service,
            self.watcher_service,
            self.account_service,
        )
        self.schedules_page = SchedulesPage(
            self.scheduler_service,
            self.workflow_service,
            self.device_service,
            self.account_service,
        )
        self.watchers_page = WatchersPage(
            self.watcher_service,
            self.workflow_service,
            self.device_service,
        )
        self.log_page = LogPage(
            self.log_service,
            self.workflow_service,
            self.device_service,
            self.watcher_service,
            self.telemetry_service,
            self.watcher_telemetry_service,
            self.account_service,
        )

        self.stack.addWidget(self.devices_page)
        self.stack.addWidget(self.accounts_page)
        self.stack.addWidget(self.workflow_page)
        self.stack.addWidget(self.schedules_page)
        self.stack.addWidget(self.watchers_page)
        self.stack.addWidget(self.log_page)
        content_layout.addWidget(self.stack)
        layout.addWidget(content, 1)

        self.nav_list.currentRowChanged.connect(self.stack.setCurrentIndex)
        self.devices_page.devices_changed.connect(self.accounts_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.workflow_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.schedules_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.watchers_page.load_watchers)
        self.devices_page.devices_changed.connect(self.log_page.refresh_filters)
        self.accounts_page.accounts_changed.connect(self.log_page.refresh_filters)
        self.accounts_page.accounts_changed.connect(self.workflow_page.refresh_runtime_targets)
        self.accounts_page.accounts_changed.connect(self.schedules_page.refresh_devices)
        self.workflow_page.workflows_changed.connect(self.accounts_page.refresh_workflows)
        self.workflow_page.workflows_changed.connect(self.schedules_page.refresh_workflows)
        self.workflow_page.workflows_changed.connect(self.schedules_page.load_schedules)
        self.workflow_page.workflows_changed.connect(self.log_page.refresh_filters)
        self.workflow_page.workflows_changed.connect(self.watchers_page.load_watchers)
        self.workflow_page.logs_changed.connect(self.log_page.load_logs)
        self.schedules_page.schedules_changed.connect(self.log_page.load_logs)
        self.schedules_page.logs_changed.connect(self.log_page.load_logs)
        self.schedules_page.run_requested.connect(self._run_schedule_now)
        self.schedules_page.toggle_requested.connect(self._toggle_schedule_enabled)
        self.watchers_page.watchers_changed.connect(self.log_page.refresh_filters)
        self.watchers_page.watchers_changed.connect(self.workflow_page.load_linked_watchers)
        self.watchers_page.logs_changed.connect(self.log_page.load_logs)
        self.devices_page.open_screen_requested.connect(self._open_screen_viewer)
        self.devices_page.devices_changed.connect(self._sync_screen_wall_button)
        self.open_screen_wall_button.clicked.connect(self.devices_page.open_screen_viewer)
        self._sync_screen_wall_button()
        self._refresh_schedule_runtime_state()

    def _sync_screen_wall_button(self) -> None:
        self.open_screen_wall_button.setEnabled(self.devices_page.has_devices())

    def _init_scheduler_timer(self) -> None:
        self.scheduler_timer = QtCore.QTimer(self)
        self.scheduler_timer.setInterval(15000)
        self.scheduler_timer.timeout.connect(self._poll_due_schedules)
        self.scheduler_timer.start()
        QtCore.QTimer.singleShot(1000, self._poll_due_schedules)

    def _run_schedule_now(self, schedule_id: int) -> None:
        self._dispatch_schedule_run(int(schedule_id), trigger_source="manual", advance_schedule=False, startup_recovery=False)

    def _toggle_schedule_enabled(self, schedule_id: int, enabled: bool) -> None:
        try:
            self.scheduler_service.set_schedule_enabled(int(schedule_id), bool(enabled))
        except Exception as exc:
            self.schedules_page.status_label.setText(f"Failed to update schedule #{schedule_id}: {exc}")
            return
        self.schedules_page.load_schedules()
        self.schedules_page.load_runs()
        self.log_page.load_logs()
        self._refresh_schedule_runtime_state()

    def _poll_due_schedules(self) -> None:
        startup_recovery = self._scheduler_startup_recovery_pending
        for schedule in self.scheduler_service.list_due_schedules():
            schedule_id = int(schedule["id"])
            self._dispatch_schedule_run(schedule_id, trigger_source="timer", advance_schedule=True, startup_recovery=startup_recovery)
        self._scheduler_startup_recovery_pending = False

    def _dispatch_schedule_run(
        self,
        schedule_id: int,
        *,
        trigger_source: str,
        advance_schedule: bool,
        startup_recovery: bool,
    ) -> None:
        decision = self.scheduler_service.resolve_run_request(
            schedule_id,
            trigger_source=trigger_source,
            advance_schedule=advance_schedule,
            startup_recovery=startup_recovery,
            is_running=schedule_id in self._schedule_runners,
        )
        action = str(decision.get("action") or "skip")
        if action == "run":
            self._start_schedule_runner(
                schedule_id,
                trigger_source=str(decision.get("trigger_source") or trigger_source),
                advance_schedule=bool(decision.get("advance_schedule", advance_schedule)),
            )
        elif action == "queue":
            self._queued_schedule_runs[schedule_id] = (
                str(decision.get("trigger_source") or trigger_source),
                bool(decision.get("advance_schedule", advance_schedule)),
            )
            self.schedules_page.status_label.setText(f"Queued schedule #{schedule_id} until the current run finishes.")
            self.schedules_page.load_runs()
            self.log_page.load_logs()
        self._refresh_schedule_runtime_state()

    def _start_schedule_runner(self, schedule_id: int, *, trigger_source: str, advance_schedule: bool) -> None:
        if schedule_id in self._schedule_runners:
            return
        runner = ScheduleRunThread(
            self.scheduler_service,
            schedule_id,
            trigger_source=trigger_source,
            advance_schedule=advance_schedule,
        )
        self._schedule_runners[schedule_id] = runner
        runner.result_ready.connect(self._on_schedule_runner_result)
        runner.finished.connect(lambda schedule_id=schedule_id: self._cleanup_schedule_runner(schedule_id))
        runner.start()
        self._refresh_schedule_runtime_state()

    def _cleanup_schedule_runner(self, schedule_id: int) -> None:
        runner = self._schedule_runners.pop(schedule_id, None)
        if runner is not None:
            runner.deleteLater()
        queued = self._queued_schedule_runs.pop(schedule_id, None)
        self._refresh_schedule_runtime_state()
        if queued is not None:
            queued_trigger_source, queued_advance_schedule = queued
            self._dispatch_schedule_run(
                schedule_id,
                trigger_source=queued_trigger_source,
                advance_schedule=queued_advance_schedule,
                startup_recovery=False,
            )

    def _on_schedule_runner_result(self, schedule_id: int, result: dict) -> None:
        self.schedules_page.notify_schedule_result(schedule_id, result)
        self._refresh_schedule_runtime_state()

    def _refresh_schedule_runtime_state(self) -> None:
        self.schedules_page.set_runtime_state(
            set(self._schedule_runners.keys()),
            set(self._queued_schedule_runs.keys()),
        )

    def _open_screen_viewer(self) -> None:
        args = [
            "-m",
            "automation_studio.viewer_process",
            "--db-path",
            str(Path.cwd() / "automation_studio.db"),
            "--refresh-ms",
            "1000",
        ]
        started, _pid = QtCore.QProcess.startDetached(sys.executable, args, str(Path.cwd()))
        if started:
            self.devices_page.status_label.setText("Opened screen viewer for all devices")
            return
        self.devices_page.status_label.setText("Failed to open screen viewer for all devices")


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    app.setApplicationName("Android Automation Studio")
    app.setStyle("Fusion")
    app.setStyleSheet(APP_STYLESHEET)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
