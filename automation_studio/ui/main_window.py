from __future__ import annotations

import sys
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    AccountRepository,
    DeviceRepository,
    LogRepository,
    RuntimeRepository,
    ScheduleGroupRepository,
    ScheduleRepository,
    ScheduleRunRepository,
    TelemetryRepository,
    UploadRepository,
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
    UploadService,
    WatcherService,
    WatcherTelemetryService,
    WorkflowService,
)
from automation_studio.ui.pages.accounts_page import AccountsPage
from automation_studio.ui.pages.devices_page import DevicesPage
from automation_studio.ui.pages.log_page import LogPage
from automation_studio.ui.pages.runtime_page import RuntimePage
from automation_studio.ui.pages.schedules_page import ScheduleRunThread, SchedulesPage
from automation_studio.ui.pages.uploads_page import UploadsPage
from automation_studio.ui.pages.watchers_page import WatchersPage
from automation_studio.ui.pages.workflow_page import WorkflowPage
from automation_studio.ui.theme import APP_STYLESHEET
from automation_studio.ui.widgets import make_button


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Android Automation Studio")
        self._schedule_runners: dict[int, ScheduleRunThread] = {}
        self._queued_schedule_runs: dict[int, tuple[str, bool]] = {}
        self._schedule_run_metadata: dict[int, dict] = {}
        self._queued_schedule_metadata: dict[int, dict] = {}
        self._scheduler_startup_recovery_pending = True
        self._init_services()
        self._build_ui()
        self._apply_initial_window_geometry()
        self._init_scheduler_timer()

    def _init_services(self) -> None:
        db_path = Path.cwd() / "automation_studio.db"
        self.db = DatabaseManager(db_path)
        self.db.init_schema()

        self.device_repository = DeviceRepository(self.db)
        self.account_repository = AccountRepository(self.db)
        self.workflow_repository = WorkflowRepository(self.db)
        self.upload_repository = UploadRepository(self.db)
        self.schedule_group_repository = ScheduleGroupRepository(self.db)
        self.schedule_repository = ScheduleRepository(self.db)
        self.schedule_run_repository = ScheduleRunRepository(self.db)
        self.log_repository = LogRepository(self.db)
        self.runtime_repository = RuntimeRepository(self.db)
        self.telemetry_repository = TelemetryRepository(self.db)
        self.watcher_repository = WatcherRepository(self.db)
        self.watcher_telemetry_repository = WatcherTelemetryRepository(self.db)

        self.device_service = DeviceService(self.device_repository)
        self.account_service = AccountService(
            self.account_repository,
            self.device_repository,
            self.workflow_repository,
        )
        self.upload_service = UploadService(
            self.upload_repository,
            self.device_repository,
            self.workflow_repository,
            self.account_service,
            runtime_repository=self.runtime_repository,
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
            runtime_repository=self.runtime_repository,
            runtime_source="main_ui",
        )
        self.workflow_service.bind_upload_service(self.upload_service)
        self.upload_service.bind_workflow_service(self.workflow_service)
        self.scheduler_service = SchedulerService(
            self.schedule_repository,
            self.schedule_run_repository,
            self.workflow_repository,
            self.device_repository,
            self.workflow_service,
            self.log_service,
            self.account_service,
            schedule_group_repository=self.schedule_group_repository,
            runtime_repository=self.runtime_repository,
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
        nav_layout.setContentsMargins(14, 18, 14, 18)
        nav_layout.setSpacing(10)

        brand = QtWidgets.QLabel("Automation\nStudio")
        brand.setObjectName("titleLabel")
        nav_layout.addWidget(brand)

        self.nav_list = QtWidgets.QListWidget()
        self.nav_list.setObjectName("navList")
        self.nav_list.addItems(["Devices", "Accounts", "Uploads", "Workflow", "Schedules", "Runtime", "Watchers", "Log"])
        self.nav_list.setCurrentRow(0)
        nav_layout.addWidget(self.nav_list, 1)

        self.open_screen_wall_button = make_button("Screen Wall")
        self.open_screen_wall_button.setMinimumHeight(36)
        nav_layout.addWidget(self.open_screen_wall_button, 0, QtCore.Qt.AlignmentFlag.AlignLeft)

        footer = QtWidgets.QLabel("PySide6\nuiautomator2\nsqlite")
        footer.setObjectName("subtitleLabel")
        footer.setWordWrap(True)
        footer.setAlignment(QtCore.Qt.AlignmentFlag.AlignLeft | QtCore.Qt.AlignmentFlag.AlignBottom)
        nav_layout.addWidget(footer)
        layout.addWidget(nav)

        content = QtWidgets.QWidget()
        content_layout = QtWidgets.QVBoxLayout(content)
        content_layout.setContentsMargins(16, 16, 16, 16)
        content_layout.setSpacing(0)

        self.stack = QtWidgets.QStackedWidget()
        self.devices_page = DevicesPage(self.device_service, self.log_service)
        self.accounts_page = AccountsPage(
            self.account_service,
            self.device_service,
            self.workflow_service,
        )
        self.uploads_page = UploadsPage(
            self.upload_service,
            self.workflow_service,
            self.device_service,
            self.account_service,
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
        self.runtime_page = RuntimePage(
            workflow_provider=self._runtime_workflow_tasks,
            upload_provider=self._runtime_upload_tasks,
            schedule_provider=self._runtime_schedule_tasks,
            stop_workflow_handler=self._stop_runtime_workflow_task,
            stop_upload_handler=self._stop_runtime_upload_task,
            cancel_upload_handler=self._cancel_runtime_upload_task,
            stop_schedule_handler=self._stop_runtime_schedule_task,
            cancel_schedule_handler=self._cancel_runtime_schedule_task,
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
        self.stack.addWidget(self.uploads_page)
        self.stack.addWidget(self.workflow_page)
        self.stack.addWidget(self.schedules_page)
        self.stack.addWidget(self.runtime_page)
        self.stack.addWidget(self.watchers_page)
        self.stack.addWidget(self.log_page)
        content_layout.addWidget(self.stack)
        layout.addWidget(content, 1)

        self.nav_list.currentRowChanged.connect(self.stack.setCurrentIndex)
        self.devices_page.devices_changed.connect(self.accounts_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.uploads_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.uploads_page.load_upload_jobs)
        self.devices_page.devices_changed.connect(self.workflow_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.schedules_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.watchers_page.load_watchers)
        self.devices_page.devices_changed.connect(self.log_page.refresh_filters)
        self.accounts_page.accounts_changed.connect(self.log_page.refresh_filters)
        self.accounts_page.accounts_changed.connect(self.uploads_page.refresh_devices)
        self.accounts_page.accounts_changed.connect(self.workflow_page.refresh_runtime_targets)
        self.accounts_page.accounts_changed.connect(self.schedules_page.refresh_devices)
        self.accounts_page.accounts_changed.connect(self.uploads_page.load_upload_jobs)
        self.workflow_page.workflows_changed.connect(self.accounts_page.refresh_workflows)
        self.workflow_page.workflows_changed.connect(self.uploads_page.refresh_workflows)
        self.workflow_page.workflows_changed.connect(self.uploads_page.load_upload_jobs)
        self.workflow_page.workflows_changed.connect(self.schedules_page.refresh_workflows)
        self.workflow_page.workflows_changed.connect(self.schedules_page.load_schedules)
        self.workflow_page.workflows_changed.connect(self.log_page.refresh_filters)
        self.workflow_page.workflows_changed.connect(self.watchers_page.load_watchers)
        self.workflow_page.logs_changed.connect(self.log_page.load_logs)
        self.workflow_page.logs_changed.connect(self.runtime_page.refresh_runtime)
        self.schedules_page.schedules_changed.connect(self.log_page.load_logs)
        self.schedules_page.logs_changed.connect(self.log_page.load_logs)
        self.schedules_page.run_requested.connect(self._run_schedule_now)
        self.schedules_page.toggle_requested.connect(self._toggle_schedule_enabled)
        self.uploads_page.uploads_changed.connect(self.log_page.load_logs)
        self.uploads_page.logs_changed.connect(self.log_page.load_logs)
        self.uploads_page.logs_changed.connect(self.runtime_page.refresh_runtime)
        self.watchers_page.watchers_changed.connect(self.log_page.refresh_filters)
        self.watchers_page.watchers_changed.connect(self.workflow_page.load_linked_watchers)
        self.watchers_page.logs_changed.connect(self.log_page.load_logs)
        self.devices_page.open_screen_requested.connect(self._open_screen_viewer)
        self.devices_page.devices_changed.connect(self._sync_screen_wall_button)
        self.open_screen_wall_button.clicked.connect(self.devices_page.open_screen_viewer)
        self._sync_screen_wall_button()
        self._refresh_schedule_runtime_state()
        self.runtime_page.refresh_runtime()

    def _sync_screen_wall_button(self) -> None:
        self.open_screen_wall_button.setEnabled(self.devices_page.has_devices())

    def _init_scheduler_timer(self) -> None:
        self.scheduler_timer = QtCore.QTimer(self)
        self.scheduler_timer.setInterval(15000)
        self.scheduler_timer.timeout.connect(self._poll_due_schedules)
        self.scheduler_timer.start()
        self.runtime_heartbeat_timer = QtCore.QTimer(self)
        self.runtime_heartbeat_timer.setInterval(5000)
        self.runtime_heartbeat_timer.timeout.connect(self._heartbeat_runtime_tasks)
        self.runtime_heartbeat_timer.start()
        QtCore.QTimer.singleShot(1000, self._poll_due_schedules)

    def _heartbeat_runtime_tasks(self) -> None:
        if not self.runtime_repository:
            return
        for metadata in list(self._queued_schedule_metadata.values()):
            self.runtime_repository.touch_task(
                str(metadata.get("task_id") or ""),
                status=str(metadata.get("status") or "queued"),
                detail=str(metadata.get("detail") or "Queued schedule"),
            )
        for metadata in list(self._schedule_run_metadata.values()):
            self.runtime_repository.touch_task(
                str(metadata.get("task_id") or ""),
                status=str(metadata.get("status") or "running"),
                detail=str(metadata.get("detail") or "Running schedule"),
            )

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
            schedule = self.scheduler_service.get_schedule(schedule_id) or {}
            self._queued_schedule_metadata[schedule_id] = {
                "task_id": f"schedule-queued:{schedule_id}",
                "schedule_id": schedule_id,
                "schedule_name": str(schedule.get("name") or f"Schedule #{schedule_id}"),
                "workflow_name": str(schedule.get("workflow_name") or "-"),
                "device_name": str(schedule.get("device_name") or "-"),
                "mode": "queued",
                "status": "queued",
                "detail": f"Queued from {str(decision.get('trigger_source') or trigger_source)} trigger",
            }
            if self.runtime_repository:
                self.runtime_repository.upsert_task(
                    task_id=f"schedule-queued:{schedule_id}",
                    category="schedule",
                    source="main_ui",
                    status="queued",
                    detail=f"Queued from {str(decision.get('trigger_source') or trigger_source)} trigger",
                    workflow_id=int(schedule.get("workflow_id") or 0) or None,
                    workflow_name=str(schedule.get("workflow_name") or "-"),
                    device_id=int(schedule.get("device_id") or 0) or None,
                    device_name=str(schedule.get("device_name") or "-"),
                    schedule_id=schedule_id,
                    scope="schedule",
                    metadata={"trigger_source": str(decision.get("trigger_source") or trigger_source)},
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
            runtime_task_id=f"schedule-running:{schedule_id}",
        )
        self._schedule_runners[schedule_id] = runner
        schedule = self.scheduler_service.get_schedule(schedule_id) or {}
        self._schedule_run_metadata[schedule_id] = {
            "task_id": f"schedule-running:{schedule_id}",
            "schedule_id": schedule_id,
            "schedule_name": str(schedule.get("name") or f"Schedule #{schedule_id}"),
            "workflow_name": str(schedule.get("workflow_name") or "-"),
            "device_name": str(schedule.get("device_name") or "-"),
            "device_id": int(schedule.get("device_id") or 0),
            "mode": str(trigger_source or "manual"),
            "status": "running",
            "detail": f"Started from {trigger_source}",
        }
        if self.runtime_repository:
            self.runtime_repository.finish_task(
                f"schedule-queued:{schedule_id}",
                status="completed",
                detail="Schedule queue entry consumed",
            )
            self.runtime_repository.upsert_task(
                task_id=f"schedule-running:{schedule_id}",
                category="schedule",
                source="main_ui",
                status="running",
                detail=f"Started from {trigger_source}",
                workflow_id=int(schedule.get("workflow_id") or 0) or None,
                workflow_name=str(schedule.get("workflow_name") or "-"),
                device_id=int(schedule.get("device_id") or 0) or None,
                device_name=str(schedule.get("device_name") or "-"),
                schedule_id=schedule_id,
                scope="schedule",
                metadata={"trigger_source": str(trigger_source or "manual")},
            )
        self._queued_schedule_metadata.pop(schedule_id, None)
        runner.result_ready.connect(self._on_schedule_runner_result)
        runner.finished.connect(lambda schedule_id=schedule_id: self._cleanup_schedule_runner(schedule_id))
        runner.start()
        self._refresh_schedule_runtime_state()

    def _cleanup_schedule_runner(self, schedule_id: int) -> None:
        metadata = self._schedule_run_metadata.get(schedule_id) or {}
        runner = self._schedule_runners.pop(schedule_id, None)
        if runner is not None:
            runner.deleteLater()
        if self.runtime_repository and metadata:
            task = self.runtime_repository.get_task(str(metadata.get("task_id") or ""))
            if task and str(task.get("status") or "") in {"queued", "running", "stopping"}:
                self.runtime_repository.finish_task(
                    str(metadata.get("task_id") or ""),
                    status="failed",
                    detail="Schedule runner ended unexpectedly",
                )
        self._schedule_run_metadata.pop(schedule_id, None)
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
        metadata = self._schedule_run_metadata.get(schedule_id) or {}
        if self.runtime_repository and metadata:
            run_status = str(result.get("run_status") or "")
            final_status = run_status or ("success" if result.get("success") else "stopped" if result.get("stopped") else "failed")
            self.runtime_repository.finish_task(
                str(metadata.get("task_id") or ""),
                status=final_status,
                detail=str(result.get("message") or final_status.title()),
            )
        self.schedules_page.notify_schedule_result(schedule_id, result)
        self._refresh_schedule_runtime_state()

    def _refresh_schedule_runtime_state(self) -> None:
        self.schedules_page.set_runtime_state(
            set(self._schedule_runners.keys()),
            set(self._queued_schedule_runs.keys()),
        )
        self.runtime_page.refresh_runtime()

    def _runtime_workflow_tasks(self) -> list[dict]:
        tasks = []
        for task in self.workflow_service.list_active_runtime_tasks():
            if int(task.get("upload_job_id") or 0) > 0:
                continue
            if int(task.get("schedule_id") or 0) > 0:
                continue
            tasks.append(
                {
                    "task_id": str(task.get("task_id") or ""),
                    "workflow_name": str(task.get("workflow_name") or "-"),
                    "device_name": str(task.get("device_name") or "-"),
                    "scope": str(task.get("scope") or "-"),
                    "started_at": str(task.get("started_at") or "-"),
                    "status": str(task.get("status") or "running"),
                    "detail": str(task.get("detail") or "-"),
                }
            )
        return tasks

    def _runtime_upload_tasks(self) -> list[dict]:
        tasks = []
        for job in self.upload_service.list_active_upload_jobs():
            status = str(job.get("status") or "")
            detail = "Queued for execution" if status == "queued" else "Upload job is running"
            tasks.append(
                {
                    "task_id": f"upload:{int(job.get('id') or 0)}",
                    "task_name": f"Upload #{int(job.get('id') or 0)}",
                    "device_name": str(job.get("device_name") or "-"),
                    "workflow_name": str(job.get("workflow_name") or "-"),
                    "started_at": str(job.get("started_at") or job.get("updated_at") or "-"),
                    "status": status or "draft",
                    "detail": detail,
                }
            )
        return tasks

    def _runtime_schedule_tasks(self) -> list[dict]:
        if self.runtime_repository:
            tasks = []
            for task in self.runtime_repository.list_active_tasks(category="schedule"):
                schedule_id = int(task.get("schedule_id") or 0)
                schedule = self.scheduler_service.get_schedule(schedule_id) or {}
                tasks.append(
                    {
                        "task_id": str(task.get("task_id") or ""),
                        "schedule_name": str(schedule.get("name") or f"Schedule #{schedule_id}" if schedule_id > 0 else "Schedule"),
                        "device_name": str(task.get("device_name") or "-"),
                        "workflow_name": str(task.get("workflow_name") or "-"),
                        "mode": str((self._schedule_run_metadata.get(schedule_id) or self._queued_schedule_metadata.get(schedule_id) or {}).get("mode") or task.get("source") or "-"),
                        "status": str(task.get("status") or "queued"),
                        "detail": str(task.get("detail") or "-"),
                    }
                )
            return tasks
        tasks = [dict(item) for item in self._schedule_run_metadata.values()]
        tasks.extend(dict(item) for item in self._queued_schedule_metadata.values())
        return tasks

    def _stop_runtime_workflow_task(self, task_id: str) -> bool:
        result = self.workflow_service.request_stop_for_runtime_task(task_id, reason="Stopped from Runtime page")
        self.runtime_page.refresh_runtime()
        return result

    def _stop_runtime_upload_task(self, task_id: str) -> bool:
        if not task_id.startswith("upload:"):
            return False
        upload_job_id = int(task_id.split(":", 1)[1] or 0)
        if upload_job_id <= 0:
            return False
        try:
            self.upload_service.request_stop_upload_job(upload_job_id, reason="Stopped upload from Runtime page")
        except Exception:
            return False
        self.runtime_page.refresh_runtime()
        self.uploads_page.load_upload_jobs()
        return True

    def _cancel_runtime_upload_task(self, task_id: str) -> bool:
        if not task_id.startswith("upload:"):
            return False
        upload_job_id = int(task_id.split(":", 1)[1] or 0)
        if upload_job_id <= 0:
            return False
        try:
            self.upload_service.cancel_queued_upload_job(upload_job_id)
        except Exception:
            return False
        self.runtime_page.refresh_runtime()
        self.uploads_page.load_upload_jobs()
        return True

    def _stop_runtime_schedule_task(self, task_id: str) -> bool:
        if task_id.startswith("schedule-queued:"):
            return self._cancel_runtime_schedule_task(task_id)
        if not task_id.startswith("schedule-running:"):
            return False
        schedule_id = int(task_id.split(":", 1)[1] or 0)
        if self.runtime_repository:
            self.runtime_repository.request_task_stop(task_id, reason="Stopped schedule from Runtime page")
            for task in self.runtime_repository.list_tasks_for_schedule(schedule_id, active_only=True):
                if str(task.get("category") or "") == "workflow":
                    self.runtime_repository.request_task_stop(
                        str(task.get("task_id") or ""),
                        reason="Stopped schedule from Runtime page",
                    )
        if schedule_id in self._schedule_run_metadata:
            self._schedule_run_metadata[schedule_id]["status"] = "stopping"
            self._schedule_run_metadata[schedule_id]["detail"] = "Stop requested from Runtime page"
        self.runtime_page.refresh_runtime()
        return True

    def _cancel_runtime_schedule_task(self, task_id: str) -> bool:
        if not task_id.startswith("schedule-queued:"):
            return False
        schedule_id = int(task_id.split(":", 1)[1] or 0)
        removed = self._queued_schedule_runs.pop(schedule_id, None)
        self._queued_schedule_metadata.pop(schedule_id, None)
        if self.runtime_repository:
            self.runtime_repository.request_task_cancel(task_id, reason="Cancelled queued schedule from Runtime page")
            self.runtime_repository.finish_task(
                task_id,
                status="cancelled",
                detail="Cancelled before execution",
            )
        if removed is None:
            return False
        self._refresh_schedule_runtime_state()
        self.schedules_page.status_label.setText(f"Cancelled queued schedule #{schedule_id}.")
        return True

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

    def _apply_initial_window_geometry(self) -> None:
        desired_width = 1600
        desired_height = 900
        self.setMinimumSize(0, 0)

        screen = self.screen() or QtWidgets.QApplication.primaryScreen()
        if not screen:
            self.resize(desired_width, desired_height)
            return

        available = screen.availableGeometry()
        width = min(desired_width, available.width())
        height = min(desired_height, available.height())
        width = max(960, width)
        height = max(640, height)

        self.resize(width, height)
        frame = self.frameGeometry()
        frame.moveCenter(available.center())
        self.move(frame.topLeft())


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    app.setApplicationName("Android Automation Studio")
    app.setStyle("Fusion")
    app.setStyleSheet(APP_STYLESHEET)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
