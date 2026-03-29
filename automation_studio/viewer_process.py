from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PySide6 import QtWidgets

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    AccountRepository,
    DeviceRepository,
    LogRepository,
    RuntimeRepository,
    TelemetryRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)
from automation_studio.services import (
    AccountService,
    DeviceService,
    LogService,
    TelemetryService,
    WatcherService,
    WatcherTelemetryService,
    WorkflowService,
)
from automation_studio.ui.screen_viewer_window import ScreenViewerWindow
from automation_studio.ui.theme import APP_STYLESHEET


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Android Automation Studio Screen Viewer")
    parser.add_argument(
        "--db-path",
        default=str(Path.cwd() / "automation_studio.db"),
        help="Path to the sqlite database used by the main app",
    )
    parser.add_argument("--refresh-ms", type=int, default=1000, help="Polling interval in milliseconds")
    return parser


def load_devices(db_path: str) -> list[dict]:
    database = DatabaseManager(Path(db_path))
    database.init_schema()
    repository = DeviceRepository(database)
    return repository.list_devices()


def build_workflow_service(db_path: str) -> WorkflowService:
    database = DatabaseManager(Path(db_path))
    database.init_schema()

    device_repository = DeviceRepository(database)
    account_repository = AccountRepository(database)
    workflow_repository = WorkflowRepository(database)
    log_repository = LogRepository(database)
    runtime_repository = RuntimeRepository(database)
    telemetry_repository = TelemetryRepository(database)
    watcher_repository = WatcherRepository(database)
    watcher_telemetry_repository = WatcherTelemetryRepository(database)

    device_service = DeviceService(device_repository)
    account_service = AccountService(account_repository, device_repository, workflow_repository)
    log_service = LogService(log_repository)
    telemetry_service = TelemetryService(telemetry_repository)
    watcher_telemetry_service = WatcherTelemetryService(watcher_telemetry_repository)
    watcher_service = WatcherService(
        watcher_repository,
        device_repository,
        device_service,
        log_service,
        watcher_telemetry_service,
    )
    return WorkflowService(
        workflow_repository,
        device_repository,
        device_service,
        log_service,
        telemetry_service,
        watcher_service,
        watcher_telemetry_service,
        account_service,
        runtime_repository=runtime_repository,
        runtime_source="screen_wall",
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = QtWidgets.QApplication(sys.argv if argv is None else [sys.argv[0], *argv])
    app.setApplicationName("Android Automation Studio Screen Viewer")
    app.setStyle("Fusion")
    app.setStyleSheet(APP_STYLESHEET)
    devices = load_devices(args.db_path)
    workflow_service = build_workflow_service(args.db_path)
    window = ScreenViewerWindow(
        devices=devices,
        workflows=workflow_service.list_workflows(),
        workflow_service=workflow_service,
        refresh_interval_ms=args.refresh_ms,
    )
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
