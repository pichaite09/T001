from __future__ import annotations

import sys
from pathlib import Path

from PySide6 import QtCore, QtWidgets

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    AccountRepository,
    DeviceRepository,
    LogRepository,
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
from automation_studio.ui.pages.accounts_page import AccountsPage
from automation_studio.ui.pages.devices_page import DevicesPage
from automation_studio.ui.pages.log_page import LogPage
from automation_studio.ui.pages.watchers_page import WatchersPage
from automation_studio.ui.pages.workflow_page import WorkflowPage
from automation_studio.ui.theme import APP_STYLESHEET


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Android Automation Studio")
        self.resize(1500, 920)
        self._init_services()
        self._build_ui()

    def _init_services(self) -> None:
        db_path = Path.cwd() / "automation_studio.db"
        self.db = DatabaseManager(db_path)
        self.db.init_schema()

        self.device_repository = DeviceRepository(self.db)
        self.account_repository = AccountRepository(self.db)
        self.workflow_repository = WorkflowRepository(self.db)
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
        self.nav_list.addItems(["Devices", "Accounts", "Workflow", "Watchers", "Log"])
        self.nav_list.setCurrentRow(0)
        nav_layout.addWidget(self.nav_list, 1)

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
        self.devices_page = DevicesPage(self.device_service)
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
        self.stack.addWidget(self.watchers_page)
        self.stack.addWidget(self.log_page)
        content_layout.addWidget(self.stack)
        layout.addWidget(content, 1)

        self.nav_list.currentRowChanged.connect(self.stack.setCurrentIndex)
        self.devices_page.devices_changed.connect(self.accounts_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.workflow_page.refresh_devices)
        self.devices_page.devices_changed.connect(self.watchers_page.load_watchers)
        self.devices_page.devices_changed.connect(self.log_page.refresh_filters)
        self.accounts_page.accounts_changed.connect(self.log_page.refresh_filters)
        self.accounts_page.accounts_changed.connect(self.workflow_page.refresh_runtime_targets)
        self.workflow_page.workflows_changed.connect(self.accounts_page.refresh_workflows)
        self.workflow_page.workflows_changed.connect(self.log_page.refresh_filters)
        self.workflow_page.workflows_changed.connect(self.watchers_page.load_watchers)
        self.workflow_page.logs_changed.connect(self.log_page.load_logs)
        self.watchers_page.watchers_changed.connect(self.log_page.refresh_filters)
        self.watchers_page.watchers_changed.connect(self.workflow_page.load_linked_watchers)
        self.watchers_page.logs_changed.connect(self.log_page.load_logs)


def main() -> None:
    app = QtWidgets.QApplication(sys.argv)
    app.setApplicationName("Android Automation Studio")
    app.setStyle("Fusion")
    app.setStyleSheet(APP_STYLESHEET)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
