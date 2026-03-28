from __future__ import annotations

import argparse
import sys
from pathlib import Path

from PySide6 import QtWidgets

from automation_studio.database import DatabaseManager
from automation_studio.repositories import DeviceRepository
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


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = QtWidgets.QApplication(sys.argv if argv is None else [sys.argv[0], *argv])
    app.setApplicationName("Android Automation Studio Screen Viewer")
    app.setStyle("Fusion")
    app.setStyleSheet(APP_STYLESHEET)
    devices = load_devices(args.db_path)
    window = ScreenViewerWindow(
        devices=devices,
        refresh_interval_ms=args.refresh_ms,
    )
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
