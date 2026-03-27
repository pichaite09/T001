from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    DeviceRepository,
    LogRepository,
    TelemetryRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)
from automation_studio.services import (
    DeviceService,
    LogService,
    TelemetryService,
    WatcherService,
    WatcherTelemetryService,
    WorkflowService,
)


class FakeDevice:
    def __init__(self) -> None:
        self.actions: list[tuple] = []

    def click(self, x: int, y: int):
        self.actions.append(("click", x, y))

    def screenshot(self, path: str):
        Path(path).write_bytes(b"img")
        self.actions.append(("screenshot", path))

    def dump_hierarchy(self):
        self.actions.append(("dump_hierarchy",))
        return "<hierarchy/>"

    def window_size(self):
        return (1080, 2400)


class FakeDeviceService(DeviceService):
    def __init__(self, device_repository: DeviceRepository, device: FakeDevice) -> None:
        super().__init__(device_repository)
        self.fake_device = device

    def connect_device(self, serial: str):
        return self.fake_device


class ServicePhase4Tests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = DatabaseManager(self.db_path)
        self.db.init_schema()
        self.workflow_repository = WorkflowRepository(self.db)
        self.device_repository = DeviceRepository(self.db)
        self.log_repository = LogRepository(self.db)
        self.telemetry_repository = TelemetryRepository(self.db)
        self.watcher_repository = WatcherRepository(self.db)
        self.watcher_telemetry_repository = WatcherTelemetryRepository(self.db)
        self.log_service = LogService(self.log_repository)
        self.telemetry_service = TelemetryService(self.telemetry_repository)
        self.watcher_telemetry_service = WatcherTelemetryService(self.watcher_telemetry_repository)
        self.fake_device = FakeDevice()
        self.device_service = FakeDeviceService(self.device_repository, self.fake_device)
        self.watcher_service = WatcherService(
            self.watcher_repository,
            self.device_repository,
            self.device_service,
            self.log_service,
            self.watcher_telemetry_service,
        )
        self.service = WorkflowService(
            self.workflow_repository,
            self.device_repository,
            self.device_service,
            self.log_service,
            self.telemetry_service,
            self.watcher_service,
            self.watcher_telemetry_service,
        )

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_export_import_preserves_phase4_schema(self) -> None:
        workflow_id = self.service.save_workflow(None, "Phase4 Export", "demo", True)
        self.service.save_step(
            None,
            workflow_id,
            1,
            "Plugin",
            "plugin:echo_context",
            json.dumps({"message": "Hello", "write_variable": "x"}, ensure_ascii=False),
            True,
        )
        exported = self.service.export_workflow_definition(workflow_id)
        imported_id = self.service.import_workflow_definition(exported)
        imported_steps = self.service.list_steps(imported_id)

        self.assertEqual(exported["workflow"]["definition_version"], 2)
        self.assertEqual(exported["steps"][0]["schema_version"], 2)
        self.assertEqual(imported_steps[0]["step_type"], "plugin:echo_context")

    def test_import_failure_cleans_up_partial_workflow(self) -> None:
        with self.assertRaises(ValueError):
            self.service.import_workflow_definition(
                {
                    "workflow": {"name": "Broken Import", "description": "", "is_active": True},
                    "steps": [
                        {
                            "position": 1,
                            "name": "Broken Jump",
                            "step_type": "conditional_jump",
                            "parameters": {"expression": "True", "target_position": 9},
                            "is_enabled": True,
                            "schema_version": 2,
                        }
                    ],
                }
            )

        workflow_names = [workflow["name"] for workflow in self.service.list_workflows()]
        self.assertNotIn("Broken Import", workflow_names)

    def test_execute_step_runs_only_selected_step(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.service.save_workflow(None, "Single Step Run", "", True)
        first_step_id = self.service.save_step(
            None,
            workflow_id,
            1,
            "Tap One",
            "tap",
            json.dumps({"x": 11, "y": 22}),
            True,
        )
        self.service.save_step(
            None,
            workflow_id,
            2,
            "Tap Two",
            "tap",
            json.dumps({"x": 33, "y": 44}),
            True,
        )

        result = self.service.execute_step(workflow_id, first_step_id, device_id)

        self.assertTrue(result["success"])
        self.assertEqual(self.fake_device.actions, [("click", 11, 22)])
        logs = self.log_service.list_logs(workflow_id=workflow_id, device_id=device_id, limit=20)
        start_log = next(log for log in logs if log["status"] == "workflow_started")
        self.assertEqual(json.loads(start_log["metadata"])["execution_scope"], "selected_step")


if __name__ == "__main__":
    unittest.main()
