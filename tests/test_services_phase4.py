from __future__ import annotations

import json
import os
import tempfile
import unittest

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
        self.watcher_service = WatcherService(
            self.watcher_repository,
            self.device_repository,
            DeviceService(self.device_repository),
            self.log_service,
            self.watcher_telemetry_service,
        )
        self.service = WorkflowService(
            self.workflow_repository,
            self.device_repository,
            DeviceService(self.device_repository),
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


if __name__ == "__main__":
    unittest.main()
