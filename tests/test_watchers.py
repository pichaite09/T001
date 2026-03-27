from __future__ import annotations

import json
import os
import tempfile
import unittest

from automation_studio.automation.engine import WorkflowExecutor
from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    DeviceRepository,
    LogRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)
from automation_studio.services import LogService, WatcherService, WatcherTelemetryService


class FakeLogService:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str]] = []

    def add(self, workflow_id, device_id, level, status, message, metadata=None):
        self.entries.append((status, message))
        return 1


class FakeSelector:
    def __init__(self, exists: bool = True) -> None:
        self._exists = exists
        self.click_count = 0

    def exists(self, timeout=None):
        return self._exists

    def wait(self, timeout=None):
        return self._exists

    def click(self):
        self.click_count += 1


class FakeDevice:
    def __init__(self) -> None:
        self.selector_map: dict[tuple[tuple[str, object], ...], FakeSelector] = {}
        self.actions: list[tuple] = []
        self.current_packages: list[str] = ["com.example.app"]

    def register_selector(self, selector: FakeSelector, **kwargs):
        key = tuple(sorted(kwargs.items()))
        self.selector_map[key] = selector
        return selector

    def __call__(self, **kwargs):
        key = tuple(sorted(kwargs.items()))
        return self.selector_map.get(key, FakeSelector(False))

    def click(self, x: int, y: int):
        self.actions.append(("click", x, y))

    def press(self, key: str):
        self.actions.append(("press", key))

    def screenshot(self, path: str):
        with open(path, "wb") as handle:
            handle.write(b"img")
        self.actions.append(("screenshot", path))

    def dump_hierarchy(self):
        self.actions.append(("dump_hierarchy",))
        return "<hierarchy/>"

    def app_current(self):
        if len(self.current_packages) > 1:
            package = self.current_packages.pop(0)
        else:
            package = self.current_packages[0]
        return {"package": package}


class FakeDeviceService:
    def __init__(self, device):
        self.device = device

    def connect_device(self, serial: str):
        return self.device


class WatcherServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = DatabaseManager(self.db_path)
        self.db.init_schema()
        self.log_service = LogService(LogRepository(self.db))
        self.watcher_telemetry_service = WatcherTelemetryService(WatcherTelemetryRepository(self.db))
        self.service = WatcherService(
            WatcherRepository(self.db),
            DeviceRepository(self.db),
            None,
            self.log_service,
            self.watcher_telemetry_service,
        )
        self.workflow_repository = WorkflowRepository(self.db)
        self.device_repository = DeviceRepository(self.db)
        self.workflow_id = self.workflow_repository.upsert_workflow(None, "WF", "", True, 2)
        self.device_id = self.device_repository.upsert_device(None, "D1", "SERIAL1", "")

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_resolve_active_watchers_merges_global_workflow_and_device_scopes(self) -> None:
        global_id = self.service.save_watcher(
            None,
            "Global Guard",
            "global",
            None,
            "elapsed_time",
            json.dumps({"seconds": 0}),
            "stop_workflow",
            json.dumps({"reason": "stop"}),
            json.dumps({"cooldown_seconds": 0, "max_triggers_per_run": 1, "active_stages": ["before_step"]}),
            True,
            10,
        )
        workflow_id = self.service.save_watcher(
            None,
            "Workflow Guard",
            "workflow",
            self.workflow_id,
            "expression",
            json.dumps({"expression": "True"}),
            "set_variable",
            json.dumps({"variable_name": "x", "value": "1"}),
            json.dumps({"cooldown_seconds": 0, "active_stages": ["before_step"]}),
            True,
            20,
        )
        device_id = self.service.save_watcher(
            None,
            "Device Guard",
            "device",
            self.device_id,
            "app_in_foreground",
            json.dumps({"package": "com.example.app"}),
            "press_back",
            json.dumps({}),
            json.dumps({"cooldown_seconds": 0, "active_stages": ["before_step"]}),
            True,
            30,
        )

        watchers = self.service.resolve_active_watchers(self.workflow_id, self.device_id)
        self.assertEqual([watcher["id"] for watcher in watchers], [global_id, workflow_id, device_id])

    def test_watcher_telemetry_summary_records_results(self) -> None:
        watcher_id = self.service.save_watcher(
            None,
            "Telemetry Guard",
            "global",
            None,
            "elapsed_time",
            json.dumps({"seconds": 0}),
            "stop_workflow",
            json.dumps({"reason": "stop"}),
            json.dumps({"cooldown_seconds": 0, "active_stages": ["before_step"]}),
            True,
            10,
        )
        self.watcher_telemetry_service.record_watcher_result(watcher_id, self.workflow_id, self.device_id, "success", "")
        summary = self.watcher_telemetry_service.summary(self.workflow_id, self.device_id, limit=5)
        self.assertEqual(summary[0]["trigger_count"], 1)
        self.assertEqual(summary[0]["success_count"], 1)
        self.assertEqual(summary[0]["success_rate"], 100.0)
        self.assertEqual(summary[0]["failure_rate"], 0.0)

    def test_profile_watchers_can_be_attached_to_workflow(self) -> None:
        popup_watcher_id = self.service.save_watcher(
            None,
            "Popup Guard",
            "global",
            None,
            "selector_exists",
            json.dumps({"text": "Allow", "timeout": 0}),
            "press_back",
            json.dumps({}),
            json.dumps({"cooldown_seconds": 0, "active_stages": ["before_step"]}),
            True,
            15,
        )
        profile_id = self.service.save_profile(
            None,
            "Popup Recovery",
            "Shared popup handlers",
            [popup_watcher_id],
            True,
        )

        self.service.save_workflow_profiles(self.workflow_id, [profile_id])

        profiles = self.service.list_profiles_for_workflow(self.workflow_id)
        self.assertEqual([profile["id"] for profile in profiles], [profile_id])
        profile_watchers = self.service.list_profile_watchers(profile_id)
        self.assertEqual([watcher["id"] for watcher in profile_watchers], [popup_watcher_id])

    def test_resolve_active_watchers_dedupes_direct_and_profile_sources(self) -> None:
        shared_watcher_id = self.service.save_watcher(
            None,
            "Shared Guard",
            "workflow",
            self.workflow_id,
            "expression",
            json.dumps({"expression": "True"}),
            "set_variable",
            json.dumps({"variable_name": "seen", "value": "1"}),
            json.dumps({"cooldown_seconds": 0, "active_stages": ["before_step"]}),
            True,
            12,
        )
        self.service.save_watcher(
            None,
            "Global Guard",
            "global",
            None,
            "elapsed_time",
            json.dumps({"seconds": 0}),
            "press_back",
            json.dumps({}),
            json.dumps({"cooldown_seconds": 0, "active_stages": ["before_step"]}),
            True,
            5,
        )
        profile_id = self.service.save_profile(
            None,
            "Shared Profile",
            "Contains workflow watcher too",
            [shared_watcher_id],
            True,
        )
        self.service.save_workflow_profiles(self.workflow_id, [profile_id])

        watchers = self.service.resolve_active_watchers(self.workflow_id, self.device_id)
        shared_matches = [watcher for watcher in watchers if watcher["id"] == shared_watcher_id]
        self.assertEqual(len(shared_matches), 1)
        self.assertEqual(shared_matches[0]["id"], shared_watcher_id)

    def test_profile_name_must_be_unique(self) -> None:
        watcher_id = self.service.save_watcher(
            None,
            "Unique Guard",
            "global",
            None,
            "expression",
            json.dumps({"expression": "True"}),
            "press_back",
            json.dumps({}),
            json.dumps({"cooldown_seconds": 0, "active_stages": ["before_step"]}),
            True,
            10,
        )
        self.service.save_profile(None, "Common Profile", "", [watcher_id], True)

        with self.assertRaisesRegex(ValueError, "Profile name already exists"):
            self.service.save_profile(None, "common profile", "", [watcher_id], True)

    def test_log_service_can_filter_by_watcher_id(self) -> None:
        self.log_service.add(self.workflow_id, self.device_id, "INFO", "watcher_matched", "Watcher one", {}, watcher_id=11)
        self.log_service.add(self.workflow_id, self.device_id, "INFO", "watcher_matched", "Watcher two", {}, watcher_id=22)

        filtered = self.log_service.list_logs(workflow_id=self.workflow_id, device_id=self.device_id, watcher_id=22, limit=10)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["watcher_id"], 22)


class WatcherRuntimeTests(unittest.TestCase):
    def test_selector_watcher_runs_click_action_before_step(self) -> None:
        device = FakeDevice()
        selector = device.register_selector(FakeSelector(True), text="Allow")
        log_service = FakeLogService()
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 1, "name": "Watcher Runtime"},
            device_record={"id": 1, "serial": "SER", "name": "Fake"},
            log_service=log_service,
            watchers=[
                {
                    "id": 1,
                    "name": "Allow Popup",
                    "scope_type": "global",
                    "scope_id": None,
                    "condition_type": "selector_exists",
                    "condition_json": json.dumps({"text": "Allow", "timeout": 0}),
                    "action_type": "run_step",
                    "action_json": json.dumps({"step_type": "click", "parameters": {"text": "Allow", "timeout": 0}}),
                    "policy_json": json.dumps({"cooldown_seconds": 0, "max_triggers_per_run": 1, "active_stages": ["before_step"]}),
                    "priority": 10,
                    "is_enabled": 1,
                }
            ],
        )
        steps = [
            {
                "id": 1,
                "position": 1,
                "name": "Main Step",
                "step_type": "wait",
                "parameters": json.dumps({"seconds": 0}),
                "is_enabled": True,
                "schema_version": 2,
            }
        ]

        summary = executor.run(steps)
        self.assertEqual(selector.click_count, 1)
        self.assertEqual(summary["watcher_trigger_count"], 1)
        self.assertFalse(summary["stopped_by_watcher"])

    def test_stop_workflow_watcher_stops_execution(self) -> None:
        executor = WorkflowExecutor(
            device=FakeDevice(),
            workflow={"id": 1, "name": "Watcher Stop"},
            device_record={"id": 1, "serial": "SER", "name": "Fake"},
            log_service=FakeLogService(),
            watchers=[
                {
                    "id": 2,
                    "name": "Emergency Stop",
                    "scope_type": "global",
                    "scope_id": None,
                    "condition_type": "elapsed_time",
                    "condition_json": json.dumps({"seconds": 0}),
                    "action_type": "stop_workflow",
                    "action_json": json.dumps({"reason": "Stop requested"}),
                    "policy_json": json.dumps({"cooldown_seconds": 0, "max_triggers_per_run": 1, "active_stages": ["before_step"]}),
                    "priority": 1,
                    "is_enabled": 1,
                }
            ],
        )
        steps = [
            {
                "id": 1,
                "position": 1,
                "name": "Tap",
                "step_type": "tap",
                "parameters": json.dumps({"x": 10, "y": 20}),
                "is_enabled": True,
                "schema_version": 2,
            }
        ]

        summary = executor.run(steps)
        self.assertTrue(summary["stopped_by_watcher"])
        self.assertEqual(summary["executed_steps"], 0)
        self.assertEqual(summary["stop_reason"], "Stop requested")

    def test_debounce_requires_two_consecutive_matches(self) -> None:
        device = FakeDevice()
        selector = device.register_selector(FakeSelector(True), text="Allow")
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 1, "name": "Watcher Debounce"},
            device_record={"id": 1, "serial": "SER", "name": "Fake"},
            log_service=FakeLogService(),
            watchers=[
                {
                    "id": 3,
                    "name": "Debounced Allow",
                    "scope_type": "global",
                    "scope_id": None,
                    "condition_type": "selector_exists",
                    "condition_json": json.dumps({"text": "Allow", "timeout": 0}),
                    "action_type": "run_step",
                    "action_json": json.dumps({"step_type": "click", "parameters": {"text": "Allow", "timeout": 0}}),
                    "policy_json": json.dumps({"cooldown_seconds": 0, "debounce_count": 2, "max_triggers_per_run": 1, "active_stages": ["before_step"]}),
                    "priority": 1,
                    "is_enabled": 1,
                }
            ],
        )
        steps = [
            {"id": 1, "position": 1, "name": "Wait 1", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2},
            {"id": 2, "position": 2, "name": "Wait 2", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2},
        ]

        summary = executor.run(steps)
        self.assertEqual(selector.click_count, 1)
        self.assertEqual(summary["watcher_trigger_count"], 1)

    def test_action_chain_runs_screenshot_then_back(self) -> None:
        device = FakeDevice()
        device.register_selector(FakeSelector(True), text="Allow")
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 1, "name": "Watcher Chain"},
            device_record={"id": 1, "serial": "SER", "name": "Fake"},
            log_service=FakeLogService(),
            watchers=[
                {
                    "id": 4,
                    "name": "Chain Popup Handler",
                    "scope_type": "global",
                    "scope_id": None,
                    "condition_type": "selector_exists",
                    "condition_json": json.dumps({"text": "Allow", "timeout": 0}),
                    "action_type": "action_chain",
                    "action_json": json.dumps(
                        {
                            "actions": [
                                {"action_type": "take_screenshot", "action": {"filename_prefix": "popup_before_back"}},
                                {"action_type": "press_back", "action": {}},
                            ]
                        }
                    ),
                    "policy_json": json.dumps({"cooldown_seconds": 0, "max_triggers_per_run": 1, "active_stages": ["before_step"]}),
                    "priority": 1,
                    "is_enabled": 1,
                }
            ],
        )
        steps = [
            {"id": 1, "position": 1, "name": "Wait", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2}
        ]

        summary = executor.run(steps)
        self.assertEqual(summary["watcher_trigger_count"], 1)
        self.assertTrue(any(action[0] == "screenshot" for action in device.actions))
        self.assertIn(("press", "back"), device.actions)

    def test_package_changed_can_stop_on_target_package(self) -> None:
        device = FakeDevice()
        device.current_packages = ["com.example.home", "com.example.app"]
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 1, "name": "Watcher Package Change"},
            device_record={"id": 1, "serial": "SER", "name": "Fake"},
            log_service=FakeLogService(),
            watchers=[
                {
                    "id": 5,
                    "name": "Stop On App Change",
                    "scope_type": "global",
                    "scope_id": None,
                    "condition_type": "package_changed",
                    "condition_json": json.dumps({"package": "com.example.app"}),
                    "action_type": "stop_workflow",
                    "action_json": json.dumps({"reason": "Package changed to target"}),
                    "policy_json": json.dumps({"cooldown_seconds": 0, "max_triggers_per_run": 1, "active_stages": ["before_step"]}),
                    "priority": 1,
                    "is_enabled": 1,
                }
            ],
        )
        steps = [
            {"id": 1, "position": 1, "name": "Wait 1", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2},
            {"id": 2, "position": 2, "name": "Wait 2", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2},
        ]

        summary = executor.run(steps)
        self.assertTrue(summary["stopped_by_watcher"])
        self.assertEqual(summary["stop_reason"], "Package changed to target")

    def test_watcher_safety_stop_triggers_when_run_exceeds_limit(self) -> None:
        device = FakeDevice()
        selector = device.register_selector(FakeSelector(True), text="Allow")
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 1, "name": "Watcher Safety"},
            device_record={"id": 1, "serial": "SER", "name": "Fake"},
            log_service=FakeLogService(),
            watchers=[
                {
                    "id": 6,
                    "name": "Storm Guard",
                    "scope_type": "global",
                    "scope_id": None,
                    "condition_type": "selector_exists",
                    "condition_json": json.dumps({"text": "Allow", "timeout": 0}),
                    "action_type": "run_step",
                    "action_json": json.dumps({"step_type": "click", "parameters": {"text": "Allow", "timeout": 0}}),
                    "policy_json": json.dumps({"cooldown_seconds": 0, "match_mode": "continue", "max_triggers_per_run": 0, "active_stages": ["before_step"]}),
                    "priority": 1,
                    "is_enabled": 1,
                }
            ],
        )
        executor._watcher_trigger_limit = 2
        steps = [
            {"id": 1, "position": 1, "name": "Wait 1", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2},
            {"id": 2, "position": 2, "name": "Wait 2", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2},
            {"id": 3, "position": 3, "name": "Wait 3", "step_type": "wait", "parameters": json.dumps({"seconds": 0}), "is_enabled": True, "schema_version": 2},
        ]

        summary = executor.run(steps)
        self.assertTrue(summary["stopped_by_watcher"])
        self.assertIn("Watcher safety stop", summary["stop_reason"])
        self.assertEqual(summary["watcher_trigger_limit"], 2)
        self.assertGreaterEqual(summary["watcher_trigger_count"], 3)
        self.assertGreaterEqual(selector.click_count, 2)


class WatcherDeviceTestServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = DatabaseManager(self.db_path)
        self.db.init_schema()
        self.device_repository = DeviceRepository(self.db)
        self.workflow_repository = WorkflowRepository(self.db)
        self.device_id = self.device_repository.upsert_device(None, "D1", "SERIAL1", "")
        self.workflow_repository.upsert_workflow(None, "WF", "", True, 2)
        self.fake_device = FakeDevice()
        self.fake_device.register_selector(FakeSelector(True), text="Allow")
        self.service = WatcherService(
            WatcherRepository(self.db),
            self.device_repository,
            FakeDeviceService(self.fake_device),
            LogService(LogRepository(self.db)),
            WatcherTelemetryService(WatcherTelemetryRepository(self.db)),
        )

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_test_condition_matches_selector(self) -> None:
        matched, message, metadata = self.service.test_condition(
            device_id=self.device_id,
            condition_type="selector_exists",
            condition_text=json.dumps({"text": "Allow", "timeout": 0}),
        )
        self.assertTrue(matched)
        self.assertIn("matched", message.lower())
        self.assertEqual(metadata["selector_type"], "selector")

    def test_test_action_executes_run_step_click(self) -> None:
        selector = self.fake_device.register_selector(FakeSelector(True), text="Confirm")
        success, message, metadata = self.service.test_action(
            device_id=self.device_id,
            action_type="run_step",
            action_text=json.dumps({"step_type": "click", "parameters": {"text": "Confirm", "timeout": 0}}),
        )
        self.assertTrue(success)
        self.assertIn("executed", message.lower())
        self.assertEqual(selector.click_count, 1)
        self.assertEqual(metadata["action_step_type"], "click")


if __name__ == "__main__":
    unittest.main()
