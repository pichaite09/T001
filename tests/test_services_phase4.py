from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

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


class FakeDevice:
    def __init__(self) -> None:
        self.actions: list[tuple] = []
        self.info = {
            "manufacturer": "Google",
            "model": "Pixel 7",
            "version": "14",
        }

    def app_start(self, package: str):
        self.actions.append(("app_start", package))

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

    def app_current(self):
        return {"package": "com.example.app", "activity": "MainActivity"}

    def screen_on(self):
        return True


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
        self.account_repository = AccountRepository(self.db)
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
        self.account_service = AccountService(
            self.account_repository,
            self.device_repository,
            self.workflow_repository,
        )
        self.service = WorkflowService(
            self.workflow_repository,
            self.device_repository,
            self.device_service,
            self.log_service,
            self.telemetry_service,
            self.watcher_service,
            self.watcher_telemetry_service,
            self.account_service,
        )

    def tearDown(self) -> None:
        for extra_path in (
            Path(self.db_path).with_suffix(".png"),
            Path(self.db_path).with_suffix(".xml"),
        ):
            if extra_path.exists():
                extra_path.unlink()
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

    def test_device_connection_persists_runtime_info(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")

        success, _message, info = self.device_service.test_connection("SERIAL1", device_id)

        self.assertTrue(success)
        self.assertEqual(info["manufacturer"], "Google")
        device = self.device_repository.get_device(device_id)
        self.assertEqual(device["last_status"], "connected")
        saved_info = json.loads(device["last_info_json"])
        self.assertEqual(saved_info["model"], "Pixel 7")
        self.assertEqual(saved_info["version"], "14")
        self.assertEqual(saved_info["current_app"]["package"], "com.example.app")
        self.assertEqual(saved_info["window_size"]["width"], 1080)
        self.assertTrue(saved_info["screen_on"])

    def test_device_maintenance_actions_save_files(self) -> None:
        screenshot_path = Path(self.db_path).with_suffix(".png")
        hierarchy_path = Path(self.db_path).with_suffix(".xml")

        success, _message, saved_screenshot = self.device_service.capture_screenshot("SERIAL1", screenshot_path)
        self.assertTrue(success)
        self.assertEqual(Path(saved_screenshot), screenshot_path)
        self.assertTrue(screenshot_path.exists())

        success, _message, saved_hierarchy = self.device_service.dump_hierarchy("SERIAL1", hierarchy_path)
        self.assertTrue(success)
        self.assertEqual(Path(saved_hierarchy), hierarchy_path)
        self.assertTrue(hierarchy_path.exists())

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

    def test_switch_account_step_runs_platform_switch_workflow_and_updates_current_account(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        switch_workflow_id = self.service.save_workflow(None, "Shopee Switch", "", True)
        self.service.save_step(
            None,
            switch_workflow_id,
            1,
            "Capture Account",
            "set_variable",
            json.dumps(
                {
                    "variable_name": "switched_to",
                    "value_mode": "template",
                    "value": "${account.get('display_name')}",
                }
            ),
            True,
        )
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            switch_workflow_id,
            True,
        )
        account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "main-shop",
            "shop_user",
            "shop_login",
            "",
            "{}",
            True,
        )

        main_workflow_id = self.service.save_workflow(None, "Main Workflow", "", True)
        step_id = self.service.save_step(
            None,
            main_workflow_id,
            1,
            "Switch Account",
            "switch_account",
            json.dumps({"platform_key": "shopee", "account_name": "main-shop", "launch_package_first": True}),
            True,
        )

        result = self.service.execute_step(main_workflow_id, step_id, device_id)

        self.assertTrue(result["success"])
        self.assertIn(("app_start", "com.shopee.th"), self.fake_device.actions)
        device_platform = self.account_service.get_device_platform(device_platform_id)
        self.assertEqual(int(device_platform["current_account_id"]), account_id)

    def test_switch_account_resolves_alias_name(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        switch_workflow_id = self.service.save_workflow(None, "Shopee Switch", "", True)
        self.service.save_step(
            None,
            switch_workflow_id,
            1,
            "Capture Account",
            "set_variable",
            json.dumps(
                {
                    "variable_name": "switched_to",
                    "value_mode": "template",
                    "value": "${account.get('display_name')}",
                }
            ),
            True,
        )
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            switch_workflow_id,
            True,
        )
        account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "techat01",
            "",
            "",
            "",
            "{}",
            True,
            aliases_text="@techat01, techat",
        )

        main_workflow_id = self.service.save_workflow(None, "Main Workflow", "", True)
        step_id = self.service.save_step(
            None,
            main_workflow_id,
            1,
            "Switch Account",
            "switch_account",
            json.dumps({"platform_key": "shopee", "account_name": "@techat01", "launch_package_first": True}),
            True,
        )

        result = self.service.execute_step(main_workflow_id, step_id, device_id)

        self.assertTrue(result["success"])
        device_platform = self.account_service.get_device_platform(device_platform_id)
        self.assertEqual(int(device_platform["current_account_id"]), account_id)

    def test_save_account_rejects_conflicting_alias_identity(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            None,
            True,
        )
        self.account_service.save_account(
            None,
            device_platform_id,
            "techat01",
            "",
            "",
            "",
            "{}",
            True,
            aliases_text="@techat01, techat",
        )

        with self.assertRaisesRegex(ValueError, "already belongs"):
            self.account_service.save_account(
                None,
                device_platform_id,
                "another",
                "",
                "",
                "",
                "{}",
                True,
                aliases_text="  @techat01  ",
            )

    def test_save_account_allows_case_distinct_identity_values(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            None,
            True,
        )
        first_account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "Techat01",
            "",
            "",
            "",
            "{}",
            True,
            aliases_text="@TechatMain",
        )
        second_account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "techat01",
            "",
            "",
            "",
            "{}",
            True,
            aliases_text="@techatmain",
        )

        self.assertNotEqual(first_account_id, second_account_id)

    def test_switch_account_alias_lookup_is_case_sensitive(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        switch_workflow_id = self.service.save_workflow(None, "Shopee Switch", "", True)
        self.service.save_step(
            None,
            switch_workflow_id,
            1,
            "Capture Account",
            "set_variable",
            json.dumps(
                {
                    "variable_name": "switched_to",
                    "value_mode": "template",
                    "value": "${account.get('display_name')}",
                }
            ),
            True,
        )
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            switch_workflow_id,
            True,
        )
        self.account_service.save_account(
            None,
            device_platform_id,
            "Techat01",
            "",
            "",
            "",
            "{}",
            True,
            aliases_text="@Techat01",
        )

        with self.assertRaisesRegex(ValueError, "Account not found"):
            self.account_service.resolve_switch_target(device_id, "shopee", account_name="@techat01")

    def test_execute_workflow_injects_selected_account_context_and_filters_logs(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.service.save_workflow(None, "Context Run", "", True)
        self.service.save_step(
            None,
            workflow_id,
            1,
            "Capture Account",
            "set_variable",
            json.dumps(
                {
                    "variable_name": "selected_account",
                    "value_mode": "template",
                    "value": "${account.get('display_name')}",
                }
            ),
            True,
        )
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            None,
            True,
        )
        account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "main-shop",
            "shop_user",
            "shop_login",
            "",
            "{}",
            True,
        )
        self.account_service.set_current_account(device_platform_id, account_id)

        result = self.service.execute_workflow(
            workflow_id,
            device_id,
            device_platform_id=device_platform_id,
            use_current_account=True,
        )

        self.assertTrue(result["success"])
        filtered_logs = self.log_service.list_logs(
            workflow_id=workflow_id,
            device_id=device_id,
            platform_key="shopee",
            account_id=account_id,
            limit=20,
        )
        self.assertTrue(filtered_logs)
        start_log = next(log for log in filtered_logs if log["status"] == "workflow_started")
        metadata = json.loads(start_log["metadata"])
        self.assertEqual(metadata["platform_key"], "shopee")
        self.assertEqual(metadata["account_id"], account_id)

    def test_run_for_each_account_switches_and_runs_target_workflow(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        switch_workflow_id = self.service.save_workflow(None, "Shopee Switch", "", True)
        self.service.save_step(
            None,
            switch_workflow_id,
            1,
            "Capture Account",
            "set_variable",
            json.dumps(
                {
                    "variable_name": "switched_to",
                    "value_mode": "template",
                    "value": "${account.get('display_name')}",
                }
            ),
            True,
        )
        target_workflow_id = self.service.save_workflow(None, "Shopee Account Task", "", True)
        self.service.save_step(
            None,
            target_workflow_id,
            1,
            "Tap Task",
            "tap",
            json.dumps({"x": 101, "y": 202}),
            True,
        )
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            switch_workflow_id,
            True,
        )
        first_account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "shop-a",
            "shop_a",
            "shop_a",
            "",
            "{}",
            True,
        )
        second_account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "shop-b",
            "shop_b",
            "shop_b",
            "",
            "{}",
            True,
        )
        main_workflow_id = self.service.save_workflow(None, "Main Workflow", "", True)
        step_id = self.service.save_step(
            None,
            main_workflow_id,
            1,
            "Run All Accounts",
            "run_for_each_account",
            json.dumps(
                {
                    "platform_key": "shopee",
                    "target_workflow_id": target_workflow_id,
                    "only_enabled": True,
                    "launch_package_first": True,
                    "continue_on_account_error": False,
                }
            ),
            True,
        )

        result = self.service.execute_step(main_workflow_id, step_id, device_id)

        self.assertTrue(result["success"])
        tap_actions = [action for action in self.fake_device.actions if action == ("click", 101, 202)]
        self.assertEqual(len(tap_actions), 2)
        app_start_actions = [action for action in self.fake_device.actions if action == ("app_start", "com.shopee.th")]
        self.assertEqual(len(app_start_actions), 2)
        device_platform = self.account_service.get_device_platform(device_platform_id)
        self.assertEqual(int(device_platform["current_account_id"]), second_account_id)
        logs = self.log_service.list_logs(workflow_id=main_workflow_id, device_id=device_id, limit=20)
        start_log = next(log for log in logs if log["status"] == "workflow_started")
        start_metadata = json.loads(start_log["metadata"])
        self.assertEqual(start_metadata["execution_scope"], "selected_step")
        self.assertEqual(start_metadata["selected_step_ids"], [step_id])


if __name__ == "__main__":
    unittest.main()
