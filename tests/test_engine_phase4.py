from __future__ import annotations

import unittest
from unittest import mock

from automation_studio.automation.engine import WorkflowExecutor


class FakeLogService:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str, str, dict]] = []

    def add(self, workflow_id, device_id, level, status, message, metadata=None):
        self.entries.append((level, status, message, metadata or {}))
        return len(self.entries)


class FakeTelemetryService:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def record_step_result(self, workflow_id, device_id, step_type, outcome, duration_ms, error_message=""):
        self.records.append(
            {
                "workflow_id": workflow_id,
                "device_id": device_id,
                "step_type": step_type,
                "outcome": outcome,
                "duration_ms": duration_ms,
                "error_message": error_message,
            }
        )


class FakeDevice:
    info = {"displayWidth": 1080, "displayHeight": 2400}

    def __init__(self) -> None:
        self.shell_commands: list[str] = []

    def shell(self, command):
        self.shell_commands.append(command)
        return (f"ok:{command}", 0)

    def screenshot(self, path):
        return path

    def dump_hierarchy(self):
        return "<hierarchy/>"

    def window_size(self):
        return (1080, 2400)


class EnginePhase4Tests(unittest.TestCase):
    def test_plugin_step_and_telemetry_are_recorded(self) -> None:
        log_service = FakeLogService()
        telemetry_service = FakeTelemetryService()
        executor = WorkflowExecutor(
            device=FakeDevice(),
            workflow={"id": 1, "name": "Plugin Run"},
            device_record={"id": 2, "serial": "SERIAL", "name": "Device"},
            log_service=log_service,
            telemetry_service=telemetry_service,
        )

        steps = [
            {
                "id": 1,
                "position": 1,
                "name": "Set User",
                "step_type": "set_variable",
                "parameters": '{"variable_name":"user","value_mode":"literal","value":"Neo"}',
                "is_enabled": True,
                "schema_version": 2,
            },
            {
                "id": 2,
                "position": 2,
                "name": "Plugin Echo",
                "step_type": "plugin:echo_context",
                "parameters": '{"message":"Hello ${vars.get(\\"user\\")}","write_variable":"plugin_out"}',
                "is_enabled": True,
                "schema_version": 2,
            },
        ]

        summary = executor.run(steps)

        self.assertEqual(summary["executed_steps"], 2)
        self.assertEqual(executor.context["vars"]["plugin_out"], "Hello Neo")
        self.assertEqual(len(telemetry_service.records), 2)
        self.assertEqual(telemetry_service.records[-1]["step_type"], "plugin:echo_context")
        self.assertEqual(telemetry_service.records[-1]["outcome"], "success")

    def test_random_wait_and_loop_until_elapsed_behave_deterministically(self) -> None:
        log_service = FakeLogService()
        telemetry_service = FakeTelemetryService()
        executor = WorkflowExecutor(
            device=FakeDevice(),
            workflow={"id": 1, "name": "Loop Run"},
            device_record={"id": 2, "serial": "SERIAL", "name": "Device"},
            log_service=log_service,
            telemetry_service=telemetry_service,
        )

        steps = [
            {
                "id": 1,
                "position": 1,
                "name": "Random Wait",
                "step_type": "random_wait",
                "parameters": '{"min_seconds":5,"max_seconds":10}',
                "is_enabled": True,
                "schema_version": 2,
            },
            {
                "id": 2,
                "position": 2,
                "name": "Loop Ten Minutes",
                "step_type": "loop_until_elapsed",
                "parameters": '{"duration_minutes":10,"target_position":1}',
                "is_enabled": True,
                "schema_version": 2,
            },
        ]

        with mock.patch("automation_studio.automation.engine.random.uniform", return_value=7.5), \
            mock.patch("automation_studio.automation.engine.time.sleep") as sleep_mock, \
            mock.patch("automation_studio.automation.engine.time.monotonic", side_effect=[0.0, 0.0, 120.0, 601.0]):
            summary = executor.run(steps)

        self.assertEqual(summary["jump_count"], 1)
        self.assertEqual(summary["executed_steps"], 4)
        sleep_mock.assert_any_call(7.5)

    def test_chance_gate_can_skip_next_step(self) -> None:
        log_service = FakeLogService()
        telemetry_service = FakeTelemetryService()
        device = FakeDevice()
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 1, "name": "Chance Gate"},
            device_record={"id": 2, "serial": "SERIAL", "name": "Device"},
            log_service=log_service,
            telemetry_service=telemetry_service,
        )

        steps = [
            {
                "id": 1,
                "position": 1,
                "name": "Maybe Like",
                "step_type": "chance_gate",
                "parameters": '{"probability_percent":10,"skip_count_on_fail":1}',
                "is_enabled": True,
                "schema_version": 2,
            },
            {
                "id": 2,
                "position": 2,
                "name": "Like Action",
                "step_type": "shell",
                "parameters": '{"command":"input tap 1 1"}',
                "is_enabled": True,
                "schema_version": 2,
            },
            {
                "id": 3,
                "position": 3,
                "name": "Always Run",
                "step_type": "shell",
                "parameters": '{"command":"echo done"}',
                "is_enabled": True,
                "schema_version": 2,
            },
        ]

        with mock.patch("automation_studio.automation.engine.random.uniform", return_value=99.0):
            summary = executor.run(steps)

        self.assertEqual(summary["jump_count"], 1)
        self.assertEqual(device.shell_commands, ["echo done"])


if __name__ == "__main__":
    unittest.main()
