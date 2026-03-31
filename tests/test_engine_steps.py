from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from automation_studio.automation.engine import WorkflowExecutor


class FakeLogService:
    def add(self, workflow_id, device_id, level, status, message, metadata=None):
        return 1


class FakeNode:
    def __init__(self, info: dict | None = None, attrib: dict | None = None) -> None:
        self.info = info or {}
        self.attrib = attrib or {}


class FakeSelector:
    def __init__(
        self,
        *,
        exists_states: list[bool] | None = None,
        info: dict | None = None,
        text: str = "",
        attrib: dict | None = None,
    ) -> None:
        self._exists_states = list(exists_states or [True])
        self.info = info or {}
        self.text = text
        self.attrib = attrib or {}
        self.click_count = 0
        self.clear_count = 0
        self.set_text_calls: list[str] = []
        self.long_click_calls: list[float | None] = []

    def exists(self, timeout=None):
        if len(self._exists_states) > 1:
            return self._exists_states.pop(0)
        return self._exists_states[0]

    def wait(self, timeout=None):
        return self.exists(timeout=timeout)

    def click(self):
        self.click_count += 1

    def clear_text(self):
        self.clear_count += 1

    def set_text(self, value: str):
        self.set_text_calls.append(value)
        self.text = value

    def long_click(self, duration=None):
        self.long_click_calls.append(duration)

    def get_text(self):
        return self.text

    def get(self):
        return FakeNode(info=self.info, attrib=self.attrib)


class FakeDevice:
    def __init__(self) -> None:
        self.info = {"displayWidth": 1080, "displayHeight": 2400}
        self.selector_map: dict[tuple[tuple[str, object], ...], FakeSelector] = {}
        self.xpath_map: dict[str, FakeSelector] = {}
        self.actions: list[tuple] = []
        self.shell_output: dict[str, tuple[str, int]] = {}

    def register_selector(self, selector: FakeSelector, **kwargs):
        key = tuple(sorted(kwargs.items()))
        self.selector_map[key] = selector
        return selector

    def register_xpath(self, xpath: str, selector: FakeSelector):
        self.xpath_map[xpath] = selector
        return selector

    def __call__(self, **kwargs):
        key = tuple(sorted(kwargs.items()))
        return self.selector_map.get(key, FakeSelector(exists_states=[False]))

    def xpath(self, xpath: str):
        return self.xpath_map.get(xpath, FakeSelector(exists_states=[False]))

    def app_start(self, package: str):
        self.actions.append(("app_start", package))

    def app_stop(self, package: str):
        self.actions.append(("app_stop", package))

    def click(self, x: int, y: int):
        self.actions.append(("click", x, y))

    def long_click(self, x: int, y: int, duration: float):
        self.actions.append(("long_click", x, y, duration))

    def send_keys(self, text: str, clear: bool = True):
        self.actions.append(("send_keys", text, clear))

    def swipe(self, x1: int, y1: int, x2: int, y2: int, duration: float):
        self.actions.append(("swipe", x1, y1, x2, y2, duration))

    def press(self, key: str):
        self.actions.append(("press", key))

    def shell(self, command: str):
        self.actions.append(("shell", command))
        return self.shell_output.get(command, (f"ok:{command}", 0))

    def screenshot(self, path: str):
        Path(path).write_bytes(b"fake-image")
        self.actions.append(("screenshot", path))
        return path

    def dump_hierarchy(self):
        self.actions.append(("dump_hierarchy",))
        return "<hierarchy/>"

    def window_size(self):
        return (1080, 2400)


class EngineStepSuiteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.device = FakeDevice()
        self.executor = WorkflowExecutor(
            device=self.device,
            workflow={"id": 1, "name": "Step Suite"},
            device_record={"id": 1, "serial": "SERIAL", "name": "Fake Device"},
            log_service=FakeLogService(),
        )

    def _step(self, step_type: str, position: int = 1) -> dict:
        return {
            "id": position,
            "position": position,
            "name": step_type,
            "step_type": step_type,
            "is_enabled": True,
            "schema_version": 2,
        }

    def _runtime(self, step_type: str, position: int = 1) -> dict:
        step = self._step(step_type, position)
        return {"step": step, "repeat_iteration": 1, "repeat_times": 1}

    def _execute(self, step_type: str, parameters: dict, position: int = 1):
        step = self._step(step_type, position)
        runtime = {"step": step, "repeat_iteration": 1, "repeat_times": 1}
        return self.executor.execute_step(step, parameters, runtime)

    def test_launch_app_step(self) -> None:
        result = self._execute("launch_app", {"package": "com.example.app"})
        self.assertEqual(result["package"], "com.example.app")
        self.assertIn(("app_start", "com.example.app"), self.device.actions)

    def test_stop_app_step(self) -> None:
        result = self._execute("stop_app", {"package": "com.example.app"})
        self.assertEqual(result["package"], "com.example.app")
        self.assertIn(("app_stop", "com.example.app"), self.device.actions)

    def test_launch_activity_step(self) -> None:
        result = self._execute(
            "launch_activity",
            {
                "package": "com.example.clone",
                "activity": "com.example.clone.MainActivity",
                "action": "android.intent.action.MAIN",
                "category": "android.intent.category.LAUNCHER",
            },
        )
        self.assertEqual(result["package"], "com.example.clone")
        self.assertEqual(result["component"], "com.example.clone/com.example.clone.MainActivity")
        self.assertIn(
            (
                "shell",
                "am start -a android.intent.action.MAIN -c android.intent.category.LAUNCHER -n com.example.clone/com.example.clone.MainActivity",
            ),
            self.device.actions,
        )

    def test_launch_app_monkey_step(self) -> None:
        result = self._execute(
            "launch_app_monkey",
            {
                "package": "com.example.clone",
                "category": "android.intent.category.LAUNCHER",
                "event_count": 1,
            },
        )
        self.assertEqual(result["package"], "com.example.clone")
        self.assertEqual(result["event_count"], 1)
        self.assertIn(
            ("shell", "monkey -p com.example.clone -c android.intent.category.LAUNCHER 1"),
            self.device.actions,
        )

    def test_tap_step(self) -> None:
        result = self._execute("tap", {"x": 10, "y": 20})
        self.assertEqual(result, {"x": 10, "y": 20})
        self.assertIn(("click", 10, 20), self.device.actions)

    def test_click_step(self) -> None:
        selector = self.device.register_selector(FakeSelector(), text="Login")
        result = self._execute("click", {"text": "Login", "timeout": 5})
        self.assertEqual(result["selector_type"], "selector")
        self.assertEqual(selector.click_count, 1)

    def test_long_click_step(self) -> None:
        self.device.register_selector(
            FakeSelector(info={"bounds": {"left": 10, "top": 20, "right": 30, "bottom": 40}}),
            text="Hold",
        )
        result = self._execute("long_click", {"text": "Hold", "duration": 0.9})
        self.assertEqual(result["duration"], 0.9)
        self.assertIn(("long_click", 20, 30, 0.9), self.device.actions)

    def test_double_click_step(self) -> None:
        with mock.patch("automation_studio.automation.engine.time.sleep") as sleep_mock:
            result = self._execute("double_click", {"x": 30, "y": 40, "interval_seconds": 0.2})
        self.assertEqual(result["interval_seconds"], 0.2)
        self.assertEqual(self.device.actions.count(("click", 30, 40)), 2)
        sleep_mock.assert_called_once_with(0.2)

    def test_set_text_step(self) -> None:
        selector = self.device.register_xpath("//input", FakeSelector())
        result = self._execute(
            "set_text",
            {"xpath": "//input", "text": "demo", "clear_first": True},
        )
        self.assertEqual(result["text_length"], 4)
        self.assertEqual(selector.clear_count, 0)
        self.assertEqual(selector.set_text_calls, ["demo"])

    def test_wait_step(self) -> None:
        with mock.patch("automation_studio.automation.engine.time.sleep") as sleep_mock:
            result = self._execute("wait", {"seconds": 2.5})
        self.assertEqual(result["seconds"], 2.5)
        sleep_mock.assert_called_once_with(2.5)

    def test_random_wait_step(self) -> None:
        with mock.patch("automation_studio.automation.engine.random.uniform", return_value=7.25), \
            mock.patch("automation_studio.automation.engine.time.sleep") as sleep_mock:
            result = self._execute("random_wait", {"min_seconds": 5, "max_seconds": 9})
        self.assertEqual(result["actual_seconds"], 7.25)
        sleep_mock.assert_called_once_with(7.25)

    def test_wait_for_text_step(self) -> None:
        self.device.register_selector(FakeSelector(), text="Success")
        result = self._execute("wait_for_text", {"text": "Success", "timeout": 3})
        self.assertEqual(result["selector_type"], "selector")

    def test_wait_for_element_step(self) -> None:
        selector = self.device.register_selector(
            FakeSelector(exists_states=[True, False]),
            resourceId="com.example:id/loading",
        )
        with mock.patch("automation_studio.automation.engine.time.sleep") as sleep_mock, \
            mock.patch("automation_studio.automation.engine.time.time", side_effect=[0.0, 0.1, 0.2]):
            result = self._execute(
                "wait_for_element",
                {
                    "resource_id": "com.example:id/loading",
                    "desired_state": "gone",
                    "timeout": 1,
                    "poll_interval_seconds": 0.1,
                },
            )
        self.assertEqual(result["desired_state"], "gone")
        self.assertFalse(selector.exists())
        sleep_mock.assert_called_once_with(0.1)

    def test_swipe_step(self) -> None:
        result = self._execute("swipe", {"direction": "up", "scale": 0.5, "duration": 0.2})
        self.assertEqual(result["repeat"], 1)
        self.assertEqual(self.device.actions[-1][0], "swipe")

    def test_scroll_step(self) -> None:
        result = self._execute("scroll", {"direction": "down", "repeat": 2, "duration": 0.1})
        self.assertEqual(result["mode"], "scroll")
        swipe_actions = [action for action in self.device.actions if action[0] == "swipe"]
        self.assertEqual(len(swipe_actions), 2)

    def test_scroll_to_selector_step(self) -> None:
        self.device.register_selector(
            FakeSelector(exists_states=[False, False, True]),
            text="Buy Now",
        )
        result = self._execute(
            "scroll_to_selector",
            {
                "text": "Buy Now",
                "direction": "up",
                "max_swipes": 3,
                "timeout": 0,
                "duration": 0.1,
                "pause_seconds": 0,
            },
        )
        self.assertTrue(result["found"])
        self.assertEqual(result["swipes_used"], 2)
        swipe_actions = [action for action in self.device.actions if action[0] == "swipe"]
        self.assertEqual(len(swipe_actions), 2)

    def test_press_key_step(self) -> None:
        result = self._execute("press_key", {"key": "back"})
        self.assertEqual(result["key"], "back")
        self.assertIn(("press", "back"), self.device.actions)

    def test_input_keycode_step(self) -> None:
        result = self._execute("input_keycode", {"keycode": 66, "long_press": True})
        self.assertEqual(result["keycode"], 66)
        self.assertIn(("shell", "input keyevent --longpress 66"), self.device.actions)

    def test_shell_step(self) -> None:
        self.device.shell_output["echo hi"] = ("hi", 0)
        result = self._execute("shell", {"command": "echo hi"})
        self.assertEqual(result["shell_output"], "hi")
        self.assertEqual(result["exit_code"], 0)

    def test_screenshot_step(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self._execute("screenshot", {"directory": temp_dir, "filename": "screen.png"})
            self.assertTrue(Path(result["artifact_path"]).exists())

    def test_dump_hierarchy_step(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self._execute("dump_hierarchy", {"directory": temp_dir, "filename": "view.xml"})
            artifact_path = Path(result["artifact_path"])
            self.assertTrue(artifact_path.exists())
            self.assertEqual(artifact_path.read_text(encoding="utf-8"), "<hierarchy/>")

    def test_assert_exists_step(self) -> None:
        self.device.register_selector(FakeSelector(), resourceId="com.example:id/result")
        result = self._execute("assert_exists", {"resource_id": "com.example:id/result", "timeout": 2})
        self.assertEqual(result["selector_type"], "selector")

    def test_assert_text_step(self) -> None:
        self.device.register_selector(
            FakeSelector(text="Success", info={"text": "Success"}),
            resourceId="com.example:id/result",
        )
        result = self._execute(
            "assert_text",
            {
                "resource_id": "com.example:id/result",
                "expected_text": "Succ",
                "match_mode": "contains",
                "timeout": 2,
            },
        )
        self.assertEqual(result["actual_text"], "Success")

    def test_assert_state_step(self) -> None:
        self.device.register_selector(
            FakeSelector(info={"selected": True}),
            resourceId="com.ss.android.ugc.trill:id/n17",
        )
        result = self._execute(
            "assert_state",
            {
                "resource_id": "com.ss.android.ugc.trill:id/n17",
                "state_name": "selected",
                "expected": True,
                "timeout": 2,
            },
        )
        self.assertTrue(result["actual"])
        self.assertEqual(result["state_name"], "selected")

    def test_assert_state_step_can_check_false(self) -> None:
        self.device.register_selector(
            FakeSelector(info={"selected": False}),
            resourceId="com.ss.android.ugc.trill:id/n17",
        )
        result = self._execute(
            "assert_state",
            {
                "resource_id": "com.ss.android.ugc.trill:id/n17",
                "state_name": "selected",
                "expected": False,
                "timeout": 2,
            },
        )
        self.assertFalse(result["actual"])

    def test_branch_on_state_step_true_branch(self) -> None:
        self.device.register_selector(
            FakeSelector(info={"selected": True}),
            resourceId="com.ss.android.ugc.trill:id/n17",
        )
        result = self._execute(
            "branch_on_state",
            {
                "resource_id": "com.ss.android.ugc.trill:id/n17",
                "state_name": "selected",
                "target_position_on_true": 10,
                "target_position_on_false": 20,
                "timeout": 2,
            },
        )
        self.assertTrue(result["actual"])
        self.assertEqual(result["jump_to_position"], 10)

    def test_branch_on_state_step_false_branch(self) -> None:
        self.device.register_selector(
            FakeSelector(info={"selected": False}),
            resourceId="com.ss.android.ugc.trill:id/n17",
        )
        result = self._execute(
            "branch_on_state",
            {
                "resource_id": "com.ss.android.ugc.trill:id/n17",
                "state_name": "selected",
                "target_position_on_true": 10,
                "target_position_on_false": 20,
                "timeout": 2,
            },
        )
        self.assertFalse(result["actual"])
        self.assertEqual(result["jump_to_position"], 20)

    def test_branch_on_exists_step_exists_branch(self) -> None:
        self.device.register_selector(
            FakeSelector(exists_states=[True]),
            resourceId="com.ss.android.ugc.trill:id/n1f",
        )
        result = self._execute(
            "branch_on_exists",
            {
                "resource_id": "com.ss.android.ugc.trill:id/n1f",
                "target_position_on_exists": 2,
                "target_position_on_missing": 5,
                "timeout": 1,
            },
        )
        self.assertTrue(result["exists"])
        self.assertEqual(result["jump_to_position"], 2)

    def test_branch_on_exists_step_missing_branch(self) -> None:
        self.device.register_selector(
            FakeSelector(exists_states=[False]),
            resourceId="com.ss.android.ugc.trill:id/n1f",
        )
        result = self._execute(
            "branch_on_exists",
            {
                "resource_id": "com.ss.android.ugc.trill:id/n1f",
                "target_position_on_exists": 2,
                "target_position_on_missing": 5,
                "timeout": 0,
            },
        )
        self.assertFalse(result["exists"])
        self.assertEqual(result["jump_to_position"], 5)

    def test_set_variable_step(self) -> None:
        runtime = self._runtime("set_variable")
        result = self.executor.execute_step(
            runtime["step"],
            {"variable_name": "next_index", "value_mode": "expression", "value": "1 + 1"},
            runtime,
        )
        self.assertEqual(result["stored_value"], 2)
        self.assertEqual(self.executor.context["vars"]["next_index"], 2)

    def test_extract_text_step(self) -> None:
        self.device.register_selector(
            FakeSelector(text="OTP-1234", info={"text": "OTP-1234"}),
            resourceId="com.example:id/otp",
        )
        result = self._execute(
            "extract_text",
            {"resource_id": "com.example:id/otp", "variable_name": "otp", "source": "text", "timeout": 2},
        )
        self.assertEqual(result["extracted_value"], "OTP-1234")
        self.assertEqual(self.executor.context["vars"]["otp"], "OTP-1234")

    def test_chance_gate_step(self) -> None:
        runtime = self._runtime("chance_gate", position=5)
        with mock.patch("automation_studio.automation.engine.random.uniform", return_value=90.0):
            result = self.executor.execute_step(
                runtime["step"],
                {"probability_percent": 10, "skip_count_on_fail": 2},
                runtime,
            )
        self.assertFalse(result["passed"])
        self.assertEqual(result["jump_to_position"], 8)

    def test_loop_until_elapsed_step(self) -> None:
        runtime = self._runtime("loop_until_elapsed", position=9)
        with mock.patch("automation_studio.automation.engine.time.monotonic", side_effect=[0.0, 30.0, 60.0, 61.0]):
            first = self.executor.execute_step(
                runtime["step"],
                {"duration_minutes": 1, "target_position": 3},
                runtime,
            )
            second = self.executor.execute_step(
                runtime["step"],
                {"duration_minutes": 1, "target_position": 3},
                runtime,
            )
        self.assertEqual(first["jump_to_position"], 3)
        self.assertIsNone(second["jump_to_position"])

    def test_conditional_jump_step(self) -> None:
        runtime = self._runtime("conditional_jump", position=7)
        self.executor.context["vars"]["loop_index"] = 2
        result = self.executor.execute_step(
            runtime["step"],
            {"expression": "int(vars.get('loop_index', 0)) >= 2", "target_position": 4},
            runtime,
        )
        self.assertTrue(result["did_jump"])
        self.assertEqual(result["jump_to_position"], 4)

    def test_plugin_step(self) -> None:
        self.executor.context["vars"]["user"] = "Alice"
        runtime = self._runtime("plugin:echo_context")
        parameters = {"message": "Hello Alice", "write_variable": "plugin_message"}
        result = self.executor.execute_step(runtime["step"], parameters, runtime)
        self.assertEqual(result["plugin"], "echo_context")
        self.assertEqual(self.executor.context["vars"]["plugin_message"], "Hello Alice")


if __name__ == "__main__":
    unittest.main()
