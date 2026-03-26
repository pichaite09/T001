from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


class WorkflowExecutor:
    def __init__(
        self,
        device: Any,
        workflow: dict[str, Any],
        device_record: dict[str, Any],
        log_service: Any,
    ) -> None:
        self.device = device
        self.workflow = workflow
        self.device_record = device_record
        self.log_service = log_service

    def run(self, steps: list[dict[str, Any]]) -> int:
        executed = 0
        for step in steps:
            if not step["is_enabled"]:
                self._log("INFO", "skipped", f"Skip disabled step: {step['name']}", step)
                continue

            parameters = self._parse_parameters(step["parameters"])
            self._log("INFO", "running", f"Running step {step['position']}: {step['name']}", step)
            self.execute_step(step["step_type"], parameters)
            self._log("INFO", "success", f"Step completed: {step['name']}", step)
            executed += 1
        return executed

    def execute_step(self, step_type: str, parameters: dict[str, Any]) -> None:
        handlers = {
            "launch_app": self._launch_app,
            "stop_app": self._stop_app,
            "tap": self._tap,
            "click": self._click,
            "set_text": self._set_text,
            "wait": self._wait,
            "wait_for_text": self._wait_for_text,
            "swipe": self._swipe,
            "press_key": self._press_key,
            "shell": self._shell,
            "screenshot": self._screenshot,
            "dump_hierarchy": self._dump_hierarchy,
            "assert_exists": self._assert_exists,
        }
        handler = handlers.get(step_type)
        if not handler:
            raise ValueError(f"Unsupported step type: {step_type}")
        handler(parameters)

    def _parse_parameters(self, raw: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if not raw:
            return {}
        return json.loads(raw)

    def _selector(self, parameters: dict[str, Any]):
        timeout = float(parameters.get("timeout", 10))
        xpath = parameters.get("xpath")
        if xpath:
            return "xpath", self.device.xpath(xpath), timeout

        selector_kwargs = {}
        if parameters.get("text"):
            selector_kwargs["text"] = parameters["text"]
        if parameters.get("resource_id"):
            selector_kwargs["resourceId"] = parameters["resource_id"]
        if parameters.get("description"):
            selector_kwargs["description"] = parameters["description"]
        if parameters.get("class_name"):
            selector_kwargs["className"] = parameters["class_name"]
        if parameters.get("focused") is True:
            selector_kwargs["focused"] = True

        if not selector_kwargs:
            return None, None, timeout
        return "selector", self.device(**selector_kwargs), timeout

    def _wait_on_target(self, target_type: str | None, target: Any, timeout: float) -> bool:
        if not target:
            return False
        if target_type == "xpath":
            return bool(target.wait(timeout=timeout))
        return bool(target.exists(timeout=timeout))

    def _launch_app(self, parameters: dict[str, Any]) -> None:
        self.device.app_start(parameters["package"])

    def _stop_app(self, parameters: dict[str, Any]) -> None:
        self.device.app_stop(parameters["package"])

    def _tap(self, parameters: dict[str, Any]) -> None:
        self.device.click(int(parameters["x"]), int(parameters["y"]))

    def _click(self, parameters: dict[str, Any]) -> None:
        target_type, target, timeout = self._selector(parameters)
        if target:
            if not self._wait_on_target(target_type, target, timeout):
                raise RuntimeError("Element not found for click step")
            target.click()
            return
        if "x" in parameters and "y" in parameters:
            self.device.click(int(parameters["x"]), int(parameters["y"]))
            return
        raise RuntimeError("Click step requires selector or x/y")

    def _set_text(self, parameters: dict[str, Any]) -> None:
        text = str(parameters.get("text", ""))
        clear_first = bool(parameters.get("clear_first", True))
        target_type, target, timeout = self._selector(parameters)
        if target:
            if not self._wait_on_target(target_type, target, timeout):
                raise RuntimeError("Element not found for set_text step")
            if clear_first and target_type != "xpath":
                target.clear_text()
            target.set_text(text)
            return
        self.device.send_keys(text, clear=clear_first)

    def _wait(self, parameters: dict[str, Any]) -> None:
        time.sleep(float(parameters.get("seconds", 1)))

    def _wait_for_text(self, parameters: dict[str, Any]) -> None:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("wait_for_text requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Target not found within timeout")

    def _swipe(self, parameters: dict[str, Any]) -> None:
        x1, y1, x2, y2 = self._resolve_swipe_points(parameters)
        duration = float(parameters.get("duration", 0.2))
        repeat = max(1, int(parameters.get("repeat", 1)))
        pause_seconds = float(parameters.get("pause_seconds", 0))

        for index in range(repeat):
            self.device.swipe(x1, y1, x2, y2, duration)
            if pause_seconds > 0 and index < repeat - 1:
                time.sleep(pause_seconds)

    def _resolve_swipe_points(self, parameters: dict[str, Any]) -> tuple[int, int, int, int]:
        direction = str(parameters.get("direction", "")).strip().lower()
        if direction:
            return self._resolve_directional_swipe(direction, parameters)

        width, height = self._window_size()
        x1 = self._resolve_axis_value(parameters, ("x1", "start_x"), ("x1_ratio", "start_x_ratio"), width, "x1")
        y1 = self._resolve_axis_value(parameters, ("y1", "start_y"), ("y1_ratio", "start_y_ratio"), height, "y1")
        x2 = self._resolve_axis_value(parameters, ("x2", "end_x"), ("x2_ratio", "end_x_ratio"), width, "x2")
        y2 = self._resolve_axis_value(parameters, ("y2", "end_y"), ("y2_ratio", "end_y_ratio"), height, "y2")
        return x1, y1, x2, y2

    def _resolve_directional_swipe(
        self,
        direction: str,
        parameters: dict[str, Any],
    ) -> tuple[int, int, int, int]:
        width, height = self._window_size()
        margin_ratio = float(parameters.get("margin_ratio", 0.1))
        margin_ratio = min(max(margin_ratio, 0.0), 0.45)
        scale = min(max(float(parameters.get("scale", 0.6)), 0.05), 1.0)
        anchor_x = min(max(float(parameters.get("anchor_x", 0.5)), 0.0), 1.0)
        anchor_y = min(max(float(parameters.get("anchor_y", 0.5)), 0.0), 1.0)

        min_x = int(width * margin_ratio)
        max_x = int(width * (1 - margin_ratio))
        min_y = int(height * margin_ratio)
        max_y = int(height * (1 - margin_ratio))
        center_x = self._clamp(int(width * anchor_x), min_x, max_x)
        center_y = self._clamp(int(height * anchor_y), min_y, max_y)

        if direction in {"up", "down"}:
            travel = int((max_y - min_y) * scale)
            half = max(1, travel // 2)
            start_y = self._clamp(center_y + half, min_y, max_y)
            end_y = self._clamp(center_y - half, min_y, max_y)
            if direction == "down":
                start_y, end_y = end_y, start_y
            return center_x, start_y, center_x, end_y

        if direction in {"left", "right"}:
            travel = int((max_x - min_x) * scale)
            half = max(1, travel // 2)
            start_x = self._clamp(center_x + half, min_x, max_x)
            end_x = self._clamp(center_x - half, min_x, max_x)
            if direction == "right":
                start_x, end_x = end_x, start_x
            return start_x, center_y, end_x, center_y

        raise RuntimeError("Swipe direction must be one of: up, down, left, right")

    def _resolve_axis_value(
        self,
        parameters: dict[str, Any],
        absolute_keys: tuple[str, ...],
        ratio_keys: tuple[str, ...],
        size: int,
        axis_name: str,
    ) -> int:
        for key in absolute_keys:
            if key in parameters:
                return int(parameters[key])
        for key in ratio_keys:
            if key in parameters:
                return int(round(float(parameters[key]) * size))
        raise RuntimeError(
            f"Swipe step requires {axis_name} or ratio keys for that axis"
        )

    def _window_size(self) -> tuple[int, int]:
        if hasattr(self.device, "window_size"):
            size = self.device.window_size()
            if isinstance(size, (list, tuple)) and len(size) >= 2:
                return int(size[0]), int(size[1])

        info = getattr(self.device, "info", {}) or {}
        width = info.get("displayWidth") or info.get("width")
        height = info.get("displayHeight") or info.get("height")
        if width and height:
            return int(width), int(height)

        raise RuntimeError("Unable to determine device screen size for swipe step")

    def _clamp(self, value: int, lower: int, upper: int) -> int:
        return max(lower, min(value, upper))

    def _press_key(self, parameters: dict[str, Any]) -> None:
        self.device.press(str(parameters["key"]))

    def _shell(self, parameters: dict[str, Any]) -> None:
        response = self.device.shell(str(parameters["command"]))
        if isinstance(response, tuple):
            output = response[0] if len(response) > 0 else ""
            exit_code = response[1] if len(response) > 1 else 0
        else:
            output = getattr(response, "output", str(response))
            exit_code = getattr(response, "exit_code", 0)
        if exit_code not in (0, None):
            raise RuntimeError(f"Shell command failed: {output}")

    def _screenshot(self, parameters: dict[str, Any]) -> None:
        directory = Path(parameters.get("directory", "artifacts/screenshots"))
        filename = parameters.get("filename") or f"screen_{int(time.time())}.png"
        directory.mkdir(parents=True, exist_ok=True)
        self.device.screenshot(str(directory / filename))

    def _dump_hierarchy(self, parameters: dict[str, Any]) -> None:
        directory = Path(parameters.get("directory", "artifacts/hierarchy"))
        filename = parameters.get("filename") or f"view_{int(time.time())}.xml"
        directory.mkdir(parents=True, exist_ok=True)
        xml = self.device.dump_hierarchy()
        (directory / filename).write_text(xml, encoding="utf-8")

    def _assert_exists(self, parameters: dict[str, Any]) -> None:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("assert_exists requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Expected element does not exist")

    def _log(self, level: str, status: str, message: str, step: dict[str, Any]) -> None:
        self.log_service.add(
            self.workflow["id"],
            self.device_record["id"],
            level,
            status,
            message,
            {
                "step_id": step["id"],
                "step_name": step["name"],
                "step_type": step["step_type"],
                "position": step["position"],
            },
        )
