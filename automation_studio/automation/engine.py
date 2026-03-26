from __future__ import annotations

import json
import random
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from automation_studio.automation.plugins import plugin_handler_for_step_type
from automation_studio.models import default_execution_policy, default_flow_control, migrate_step_parameters


TEMPLATE_PATTERN = re.compile(r"\$\{([^}]+)\}")


class WorkflowExecutor:
    POLICY_KEYS = set(default_execution_policy().keys())
    FLOW_KEYS = set(default_flow_control().keys())

    def __init__(
        self,
        device: Any,
        workflow: dict[str, Any],
        device_record: dict[str, Any],
        log_service: Any,
        telemetry_service: Any | None = None,
    ) -> None:
        self.device = device
        self.workflow = workflow
        self.device_record = device_record
        self.log_service = log_service
        self.telemetry_service = telemetry_service
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_artifact_dir = (
            Path("artifacts")
            / "runs"
            / f"workflow_{self.workflow['id']}_device_{self.device_record['id']}_{self.run_id}"
        )
        self.run_artifact_dir.mkdir(parents=True, exist_ok=True)
        self.context: dict[str, Any] = {
            "vars": {},
            "workflow": {"id": workflow["id"], "name": workflow["name"], "description": workflow.get("description", "")},
            "device": dict(device_record),
            "run": {
                "id": self.run_id,
                "artifact_dir": str(self.run_artifact_dir),
                "started_at": datetime.now().isoformat(timespec="seconds"),
            },
        }

    def run(self, steps: list[dict[str, Any]]) -> dict[str, Any]:
        ordered_steps = sorted(steps, key=lambda item: (int(item["position"]), int(item["id"])))
        position_to_index = {int(step["position"]): index for index, step in enumerate(ordered_steps)}
        summary = {
            "executed_steps": 0,
            "continued_failures": 0,
            "skipped_failures": 0,
            "skipped_conditions": 0,
            "jump_count": 0,
            "run_id": self.run_id,
            "artifact_dir": str(self.run_artifact_dir),
        }

        index = 0
        transitions = 0
        max_transitions = max(1000, len(ordered_steps) * 200)

        while 0 <= index < len(ordered_steps):
            transitions += 1
            if transitions > max_transitions:
                raise RuntimeError(
                    f"Workflow exceeded safety limit of {max_transitions} transitions. "
                    "Check conditional jumps or loop logic."
                )

            step = ordered_steps[index]
            runtime = {"step": step, "repeat_iteration": 1, "repeat_times": 1}
            if not step["is_enabled"]:
                self._log("INFO", "step_skipped", f"Skip disabled step: {step['name']}", step, {})
                self._record_telemetry(step, "skipped", 0, "")
                index += 1
                continue

            parameters = self._parse_parameters(step["parameters"])
            parameters = migrate_step_parameters(
                step["step_type"],
                parameters,
                int(step.get("schema_version", 1) or 1),
            )
            step_parameters, policy, flow = self._split_step_parameters(parameters)

            run_if_expression = str(flow.get("run_if_expression", "") or "").strip()
            if run_if_expression and not self._truthy(
                self._evaluate_expression(run_if_expression, step=step, parameters=step_parameters, runtime=runtime)
            ):
                self._log(
                    "INFO",
                    "step_condition_skipped",
                    f"Condition skipped step: {step['name']}",
                    step,
                    {"run_if_expression": run_if_expression},
                )
                self._record_telemetry(step, "skipped", 0, "")
                summary["skipped_conditions"] += 1
                index += 1
                continue

            repeat_times = int(flow.get("repeat_times", 1) or 1)
            result: dict[str, Any] | None = None
            for repeat_iteration in range(1, repeat_times + 1):
                runtime = {
                    "step": step,
                    "repeat_iteration": repeat_iteration,
                    "repeat_times": repeat_times,
                }
                resolved_step_parameters = self._resolve_step_parameters(step["step_type"], step_parameters, step, runtime)
                result = self._execute_step_with_policy(step, resolved_step_parameters, policy, runtime)
                if result["status"] == "success":
                    summary["executed_steps"] += 1
                    result_variable = str(flow.get("result_variable", "") or "").strip()
                    if result_variable:
                        self.context["vars"][result_variable] = result.get("result_metadata", {})
                    if repeat_iteration < repeat_times:
                        delay_seconds = float(flow.get("repeat_delay_seconds", 0) or 0)
                        if delay_seconds > 0:
                            time.sleep(delay_seconds)
                    continue

                if result["status"] == "continued_failure":
                    summary["continued_failures"] += 1
                elif result["status"] == "skipped_failure":
                    summary["skipped_failures"] += 1
                break

            next_index = index + 1
            jump_to_position = result.get("jump_to_position") if result else None
            if jump_to_position is not None:
                if int(jump_to_position) not in position_to_index:
                    raise RuntimeError(f"Conditional jump target {jump_to_position} does not exist")
                next_index = position_to_index[int(jump_to_position)]
                summary["jump_count"] += 1

            index = next_index

        summary["context_variables"] = len(self.context["vars"])
        summary["transition_count"] = transitions
        return summary

    def execute_step(
        self,
        step: dict[str, Any],
        parameters: dict[str, Any],
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        handlers = {
            "launch_app": self._launch_app,
            "stop_app": self._stop_app,
            "tap": self._tap,
            "click": self._click,
            "long_click": self._long_click,
            "double_click": self._double_click,
            "set_text": self._set_text,
            "wait": self._wait,
            "random_wait": self._random_wait,
            "wait_for_text": self._wait_for_text,
            "wait_for_element": self._wait_for_element,
            "swipe": self._swipe,
            "scroll": self._scroll,
            "press_key": self._press_key,
            "input_keycode": self._input_keycode,
            "shell": self._shell,
            "screenshot": self._screenshot,
            "dump_hierarchy": self._dump_hierarchy,
            "assert_exists": self._assert_exists,
            "assert_text": self._assert_text,
            "set_variable": self._set_variable,
            "extract_text": self._extract_text,
            "chance_gate": self._chance_gate,
            "loop_until_elapsed": self._loop_until_elapsed,
            "conditional_jump": self._conditional_jump,
        }
        handler = handlers.get(step["step_type"])
        if not handler:
            plugin_handler = plugin_handler_for_step_type(step["step_type"])
            if plugin_handler:
                return self._run_plugin_handler(plugin_handler, parameters)
            raise ValueError(f"Unsupported step type: {step['step_type']}")
        result = handler(parameters, runtime)
        return result if isinstance(result, dict) else {}

    def _execute_step_with_policy(
        self,
        step: dict[str, Any],
        parameters: dict[str, Any],
        policy: dict[str, Any],
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        max_attempts = int(policy["retry_count"]) + 1
        step_artifact_dir = self._step_artifact_dir(step)
        step_artifact_dir.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, max_attempts + 1):
            started = time.perf_counter()
            self._log(
                "INFO",
                "step_started",
                f"Running step {step['position']}: {step['name']} (attempt {attempt}/{max_attempts})",
                step,
                {
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "policy": policy,
                    "repeat_iteration": runtime["repeat_iteration"],
                    "repeat_times": runtime["repeat_times"],
                },
            )

            try:
                result_metadata = self._run_with_timeout(step, parameters, runtime, float(policy["step_timeout_seconds"]))
                duration_ms = int((time.perf_counter() - started) * 1000)
                metadata = {
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "duration_ms": duration_ms,
                    "policy": policy,
                    "repeat_iteration": runtime["repeat_iteration"],
                    "repeat_times": runtime["repeat_times"],
                }
                metadata.update(result_metadata)
                self._log("INFO", "step_success", f"Step completed: {step['name']}", step, metadata)
                self._record_telemetry(step, "success", duration_ms, "")
                return {
                    "status": "success",
                    "duration_ms": duration_ms,
                    "result_metadata": result_metadata,
                    "jump_to_position": result_metadata.get("jump_to_position"),
                }
            except Exception as exc:
                duration_ms = int((time.perf_counter() - started) * 1000)
                error_message = str(exc)
                base_metadata = {
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "duration_ms": duration_ms,
                    "error": error_message,
                    "policy": policy,
                    "repeat_iteration": runtime["repeat_iteration"],
                    "repeat_times": runtime["repeat_times"],
                }

                if attempt < max_attempts:
                    retry_delay = float(policy["retry_delay_seconds"])
                    retry_metadata = dict(base_metadata)
                    retry_metadata["retry_delay_seconds"] = retry_delay
                    self._log(
                        "WARNING",
                        "step_retry",
                        f"Step failed, retrying: {step['name']} ({attempt}/{max_attempts})",
                        step,
                        retry_metadata,
                    )
                    if retry_delay > 0:
                        time.sleep(retry_delay)
                    continue

                failure_artifacts = self._capture_failure_artifacts(step, step_artifact_dir, policy)
                failure_metadata = dict(base_metadata)
                failure_metadata.update(failure_artifacts)

                if policy["on_failure"] == "skip":
                    self._log(
                        "WARNING",
                        "step_skipped_failure",
                        f"Step failed and was skipped: {step['name']}",
                        step,
                        failure_metadata,
                    )
                    self._record_telemetry(step, "skipped_failure", duration_ms, error_message)
                    return {"status": "skipped_failure", "duration_ms": duration_ms, "result_metadata": failure_metadata}

                if bool(policy["continue_on_error"]):
                    self._log(
                        "ERROR",
                        "step_failed_continued",
                        f"Step failed but workflow continued: {step['name']}",
                        step,
                        failure_metadata,
                    )
                    self._record_telemetry(step, "continued_failure", duration_ms, error_message)
                    return {"status": "continued_failure", "duration_ms": duration_ms, "result_metadata": failure_metadata}

                self._log(
                    "ERROR",
                    "step_failed",
                    f"Step failed: {step['name']}",
                    step,
                    failure_metadata,
                )
                self._record_telemetry(step, "failure", duration_ms, error_message)
                raise RuntimeError(f"Step '{step['name']}' failed: {error_message}") from exc

        return {"status": "success", "duration_ms": 0, "result_metadata": {}}

    def _run_with_timeout(
        self,
        step: dict[str, Any],
        parameters: dict[str, Any],
        runtime: dict[str, Any],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        if timeout_seconds <= 0:
            return self.execute_step(step, parameters, runtime)

        result_holder: dict[str, Any] = {}
        error_holder: dict[str, Exception] = {}

        def runner() -> None:
            try:
                result_holder["value"] = self.execute_step(step, parameters, runtime)
            except Exception as exc:
                error_holder["error"] = exc

        thread = threading.Thread(target=runner, daemon=True)
        thread.start()
        thread.join(timeout_seconds)
        if thread.is_alive():
            raise TimeoutError(f"Step timed out after {timeout_seconds:.2f} seconds")
        if "error" in error_holder:
            raise error_holder["error"]
        return result_holder.get("value", {})

    def _split_step_parameters(
        self,
        parameters: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        policy = dict(default_execution_policy())
        flow = dict(default_flow_control())
        for key in self.POLICY_KEYS:
            if key in parameters:
                policy[key] = parameters[key]
        for key in self.FLOW_KEYS:
            if key in parameters:
                flow[key] = parameters[key]
        step_parameters = {
            key: value for key, value in parameters.items() if key not in self.POLICY_KEYS and key not in self.FLOW_KEYS
        }
        return step_parameters, policy, flow

    def _capture_failure_artifacts(
        self,
        step: dict[str, Any],
        step_artifact_dir: Path,
        policy: dict[str, Any],
    ) -> dict[str, Any]:
        metadata: dict[str, Any] = {}

        if policy.get("on_failure") == "take_screenshot":
            screenshot_path = step_artifact_dir / f"failure_{step['id']}.png"
            try:
                self.device.screenshot(str(screenshot_path))
                metadata["failure_screenshot"] = str(screenshot_path)
            except Exception as exc:
                metadata["failure_screenshot_error"] = str(exc)

        if bool(policy.get("capture_hierarchy_on_failure")):
            hierarchy_path = step_artifact_dir / f"failure_{step['id']}.xml"
            try:
                xml = self.device.dump_hierarchy()
                hierarchy_path.write_text(xml, encoding="utf-8")
                metadata["failure_hierarchy"] = str(hierarchy_path)
            except Exception as exc:
                metadata["failure_hierarchy_error"] = str(exc)

        return metadata

    def _step_artifact_dir(self, step: dict[str, Any]) -> Path:
        safe_name = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in step["name"])
        return self.run_artifact_dir / f"{int(step['position']):03d}_{safe_name}"

    def _parse_parameters(self, raw: str | dict[str, Any]) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if not raw:
            return {}
        return json.loads(raw)

    def _selector(self, parameters: dict[str, Any]) -> tuple[str | None, Any, float]:
        selector_timeout = float(parameters.get("timeout", 10))
        xpath = parameters.get("xpath")
        if xpath:
            return "xpath", self.device.xpath(xpath), selector_timeout

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
            return None, None, selector_timeout
        return "selector", self.device(**selector_kwargs), selector_timeout

    def _wait_on_target(self, target_type: str | None, target: Any, timeout: float) -> bool:
        if not target:
            return False
        if target_type == "xpath":
            return bool(target.wait(timeout=timeout))
        return bool(target.exists(timeout=timeout))

    def _target_exists_now(self, target_type: str | None, target: Any) -> bool:
        if not target:
            return False
        if target_type == "xpath":
            exists = getattr(target, "exists", None)
            if callable(exists):
                return bool(exists())
            if exists is not None:
                return bool(exists)
            try:
                return bool(target.wait(timeout=0))
            except Exception:
                return False

        exists = getattr(target, "exists", None)
        if callable(exists):
            try:
                return bool(exists(timeout=0))
            except TypeError:
                return bool(exists())
        return bool(exists)

    def _wait_for_target_state(
        self,
        target_type: str | None,
        target: Any,
        desired_state: str,
        timeout: float,
        poll_interval_seconds: float,
    ) -> bool:
        if desired_state == "exists":
            return self._wait_on_target(target_type, target, timeout)

        deadline = time.time() + timeout
        while time.time() <= deadline:
            if not self._target_exists_now(target_type, target):
                return True
            time.sleep(poll_interval_seconds)
        return False

    def _extract_target_info(self, target_type: str | None, target: Any) -> dict[str, Any]:
        if not target:
            return {}
        info = {}

        if target_type == "xpath" and hasattr(target, "get"):
            try:
                node = target.get()
            except Exception:
                node = None
            if node is not None:
                node_info = getattr(node, "info", None)
                if isinstance(node_info, dict):
                    info.update(node_info)
                if hasattr(node, "attrib") and isinstance(node.attrib, dict):
                    info.update(node.attrib)

        target_info = getattr(target, "info", None)
        if isinstance(target_info, dict):
            info.update(target_info)
        return info

    def _target_center(self, target_type: str | None, target: Any) -> tuple[int, int] | None:
        info = self._extract_target_info(target_type, target)
        bounds = info.get("bounds")
        if isinstance(bounds, dict):
            left = bounds.get("left")
            top = bounds.get("top")
            right = bounds.get("right")
            bottom = bounds.get("bottom")
            if None not in {left, top, right, bottom}:
                return int((left + right) / 2), int((top + bottom) / 2)
        return None

    def _resolve_step_parameters(
        self,
        step_type: str,
        parameters: dict[str, Any],
        step: dict[str, Any],
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        if step_type == "set_variable":
            resolved = dict(parameters)
            if str(parameters.get("value_mode", "literal") or "literal") == "template":
                resolved["value"] = self._resolve_templates(parameters.get("value"), step=step, parameters=parameters, runtime=runtime)
            return resolved
        if step_type == "conditional_jump":
            return dict(parameters)
        return self._resolve_templates(parameters, step=step, parameters=parameters, runtime=runtime)

    def _resolve_templates(
        self,
        value: Any,
        step: dict[str, Any],
        parameters: dict[str, Any],
        runtime: dict[str, Any],
    ) -> Any:
        if isinstance(value, dict):
            return {
                key: self._resolve_templates(item, step=step, parameters=parameters, runtime=runtime)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._resolve_templates(item, step=step, parameters=parameters, runtime=runtime) for item in value]
        if not isinstance(value, str):
            return value

        full_match = TEMPLATE_PATTERN.fullmatch(value)
        if full_match:
            return self._evaluate_expression(full_match.group(1), step=step, parameters=parameters, runtime=runtime)

        def replacer(match: re.Match[str]) -> str:
            resolved = self._evaluate_expression(match.group(1), step=step, parameters=parameters, runtime=runtime)
            return "" if resolved is None else str(resolved)

        return TEMPLATE_PATTERN.sub(replacer, value)

    def _safe_eval_context(
        self,
        step: dict[str, Any],
        parameters: dict[str, Any],
        runtime: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        builtins_map = {
            "int": int,
            "float": float,
            "str": str,
            "bool": bool,
            "len": len,
            "min": min,
            "max": max,
            "sum": sum,
            "round": round,
            "sorted": sorted,
            "any": any,
            "all": all,
        }
        scope = {
            "vars": self.context["vars"],
            "workflow": self.context["workflow"],
            "device": self.context["device"],
            "run": self.context["run"],
            "step": step,
            "parameters": parameters,
            "repeat_iteration": runtime["repeat_iteration"],
            "repeat_times": runtime["repeat_times"],
            "json": json,
        }
        return {"__builtins__": {}}, {**builtins_map, **scope}

    def _evaluate_expression(
        self,
        expression: str,
        step: dict[str, Any],
        parameters: dict[str, Any],
        runtime: dict[str, Any],
    ) -> Any:
        globals_map, locals_map = self._safe_eval_context(step, parameters, runtime)
        try:
            return eval(expression, globals_map, locals_map)
        except Exception as exc:
            raise RuntimeError(f"Expression error '{expression}': {exc}") from exc

    def _truthy(self, value: Any) -> bool:
        if isinstance(value, str):
            return value.strip().lower() not in {"", "0", "false", "no", "off", "none"}
        return bool(value)

    def _run_plugin_handler(self, plugin_handler, parameters: dict[str, Any]) -> dict[str, Any]:
        result = plugin_handler(self.device, parameters, self.context)
        return result if isinstance(result, dict) else {"plugin_result": result}

    def _record_telemetry(
        self,
        step: dict[str, Any],
        outcome: str,
        duration_ms: int,
        error_message: str,
    ) -> None:
        if not self.telemetry_service:
            return
        self.telemetry_service.record_step_result(
            workflow_id=self.workflow["id"],
            device_id=self.device_record["id"],
            step_type=step["step_type"],
            outcome=outcome,
            duration_ms=duration_ms,
            error_message=error_message,
        )

    def _launch_app(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        self.device.app_start(parameters["package"])
        return {"package": parameters["package"]}

    def _stop_app(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        self.device.app_stop(parameters["package"])
        return {"package": parameters["package"]}

    def _tap(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        x = int(parameters["x"])
        y = int(parameters["y"])
        self.device.click(x, y)
        return {"x": x, "y": y}

    def _click(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if target:
            if not self._wait_on_target(target_type, target, timeout):
                raise RuntimeError("Element not found for click step")
            target.click()
            return {"selector_type": target_type}
        if "x" in parameters and "y" in parameters:
            x = int(parameters["x"])
            y = int(parameters["y"])
            self.device.click(x, y)
            return {"x": x, "y": y}
        raise RuntimeError("Click step requires selector or x/y")

    def _long_click(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        duration = float(parameters.get("duration", 0.8))
        target_type, target, timeout = self._selector(parameters)
        if target:
            if not self._wait_on_target(target_type, target, timeout):
                raise RuntimeError("Element not found for long_click step")
            center = self._target_center(target_type, target)
            if center:
                self.device.long_click(center[0], center[1], duration)
                return {"selector_type": target_type, "x": center[0], "y": center[1], "duration": duration}
            if hasattr(target, "long_click"):
                try:
                    target.long_click(duration)
                except TypeError:
                    target.long_click()
                return {"selector_type": target_type, "duration": duration}
            raise RuntimeError("Unable to resolve target center for long click")
        if "x" in parameters and "y" in parameters:
            x = int(parameters["x"])
            y = int(parameters["y"])
            self.device.long_click(x, y, duration)
            return {"x": x, "y": y, "duration": duration}
        raise RuntimeError("long_click requires selector or x/y")

    def _double_click(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        interval_seconds = float(parameters.get("interval_seconds", 0.15))
        target_type, target, timeout = self._selector(parameters)
        if target:
            if not self._wait_on_target(target_type, target, timeout):
                raise RuntimeError("Element not found for double_click step")
            center = self._target_center(target_type, target)
            if center:
                self.device.click(center[0], center[1])
                time.sleep(interval_seconds)
                self.device.click(center[0], center[1])
                return {"selector_type": target_type, "x": center[0], "y": center[1], "interval_seconds": interval_seconds}
            target.click()
            time.sleep(interval_seconds)
            target.click()
            return {"selector_type": target_type, "interval_seconds": interval_seconds}
        if "x" in parameters and "y" in parameters:
            x = int(parameters["x"])
            y = int(parameters["y"])
            self.device.click(x, y)
            time.sleep(interval_seconds)
            self.device.click(x, y)
            return {"x": x, "y": y, "interval_seconds": interval_seconds}
        raise RuntimeError("double_click requires selector or x/y")

    def _set_text(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        text = str(parameters.get("text", ""))
        clear_first = bool(parameters.get("clear_first", True))
        target_type, target, timeout = self._selector(parameters)
        if target:
            if not self._wait_on_target(target_type, target, timeout):
                raise RuntimeError("Element not found for set_text step")
            if clear_first and target_type != "xpath":
                target.clear_text()
            target.set_text(text)
            return {"selector_type": target_type, "text_length": len(text)}
        self.device.send_keys(text, clear=clear_first)
        return {"text_length": len(text), "sent_to_focused_input": True}

    def _wait(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        seconds = float(parameters.get("seconds", 1))
        time.sleep(seconds)
        return {"seconds": seconds}

    def _random_wait(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        min_seconds = float(parameters.get("min_seconds", 0))
        max_seconds = float(parameters.get("max_seconds", min_seconds))
        actual_seconds = random.uniform(min_seconds, max_seconds)
        time.sleep(actual_seconds)
        return {
            "min_seconds": min_seconds,
            "max_seconds": max_seconds,
            "actual_seconds": round(actual_seconds, 3),
        }

    def _wait_for_text(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("wait_for_text requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Target not found within timeout")
        return {"selector_type": target_type}

    def _wait_for_element(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("wait_for_element requires text, resource_id, xpath or description")
        desired_state = str(parameters.get("desired_state", "exists") or "exists")
        poll_interval_seconds = float(parameters.get("poll_interval_seconds", 0.5))
        if not self._wait_for_target_state(target_type, target, desired_state, timeout, poll_interval_seconds):
            raise RuntimeError(f"Target did not reach state '{desired_state}' within timeout")
        return {"selector_type": target_type, "desired_state": desired_state}

    def _swipe(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        return self._run_swipe(parameters)

    def _scroll(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        metadata = self._run_swipe(parameters)
        metadata["mode"] = "scroll"
        return metadata

    def _run_swipe(self, parameters: dict[str, Any]) -> dict[str, Any]:
        x1, y1, x2, y2 = self._resolve_swipe_points(parameters)
        duration = float(parameters.get("duration", 0.2))
        repeat = max(1, int(parameters.get("repeat", 1)))
        pause_seconds = float(parameters.get("pause_seconds", 0))

        for index in range(repeat):
            self.device.swipe(x1, y1, x2, y2, duration)
            if pause_seconds > 0 and index < repeat - 1:
                time.sleep(pause_seconds)
        return {"path": {"x1": x1, "y1": y1, "x2": x2, "y2": y2}, "repeat": repeat}

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

    def _resolve_directional_swipe(self, direction: str, parameters: dict[str, Any]) -> tuple[int, int, int, int]:
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
        raise RuntimeError(f"Swipe step requires {axis_name} or ratio keys for that axis")

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

    def _press_key(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        key = str(parameters["key"])
        self.device.press(key)
        return {"key": key}

    def _input_keycode(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        keycode = int(parameters["keycode"])
        prefix = "--longpress " if bool(parameters.get("long_press")) else ""
        response = self.device.shell(f"input keyevent {prefix}{keycode}")
        if isinstance(response, tuple):
            output = response[0] if len(response) > 0 else ""
            exit_code = response[1] if len(response) > 1 else 0
        else:
            output = getattr(response, "output", str(response))
            exit_code = getattr(response, "exit_code", 0)
        if exit_code not in (0, None):
            raise RuntimeError(f"Keycode command failed: {output}")
        return {"keycode": keycode, "long_press": bool(parameters.get("long_press")), "shell_output": output}

    def _shell(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        response = self.device.shell(str(parameters["command"]))
        if isinstance(response, tuple):
            output = response[0] if len(response) > 0 else ""
            exit_code = response[1] if len(response) > 1 else 0
        else:
            output = getattr(response, "output", str(response))
            exit_code = getattr(response, "exit_code", 0)
        if exit_code not in (0, None):
            raise RuntimeError(f"Shell command failed: {output}")
        return {"shell_output": output, "exit_code": exit_code}

    def _screenshot(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        directory = Path(parameters.get("directory", "artifacts/screenshots"))
        filename = parameters.get("filename") or f"screen_{int(time.time())}.png"
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / filename
        self.device.screenshot(str(file_path))
        return {"artifact_path": str(file_path)}

    def _dump_hierarchy(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        directory = Path(parameters.get("directory", "artifacts/hierarchy"))
        filename = parameters.get("filename") or f"view_{int(time.time())}.xml"
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / filename
        xml = self.device.dump_hierarchy()
        file_path.write_text(xml, encoding="utf-8")
        return {"artifact_path": str(file_path)}

    def _assert_exists(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("assert_exists requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Expected element does not exist")
        return {"selector_type": target_type}

    def _assert_text(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("assert_text requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Target not found within timeout")
        actual_text = self._read_target_value(target_type, target, "text")
        expected_text = str(parameters.get("expected_text", ""))
        match_mode = str(parameters.get("match_mode", "contains") or "contains")
        if not self._text_matches(actual_text, expected_text, match_mode):
            raise RuntimeError(f"Text assertion failed. Expected {match_mode} '{expected_text}' but got '{actual_text}'")
        return {"selector_type": target_type, "actual_text": actual_text, "match_mode": match_mode}

    def _set_variable(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        variable_name = str(parameters["variable_name"]).strip()
        value_mode = str(parameters.get("value_mode", "literal") or "literal")
        raw_value = parameters.get("value", "")

        if value_mode == "literal":
            stored_value = str(raw_value)
        elif value_mode == "template":
            stored_value = raw_value
        elif value_mode == "expression":
            stored_value = self._evaluate_expression(str(raw_value), step=runtime["step"], parameters=parameters, runtime=runtime)
        elif value_mode == "json":
            stored_value = json.loads(str(raw_value or "null"))
        else:
            raise RuntimeError(f"Unsupported value mode: {value_mode}")

        self.context["vars"][variable_name] = stored_value
        return {"variable_name": variable_name, "stored_value": stored_value, "value_mode": value_mode}

    def _extract_text(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("extract_text requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Target not found within timeout")
        variable_name = str(parameters["variable_name"]).strip()
        source = str(parameters.get("source", "text") or "text")
        extracted_value = self._read_target_value(target_type, target, source)
        self.context["vars"][variable_name] = extracted_value
        return {"variable_name": variable_name, "source": source, "extracted_value": extracted_value}

    def _chance_gate(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        probability_percent = float(parameters.get("probability_percent", 0))
        roll_percent = random.uniform(0, 100)
        passed = roll_percent <= probability_percent
        jump_to_position = None

        if passed:
            target_position_on_pass = int(parameters.get("target_position_on_pass", 0) or 0)
            if target_position_on_pass > 0:
                jump_to_position = target_position_on_pass
        else:
            target_position_on_fail = int(parameters.get("target_position_on_fail", 0) or 0)
            if target_position_on_fail > 0:
                jump_to_position = target_position_on_fail
            else:
                skip_count_on_fail = int(parameters.get("skip_count_on_fail", 0) or 0)
                if skip_count_on_fail > 0:
                    jump_to_position = int(runtime["step"]["position"]) + skip_count_on_fail + 1

        return {
            "probability_percent": probability_percent,
            "roll_percent": round(roll_percent, 3),
            "passed": passed,
            "jump_to_position": jump_to_position,
        }

    def _loop_until_elapsed(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        duration_minutes = float(parameters.get("duration_minutes", 0))
        duration_seconds = duration_minutes * 60.0
        target_position = int(parameters["target_position"])
        runtime_state = self.context.setdefault("_runtime", {})
        state_key = f"loop_until_elapsed:{runtime['step']['id']}"
        started_at = float(runtime_state.get(state_key, time.monotonic()))
        runtime_state.setdefault(state_key, started_at)
        elapsed_seconds = time.monotonic() - started_at

        jump_to_position = target_position if elapsed_seconds < duration_seconds else None
        if jump_to_position is None:
            runtime_state.pop(state_key, None)

        return {
            "duration_minutes": duration_minutes,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "remaining_seconds": round(max(duration_seconds - elapsed_seconds, 0), 3),
            "jump_to_position": jump_to_position,
        }

    def _conditional_jump(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        expression = str(parameters["expression"])
        target_position = int(parameters["target_position"])
        should_jump = self._truthy(
            self._evaluate_expression(expression, step=runtime["step"], parameters=parameters, runtime=runtime)
        )
        return {
            "expression": expression,
            "jump_to_position": target_position if should_jump else None,
            "did_jump": should_jump,
        }

    def _read_target_value(self, target_type: str | None, target: Any, source: str) -> Any:
        if source == "info_json":
            return self._extract_target_info(target_type, target)

        if source == "text":
            for attribute in ("get_text", "text"):
                member = getattr(target, attribute, None)
                if callable(member):
                    try:
                        return member()
                    except TypeError:
                        continue
                if member:
                    return member

        info = self._extract_target_info(target_type, target)
        if source == "content_desc":
            return info.get("contentDescription") or info.get("content_desc") or info.get("description") or ""
        if source == "resource_id":
            return info.get("resourceName") or info.get("resource_id") or ""
        if source == "class_name":
            return info.get("className") or info.get("class_name") or ""
        if source == "text":
            return info.get("text") or ""
        raise RuntimeError(f"Unsupported extract source: {source}")

    def _text_matches(self, actual_text: Any, expected_text: str, match_mode: str) -> bool:
        actual = "" if actual_text is None else str(actual_text)
        if match_mode == "exact":
            return actual == expected_text
        if match_mode == "contains":
            return expected_text in actual
        if match_mode == "starts_with":
            return actual.startswith(expected_text)
        if match_mode == "ends_with":
            return actual.endswith(expected_text)
        return False

    def _log(
        self,
        level: str,
        status: str,
        message: str,
        step: dict[str, Any],
        metadata: dict[str, Any],
    ) -> None:
        base_metadata = {
            "step_id": step["id"],
            "step_name": step["name"],
            "step_type": step["step_type"],
            "position": step["position"],
            "run_id": self.run_id,
            "artifact_dir": str(self._step_artifact_dir(step)),
            "context_var_count": len(self.context["vars"]),
        }
        base_metadata.update(metadata)
        self.log_service.add(
            self.workflow["id"],
            self.device_record["id"],
            level,
            status,
            message,
            base_metadata,
        )
