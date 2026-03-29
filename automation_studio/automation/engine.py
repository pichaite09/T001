from __future__ import annotations

import json
import os
import random
import re
import shlex
import subprocess
import threading
import time
import shutil
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from datetime import datetime
from pathlib import Path, PurePosixPath
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
        watchers: list[dict[str, Any]] | None = None,
        watcher_telemetry_service: Any | None = None,
        switch_account_handler: Any | None = None,
        run_for_each_account_handler: Any | None = None,
        prepare_upload_context_handler: Any | None = None,
        shared_context: dict[str, Any] | None = None,
        external_stop_checker: Any | None = None,
    ) -> None:
        self.device = device
        self.workflow = workflow
        self.device_record = device_record
        self.log_service = log_service
        self.telemetry_service = telemetry_service
        self.watchers = sorted(watchers or [], key=lambda item: (int(item.get("priority", 100)), int(item.get("id", 0))))
        self.watcher_telemetry_service = watcher_telemetry_service
        self.switch_account_handler = switch_account_handler
        self.run_for_each_account_handler = run_for_each_account_handler
        self.prepare_upload_context_handler = prepare_upload_context_handler
        self.external_stop_checker = external_stop_checker
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_artifact_dir = (
            Path("artifacts")
            / "runs"
            / f"workflow_{self.workflow['id']}_device_{self.device_record['id']}_{self.run_id}"
        )
        self.run_artifact_dir.mkdir(parents=True, exist_ok=True)
        shared_vars = shared_context.get("vars") if shared_context else None
        self.context: dict[str, Any] = {
            "vars": shared_vars if isinstance(shared_vars, dict) else {},
            "workflow": {"id": workflow["id"], "name": workflow["name"], "description": workflow.get("description", "")},
            "device": dict(device_record),
            "run": {
                "id": self.run_id,
                "artifact_dir": str(self.run_artifact_dir),
                "started_at": datetime.now().isoformat(timespec="seconds"),
            },
        }
        if shared_context:
            if "platform" in shared_context:
                self.context["platform"] = shared_context["platform"]
            if "account" in shared_context:
                self.context["account"] = shared_context["account"]
            if "upload" in shared_context:
                self.context["upload"] = shared_context["upload"]
        self._watcher_runtime: dict[int, dict[str, Any]] = {}
        self._watcher_action_depth = 0
        self._stop_requested = False
        self._stop_reason = ""
        self._watcher_total_triggers = 0
        self._watcher_trigger_limit = max(50, len(self.watchers) * 20) if self.watchers else 0
        self._watcher_chain_limit = 10

    def request_stop(self, reason: str = "Workflow stopped by user") -> None:
        self._stop_requested = True
        self._stop_reason = str(reason or "Workflow stopped by user")

    def _poll_external_stop_request(self) -> None:
        if self._stop_requested or not callable(self.external_stop_checker):
            return
        try:
            payload = self.external_stop_checker()
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        if bool(payload.get("stop_requested")) or bool(payload.get("cancel_requested")):
            self.request_stop(str(payload.get("control_reason") or "Workflow stopped externally"))

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
            self._poll_external_stop_request()
            transitions += 1
            if transitions > max_transitions:
                raise RuntimeError(
                    f"Workflow exceeded safety limit of {max_transitions} transitions. "
                    "Check conditional jumps or loop logic."
                )

            step = ordered_steps[index]
            runtime = {"step": step, "repeat_iteration": 1, "repeat_times": 1}
            self._poll_watchers("before_step", step, runtime)
            self._poll_external_stop_request()
            if self._stop_requested:
                break
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
                self._poll_watchers("after_step", step, runtime)
                if self._stop_requested:
                    break
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
                            self._sleep_with_watchers(delay_seconds, step, runtime)
                    continue

                if result["status"] == "continued_failure":
                    summary["continued_failures"] += 1
                elif result["status"] == "skipped_failure":
                    summary["skipped_failures"] += 1
                break

            self._poll_watchers("after_step", step, runtime)
            self._poll_external_stop_request()
            if self._stop_requested:
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
        summary["watcher_count"] = len(self.watchers)
        summary["watcher_trigger_count"] = self._watcher_total_triggers
        summary["watcher_trigger_limit"] = self._watcher_trigger_limit
        summary["stopped_by_watcher"] = self._stop_requested
        summary["stop_reason"] = self._stop_reason
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
            "scroll_to_selector": self._scroll_to_selector,
            "switch_account": self._switch_account,
            "run_for_each_account": self._run_for_each_account,
            "prepare_upload_context": self._prepare_upload_context,
            "download_video_asset": self._download_video_asset,
            "push_file_to_device": self._push_file_to_device,
            "delete_local_file": self._delete_local_file,
            "press_key": self._press_key,
            "input_keycode": self._input_keycode,
            "shell": self._shell,
            "screenshot": self._screenshot,
            "dump_hierarchy": self._dump_hierarchy,
            "assert_exists": self._assert_exists,
            "assert_text": self._assert_text,
            "assert_state": self._assert_state,
            "branch_on_state": self._branch_on_state,
            "branch_on_exists": self._branch_on_exists,
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
                        self._sleep_with_watchers(retry_delay, step, runtime)
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

    def _poll_watchers(self, stage: str, step: dict[str, Any] | None, runtime: dict[str, Any]) -> None:
        if not self.watchers or self._watcher_action_depth > 0 or self._stop_requested:
            return

        for watcher in self.watchers:
            watcher_id = int(watcher.get("id", 0) or 0)
            state = self._watcher_runtime.setdefault(
                watcher_id,
                {
                    "trigger_count": 0,
                    "last_triggered_at": 0.0,
                    "consecutive_matches": 0,
                },
            )
            policy = self._watcher_policy(watcher)
            active_stages = {str(item) for item in policy.get("active_stages", [])}
            if stage not in active_stages:
                continue

            max_triggers_per_run = int(policy.get("max_triggers_per_run", 0) or 0)
            if max_triggers_per_run and int(state.get("trigger_count", 0)) >= max_triggers_per_run:
                continue

            cooldown_seconds = float(policy.get("cooldown_seconds", 0) or 0)
            last_triggered_at = float(state.get("last_triggered_at", 0.0) or 0.0)
            if cooldown_seconds > 0 and (time.monotonic() - last_triggered_at) < cooldown_seconds:
                continue

            matched, condition_metadata = self._watcher_matches(watcher, stage, step, runtime, state)
            if not matched:
                state["consecutive_matches"] = 0
                continue

            state["consecutive_matches"] = int(state.get("consecutive_matches", 0)) + 1
            debounce_count = max(1, int(policy.get("debounce_count", 1) or 1))
            if int(state["consecutive_matches"]) < debounce_count:
                continue

            state["consecutive_matches"] = 0
            state["trigger_count"] = int(state.get("trigger_count", 0)) + 1
            state["last_triggered_at"] = time.monotonic()
            self._watcher_total_triggers += 1
            if self._watcher_trigger_limit and self._watcher_total_triggers > self._watcher_trigger_limit:
                self._stop_requested = True
                self._stop_reason = (
                    f"Watcher safety stop: exceeded {self._watcher_trigger_limit} watcher triggers in one run"
                )
                self._log_watcher(
                    "ERROR",
                    "watcher_safety_stop",
                    self._stop_reason,
                    watcher,
                    step,
                    {
                        "stage": stage,
                        "watcher_trigger_limit": self._watcher_trigger_limit,
                        "watcher_trigger_count": self._watcher_total_triggers,
                    },
                )
                break
            self._log_watcher(
                "INFO",
                "watcher_matched",
                f"Watcher matched: {watcher['name']}",
                watcher,
                step,
                {
                        "stage": stage,
                        "condition_type": watcher["condition_type"],
                        "trigger_count": state["trigger_count"],
                        "total_watcher_triggers": self._watcher_total_triggers,
                        "debounce_count": debounce_count,
                        **condition_metadata,
                    },
            )

            try:
                action_metadata = self._execute_watcher_action(watcher, step, runtime, condition_metadata)
                self._record_watcher_telemetry(watcher_id, "success", "")
                self._log_watcher(
                    "INFO",
                    "watcher_action_success",
                    f"Watcher action completed: {watcher['name']}",
                    watcher,
                    step,
                    {
                        "stage": stage,
                        "action_type": watcher["action_type"],
                        **action_metadata,
                    },
                )
            except Exception as exc:
                error_message = str(exc)
                self._record_watcher_telemetry(watcher_id, "failure", error_message)
                self._log_watcher(
                    "ERROR",
                    "watcher_action_failed",
                    f"Watcher action failed: {watcher['name']}",
                    watcher,
                    step,
                    {
                        "stage": stage,
                        "action_type": watcher["action_type"],
                        "error": error_message,
                    },
                )
                continue

            match_mode = str(policy.get("match_mode", "first_match") or "first_match")
            if bool(policy.get("stop_after_match")) or match_mode == "first_match":
                break

    def _watcher_policy(self, watcher: dict[str, Any]) -> dict[str, Any]:
        policy = {
            "cooldown_seconds": 3.0,
            "debounce_count": 1,
            "max_triggers_per_run": 0,
            "stop_after_match": False,
            "match_mode": "first_match",
            "active_stages": ["before_step", "after_step", "during_wait"],
        }
        policy.update(self._parse_parameters(watcher.get("policy_json") or "{}"))
        return policy

    def _watcher_matches(
        self,
        watcher: dict[str, Any],
        stage: str,
        step: dict[str, Any] | None,
        runtime: dict[str, Any],
        state: dict[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        condition = self._parse_parameters(watcher.get("condition_json") or "{}")
        condition_type = str(watcher.get("condition_type") or "")

        if condition_type in {"selector_exists", "text_exists", "selector_gone", "text_contains"}:
            target_type, target, timeout = self._selector(condition)
            if not target:
                return False, {}
            if timeout > 0 and condition_type in {"selector_exists", "text_exists", "text_contains"}:
                exists = self._wait_on_target(target_type, target, timeout)
            else:
                exists = self._target_exists_now(target_type, target)

            if condition_type == "selector_gone":
                return (not exists), {"selector_type": target_type}

            if not exists:
                return False, {}

            if condition_type == "text_contains":
                expected_text = str(condition.get("expected_text", "") or "")
                actual_text = str(self._read_target_value(target_type, target, "text") or "")
                return expected_text in actual_text, {
                    "selector_type": target_type,
                    "expected_text": expected_text,
                    "actual_text": actual_text,
                }

            return True, {"selector_type": target_type}

        if condition_type == "app_in_foreground":
            package = str(condition.get("package", "") or "")
            current = self.device.app_current() if hasattr(self.device, "app_current") else {}
            current_package = str(current.get("package") or current.get("packageName") or "")
            return current_package == package, {"current_package": current_package}

        if condition_type == "package_changed":
            target_package = str(condition.get("package", "") or "")
            current = self.device.app_current() if hasattr(self.device, "app_current") else {}
            current_package = str(current.get("package") or current.get("packageName") or "")
            previous_package = state.get("last_package")
            state["last_package"] = current_package
            if previous_package is None:
                return False, {"current_package": current_package}
            changed = current_package != previous_package
            if target_package:
                changed = changed and current_package == target_package
            return changed, {"previous_package": previous_package, "current_package": current_package}

        if condition_type == "elapsed_time":
            seconds = float(condition.get("seconds", 0) or 0)
            started_at_text = str(self.context["run"]["started_at"])
            started_at = datetime.fromisoformat(started_at_text)
            elapsed = max((datetime.now() - started_at).total_seconds(), 0.0)
            return elapsed >= seconds, {"elapsed_seconds": round(elapsed, 3)}

        if condition_type == "variable_changed":
            variable_name = str(condition.get("variable_name", "") or "")
            current_value = self.context["vars"].get(variable_name)
            previous_value = state.get("last_variable_value")
            state["last_variable_value"] = current_value
            if previous_value is None:
                return False, {"variable_name": variable_name, "current_value": current_value}
            return previous_value != current_value, {
                "variable_name": variable_name,
                "previous_value": previous_value,
                "current_value": current_value,
            }

        if condition_type == "expression":
            expression = str(condition.get("expression", "") or "")
            matched = self._truthy(self._evaluate_expression(expression, step=step, parameters=condition, runtime=runtime))
            return matched, {"expression": expression}

        return False, {}

    def _execute_watcher_action(
        self,
        watcher: dict[str, Any],
        step: dict[str, Any] | None,
        runtime: dict[str, Any],
        condition_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        action = self._parse_parameters(watcher.get("action_json") or "{}")
        action_type = str(watcher.get("action_type") or "")

        if action_type == "run_step":
            action_step_type = str(action.get("step_type", "") or "").strip()
            action_parameters = action.get("parameters", {})
            if not isinstance(action_parameters, dict):
                raise RuntimeError("Watcher run_step parameters must be an object")
            pseudo_step = {
                "id": f"watcher_{watcher['id']}",
                "name": f"[Watcher] {watcher['name']}",
                "step_type": action_step_type,
                "position": 0,
            }
            pseudo_runtime = {"step": pseudo_step, "repeat_iteration": 1, "repeat_times": 1}
            resolved_parameters = self._resolve_step_parameters(action_step_type, action_parameters, pseudo_step, pseudo_runtime)
            self._watcher_action_depth += 1
            try:
                result = self.execute_step(pseudo_step, resolved_parameters, pseudo_runtime)
            finally:
                self._watcher_action_depth -= 1
            return {"action_step_type": action_step_type, "result": result, **condition_metadata}

        if action_type == "action_chain":
            actions = action.get("actions", [])
            if isinstance(actions, str):
                actions = json.loads(actions)
            if not isinstance(actions, list):
                raise RuntimeError("Watcher action_chain actions must be a list")
            if len(actions) > self._watcher_chain_limit:
                raise RuntimeError(
                    f"Watcher action_chain exceeds safety limit of {self._watcher_chain_limit} actions"
                )
            chain_results: list[dict[str, Any]] = []
            for index, item in enumerate(actions, start=1):
                if not isinstance(item, dict):
                    raise RuntimeError(f"Watcher action_chain entry #{index} must be an object")
                nested_watcher = dict(watcher)
                nested_watcher["action_type"] = item.get("action_type")
                nested_watcher["action_json"] = json.dumps(item.get("action", {}), ensure_ascii=False)
                chain_results.append(self._execute_watcher_action(nested_watcher, step, runtime, condition_metadata))
            return {"chain_length": len(chain_results), "chain_results": chain_results, **condition_metadata}

        if action_type == "press_back":
            self.device.press("back")
            return {"pressed_key": "back", **condition_metadata}

        if action_type == "take_screenshot":
            prefix = str(action.get("filename_prefix") or "watcher_event")
            directory = self.run_artifact_dir / "watchers"
            directory.mkdir(parents=True, exist_ok=True)
            file_path = directory / f"{prefix}_{int(time.time())}.png"
            self.device.screenshot(str(file_path))
            return {"artifact_path": str(file_path), **condition_metadata}

        if action_type == "dump_hierarchy":
            prefix = str(action.get("filename_prefix") or "watcher_view")
            directory = self.run_artifact_dir / "watchers"
            directory.mkdir(parents=True, exist_ok=True)
            file_path = directory / f"{prefix}_{int(time.time())}.xml"
            xml = self.device.dump_hierarchy()
            file_path.write_text(xml, encoding="utf-8")
            return {"artifact_path": str(file_path), **condition_metadata}

        if action_type == "set_variable":
            variable_name = str(action.get("variable_name") or "").strip()
            self.context["vars"][variable_name] = action.get("value")
            return {"variable_name": variable_name, "stored_value": action.get("value"), **condition_metadata}

        if action_type == "stop_workflow":
            self._stop_requested = True
            self._stop_reason = str(action.get("reason") or f"Watcher '{watcher['name']}' stopped the workflow")
            return {"stop_reason": self._stop_reason, **condition_metadata}

        raise RuntimeError(f"Unsupported watcher action: {action_type}")

    def _record_watcher_telemetry(self, watcher_id: int, outcome: str, error_message: str) -> None:
        if not self.watcher_telemetry_service:
            return
        self.watcher_telemetry_service.record_watcher_result(
            watcher_id=watcher_id,
            workflow_id=self.workflow["id"],
            device_id=self.device_record["id"],
            outcome=outcome,
            error_message=error_message,
        )

    def _log_watcher(
        self,
        level: str,
        status: str,
        message: str,
        watcher: dict[str, Any],
        step: dict[str, Any] | None,
        metadata: dict[str, Any],
    ) -> None:
        payload = {
            "watcher_id": watcher["id"],
            "watcher_name": watcher["name"],
            "scope_type": watcher["scope_type"],
            "scope_id": watcher.get("scope_id"),
            "run_id": self.run_id,
        }
        payload.update(self._context_log_metadata())
        if step:
            payload.update(
                {
                    "step_id": step.get("id"),
                    "step_name": step.get("name"),
                    "step_type": step.get("step_type"),
                    "position": step.get("position"),
                }
            )
        payload.update(metadata)
        self.log_service.add(
            self.workflow["id"],
            self.device_record["id"],
            level,
            status,
            message,
            payload,
        )

    def _sleep_with_watchers(
        self,
        seconds: float,
        step: dict[str, Any],
        runtime: dict[str, Any],
    ) -> None:
        remaining = max(float(seconds), 0.0)
        if remaining <= 0:
            return
        has_external_stop = callable(self.external_stop_checker)
        if not self.watchers or self._watcher_action_depth > 0:
            if not has_external_stop:
                time.sleep(remaining)
                return
            interval = 0.2
            while remaining > 0:
                chunk = min(interval, remaining)
                time.sleep(chunk)
                remaining -= chunk
                self._poll_external_stop_request()
                if self._stop_requested:
                    break
            return
        interval = 0.2
        while remaining > 0:
            chunk = min(interval, remaining)
            time.sleep(chunk)
            remaining -= chunk
            self._poll_external_stop_request()
            self._poll_watchers("during_wait", step, runtime)
            if self._stop_requested:
                break

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
        step: dict[str, Any] | None = None,
        runtime: dict[str, Any] | None = None,
    ) -> bool:
        if desired_state == "exists":
            return self._wait_on_target(target_type, target, timeout)

        deadline = time.time() + timeout
        while time.time() <= deadline:
            if not self._target_exists_now(target_type, target):
                return True
            if step and runtime:
                self._sleep_with_watchers(poll_interval_seconds, step, runtime)
                if self._stop_requested:
                    return False
            else:
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
            "platform": self.context.get("platform", {}),
            "account": self.context.get("account", {}),
            "upload": self.context.get("upload", {}),
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
        self._sleep_with_watchers(seconds, runtime["step"], runtime)
        return {"seconds": seconds}

    def _random_wait(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        min_seconds = float(parameters.get("min_seconds", 0))
        max_seconds = float(parameters.get("max_seconds", min_seconds))
        actual_seconds = random.uniform(min_seconds, max_seconds)
        self._sleep_with_watchers(actual_seconds, runtime["step"], runtime)
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
        if not self._wait_for_target_state(
            target_type,
            target,
            desired_state,
            timeout,
            poll_interval_seconds,
            runtime["step"],
            runtime,
        ):
            raise RuntimeError(f"Target did not reach state '{desired_state}' within timeout")
        return {"selector_type": target_type, "desired_state": desired_state}

    def _swipe(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        return self._run_swipe(parameters)

    def _scroll(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        metadata = self._run_swipe(parameters)
        metadata["mode"] = "scroll"
        return metadata

    def _scroll_to_selector(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, selector_timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("scroll_to_selector requires text, resource_id, xpath or description")

        max_swipes = max(1, int(parameters.get("max_swipes", 1) or 1))
        timeout = max(float(parameters.get("timeout", selector_timeout) or selector_timeout or 0), 0.0)
        swipe_parameters = dict(parameters)
        swipe_parameters["repeat"] = 1

        for swipe_index in range(max_swipes + 1):
            if timeout > 0:
                found = self._wait_on_target(target_type, target, timeout)
            else:
                found = self._target_exists_now(target_type, target)
            if found:
                return {
                    "selector_type": target_type,
                    "found": True,
                    "swipes_used": swipe_index,
                    "max_swipes": max_swipes,
                }

            if swipe_index >= max_swipes:
                break

            self._run_swipe(swipe_parameters)
            pause_seconds = float(parameters.get("pause_seconds", 0) or 0)
            if pause_seconds > 0:
                self._sleep_with_watchers(pause_seconds, runtime["step"], runtime)
                if self._stop_requested:
                    break

        raise RuntimeError(f"Target not found after {max_swipes} swipe(s)")

    def _switch_account(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        if not self.switch_account_handler:
            raise RuntimeError("switch_account is not configured for this runtime")
        return self.switch_account_handler(self, parameters, runtime)

    def _run_for_each_account(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        if not self.run_for_each_account_handler:
            raise RuntimeError("run_for_each_account is not configured for this runtime")
        return self.run_for_each_account_handler(self, parameters, runtime)

    def _prepare_upload_context(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        if not self.prepare_upload_context_handler:
            raise RuntimeError("prepare_upload_context is not configured for this runtime")
        return self.prepare_upload_context_handler(self, parameters, runtime)

    def _download_video_asset(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        source = str(
            parameters.get("video_url")
            or self.context.get("upload", {}).get("video_url")
            or self.context.get("vars", {}).get("upload_video_url")
            or ""
        ).strip()
        if not source:
            raise RuntimeError("download_video_asset requires video_url or upload.video_url")

        directory = Path(parameters.get("directory", self.run_artifact_dir / "downloads"))
        directory.mkdir(parents=True, exist_ok=True)
        filename = str(parameters.get("filename") or "").strip()
        if not filename:
            filename = self._filename_from_source(source)
        target_path = directory / filename

        parsed = urlparse(source)
        if parsed.scheme in {"http", "https"}:
            self._download_http_asset(source, target_path, parameters)
        else:
            source_path = Path(parsed.path if parsed.scheme == "file" else source)
            if not source_path.exists():
                raise RuntimeError(f"Video source not found: {source}")
            if source_path.resolve() != target_path.resolve():
                shutil.copy2(source_path, target_path)

        self.context.setdefault("upload", {})
        self.context["upload"]["local_video_path"] = str(target_path)
        self.context["vars"]["upload_local_video_path"] = str(target_path)
        return {"artifact_path": str(target_path), "local_video_path": str(target_path)}

    def _download_http_asset(self, source: str, target_path: Path, parameters: dict[str, Any]) -> None:
        headers = {
            "User-Agent": str(
                parameters.get("user_agent")
                or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ).strip(),
            "Accept": str(parameters.get("accept") or "*/*").strip(),
        }
        referer = str(parameters.get("referer") or "").strip()
        if referer:
            headers["Referer"] = referer
        cookies = str(parameters.get("cookies") or "").strip()
        if cookies:
            headers["Cookie"] = cookies

        extra_headers = parameters.get("headers_json")
        if extra_headers:
            try:
                parsed_headers = json.loads(str(extra_headers))
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid headers_json: {exc}") from exc
            if not isinstance(parsed_headers, dict):
                raise RuntimeError("headers_json must be a JSON object")
            for key, value in parsed_headers.items():
                key_text = str(key).strip()
                if key_text:
                    headers[key_text] = str(value)

        timeout_seconds = float(parameters.get("timeout_seconds", 60) or 60)
        request = Request(source, headers=headers)
        try:
            with urlopen(request, timeout=timeout_seconds) as response, target_path.open("wb") as target_file:
                shutil.copyfileobj(response, target_file)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to download video asset from URL: {exc}. "
                "If the source is protected, set Referer/Cookies/Headers JSON or use a direct downloadable URL."
            ) from exc

    def _push_file_to_device(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        local_path = str(
            parameters.get("local_path")
            or self.context.get("upload", {}).get("local_video_path")
            or self.context.get("vars", {}).get("upload_local_video_path")
            or ""
        ).strip()
        if not local_path:
            raise RuntimeError("push_file_to_device requires local_path or upload.local_video_path")

        device_path = str(
            parameters.get("device_path")
            or self.context.get("upload", {}).get("device_video_path")
            or self.context.get("vars", {}).get("upload_device_video_path")
            or ""
        ).strip()
        if not device_path:
            raise RuntimeError("push_file_to_device requires device_path")

        source_path = Path(local_path)
        if not source_path.exists():
            raise RuntimeError(f"Local file not found: {local_path}")

        serial = str(self.device_record.get("serial") or "").strip()
        if not serial:
            raise RuntimeError("Device serial is required for adb push")

        adb_path = self._find_adb_executable()
        create_parent = bool(parameters.get("create_parent", True))
        remote_parent = str(PurePosixPath(device_path).parent)
        if create_parent and remote_parent not in {"", ".", "/"}:
            mkdir_result = subprocess.run(
                [adb_path, "-s", serial, "shell", "mkdir", "-p", remote_parent],
                capture_output=True,
                text=True,
            )
            if mkdir_result.returncode != 0:
                message = (mkdir_result.stderr or mkdir_result.stdout or "").strip() or "Unknown adb mkdir error"
                raise RuntimeError(f"Failed to prepare device directory: {message}")

        push_result = subprocess.run(
            [adb_path, "-s", serial, "push", str(source_path), device_path],
            capture_output=True,
            text=True,
        )
        if push_result.returncode != 0:
            message = (push_result.stderr or push_result.stdout or "").strip() or "Unknown adb push error"
            raise RuntimeError(f"Failed to push file to device: {message}")

        verify_result = subprocess.run(
            [adb_path, "-s", serial, "shell", f"ls {shlex.quote(device_path)}"],
            capture_output=True,
            text=True,
        )
        if verify_result.returncode != 0:
            message = (verify_result.stderr or verify_result.stdout or "").strip() or "Unknown adb verify error"
            raise RuntimeError(f"File push completed but device file was not found: {message}")

        media_uri = f"file://{device_path}"
        media_scan_result = subprocess.run(
            [
                adb_path,
                "-s",
                serial,
                "shell",
                "am",
                "broadcast",
                "-a",
                "android.intent.action.MEDIA_SCANNER_SCAN_FILE",
                "-d",
                media_uri,
            ],
            capture_output=True,
            text=True,
        )

        self.context.setdefault("upload", {})
        self.context["upload"]["device_video_path"] = device_path
        self.context["vars"]["upload_device_video_path"] = device_path
        output_parts = [
            (push_result.stdout or push_result.stderr or "").strip(),
            (verify_result.stdout or verify_result.stderr or "").strip(),
            (media_scan_result.stdout or media_scan_result.stderr or "").strip(),
        ]
        output = "\n".join(part for part in output_parts if part)
        return {
            "local_video_path": str(source_path),
            "device_video_path": device_path,
            "adb_output": output,
        }

    def _delete_local_file(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        local_path = str(
            parameters.get("local_path")
            or self.context.get("upload", {}).get("local_video_path")
            or self.context.get("vars", {}).get("upload_local_video_path")
            or ""
        ).strip()
        if not local_path:
            raise RuntimeError("delete_local_file requires local_path or upload.local_video_path")

        target_path = Path(local_path)
        missing_ok = bool(parameters.get("missing_ok", True))
        if not target_path.exists():
            if missing_ok:
                if bool(parameters.get("clear_upload_local_video_path", True)):
                    self.context.setdefault("upload", {})
                    self.context["upload"]["local_video_path"] = ""
                    self.context["vars"]["upload_local_video_path"] = ""
                return {"local_video_path": str(target_path), "deleted": False, "missing": True}
            raise RuntimeError(f"Local file not found: {local_path}")

        target_path.unlink()
        if bool(parameters.get("clear_upload_local_video_path", True)):
            self.context.setdefault("upload", {})
            self.context["upload"]["local_video_path"] = ""
            self.context["vars"]["upload_local_video_path"] = ""
        return {"local_video_path": str(target_path), "deleted": True, "missing": False}

    def _find_adb_executable(self) -> str:
        direct = shutil.which("adb")
        if direct:
            return direct

        sdk_roots = [
            os.environ.get("ANDROID_SDK_ROOT", ""),
            os.environ.get("ANDROID_HOME", ""),
            str(Path.home() / "AppData" / "Local" / "Android" / "Sdk"),
        ]
        for sdk_root in sdk_roots:
            sdk_root = str(sdk_root or "").strip()
            if not sdk_root:
                continue
            for candidate in (Path(sdk_root) / "platform-tools" / "adb.exe", Path(sdk_root) / "platform-tools" / "adb"):
                if candidate.exists():
                    return str(candidate)

        raise RuntimeError("adb executable not found. Install Android platform-tools or add adb to PATH.")

    def _filename_from_source(self, source: str) -> str:
        parsed = urlparse(source)
        candidate = Path(parsed.path if parsed.scheme else source).name
        candidate = candidate or "video.mp4"
        if "." not in candidate:
            candidate += ".mp4"
        return candidate

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

    def _assert_state(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("assert_state requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Target not found within timeout")
        state_name = str(parameters.get("state_name", "selected") or "selected").strip()
        expected = bool(parameters.get("expected", True))
        actual = self._read_target_state(target_type, target, state_name)
        if actual is None:
            raise RuntimeError(f"State '{state_name}' is not available on the target")
        if actual is not expected:
            raise RuntimeError(f"State assertion failed. Expected {state_name}={expected} but got {actual}")
        return {"selector_type": target_type, "state_name": state_name, "expected": expected, "actual": actual}

    def _branch_on_state(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("branch_on_state requires text, resource_id, xpath or description")
        if not self._wait_on_target(target_type, target, timeout):
            raise RuntimeError("Target not found within timeout")
        state_name = str(parameters.get("state_name", "selected") or "selected").strip()
        actual = self._read_target_state(target_type, target, state_name)
        if actual is None:
            raise RuntimeError(f"State '{state_name}' is not available on the target")
        target_position_on_true = int(parameters["target_position_on_true"])
        target_position_on_false = int(parameters["target_position_on_false"])
        jump_to_position = target_position_on_true if actual else target_position_on_false
        return {
            "selector_type": target_type,
            "state_name": state_name,
            "actual": actual,
            "jump_to_position": jump_to_position,
            "target_position_on_true": target_position_on_true,
            "target_position_on_false": target_position_on_false,
        }

    def _branch_on_exists(self, parameters: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
        target_type, target, timeout = self._selector(parameters)
        if not target:
            raise RuntimeError("branch_on_exists requires text, resource_id, xpath or description")
        exists = self._wait_on_target(target_type, target, timeout) if timeout > 0 else self._target_exists_now(target_type, target)
        target_position_on_exists = int(parameters["target_position_on_exists"])
        target_position_on_missing = int(parameters["target_position_on_missing"])
        jump_to_position = target_position_on_exists if exists else target_position_on_missing
        return {
            "selector_type": target_type,
            "exists": exists,
            "jump_to_position": jump_to_position,
            "target_position_on_exists": target_position_on_exists,
            "target_position_on_missing": target_position_on_missing,
        }

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

    def _read_target_state(self, target_type: str | None, target: Any, state_name: str) -> bool | None:
        info = self._extract_target_info(target_type, target)
        candidate_keys = {
            "selected": ("selected",),
            "checked": ("checked",),
            "enabled": ("enabled",),
            "focused": ("focused",),
            "clickable": ("clickable",),
            "scrollable": ("scrollable",),
            "long_clickable": ("longClickable", "long_clickable"),
        }.get(state_name, (state_name,))

        for key in candidate_keys:
            if key in info:
                return self._coerce_bool_value(info.get(key))
        return None

    def _coerce_bool_value(self, value: Any) -> bool | None:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"true", "1", "yes", "on"}:
                return True
            if normalized in {"false", "0", "no", "off"}:
                return False
        return None

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
        base_metadata.update(self._context_log_metadata())
        base_metadata.update(metadata)
        self.log_service.add(
            self.workflow["id"],
            self.device_record["id"],
            level,
            status,
            message,
            base_metadata,
        )

    def _context_log_metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        platform = self.context.get("platform")
        if isinstance(platform, dict):
            metadata.update(
                {
                    "device_platform_id": platform.get("id"),
                    "platform_key": platform.get("key") or "",
                    "platform_name": platform.get("name") or "",
                }
            )
        account = self.context.get("account")
        if isinstance(account, dict):
            metadata.update(
                {
                    "account_id": account.get("id"),
                    "account_name": account.get("display_name") or "",
                    "account_username": account.get("username") or "",
                    "account_login_id": account.get("login_id") or "",
                }
            )
        return metadata
