from __future__ import annotations

import importlib
import json
from datetime import datetime
from typing import Any

from automation_studio.automation.engine import WorkflowExecutor
from automation_studio.models import (
    STEP_SCHEMA_VERSION,
    default_watcher_policy,
    WORKFLOW_DEFINITION_VERSION,
    migrate_step_parameters,
    validate_step_parameters,
    validate_watcher_config,
    validate_workflow_structure,
)
from automation_studio.repositories import (
    DeviceRepository,
    LogRepository,
    TelemetryRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)


class DeviceService:
    def __init__(self, device_repository: DeviceRepository) -> None:
        self.device_repository = device_repository

    def list_devices(self) -> list[dict[str, Any]]:
        return self.device_repository.list_devices()

    def save_device(self, device_id: int | None, name: str, serial: str, notes: str) -> int:
        return self.device_repository.upsert_device(device_id, name, serial, notes)

    def delete_device(self, device_id: int) -> None:
        self.device_repository.delete_device(device_id)

    def _load_uiautomator2(self):
        try:
            return importlib.import_module("uiautomator2")
        except ImportError as exc:
            raise RuntimeError(
                "ยังไม่ได้ติดตั้ง uiautomator2 กรุณารัน pip install -r requirements.txt"
            ) from exc

    def connect_device(self, serial: str):
        uiautomator2 = self._load_uiautomator2()
        return uiautomator2.connect(serial)

    def test_connection(
        self,
        serial: str,
        device_id: int | None = None,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        try:
            device = self.connect_device(serial)
            info = device.info
            manufacturer = info.get("manufacturer") or info.get("brand") or "Unknown"
            model = info.get("model") or "Unknown"
            android_version = info.get("version") or "Unknown"
            message = f"Connected: {manufacturer} {model} / Android {android_version}"
            if device_id:
                self.device_repository.update_status(device_id, "connected")
            return True, message, info
        except Exception as exc:
            if device_id:
                self.device_repository.update_status(device_id, "failed")
            return False, f"Connection failed: {exc}", None


class LogService:
    def __init__(self, log_repository: LogRepository) -> None:
        self.log_repository = log_repository

    def add(
        self,
        workflow_id: int | None,
        device_id: int | None,
        level: str,
        status: str,
        message: str,
        metadata: dict[str, Any] | None = None,
        watcher_id: int | None = None,
    ) -> int:
        return self.log_repository.add_log(workflow_id, device_id, level, status, message, metadata, watcher_id)

    def list_logs(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
        watcher_id: int | None = None,
        status: str | None = None,
        limit: int = 300,
        ) -> list[dict[str, Any]]:
        return self.log_repository.list_logs(workflow_id, device_id, watcher_id, status, limit)


class TelemetryService:
    def __init__(self, telemetry_repository: TelemetryRepository) -> None:
        self.telemetry_repository = telemetry_repository

    def record_step_result(
        self,
        workflow_id: int | None,
        device_id: int | None,
        step_type: str,
        outcome: str,
        duration_ms: int,
        error_message: str = "",
    ) -> None:
        self.telemetry_repository.record_step_result(
            workflow_id=workflow_id,
            device_id=device_id,
            step_type=step_type,
            outcome=outcome,
            duration_ms=duration_ms,
            error_message=error_message,
        )

    def summary(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        return self.telemetry_repository.summary(workflow_id=workflow_id, device_id=device_id, limit=limit)


class WatcherTelemetryService:
    def __init__(self, watcher_telemetry_repository: WatcherTelemetryRepository) -> None:
        self.watcher_telemetry_repository = watcher_telemetry_repository

    def record_watcher_result(
        self,
        watcher_id: int,
        workflow_id: int | None,
        device_id: int | None,
        outcome: str,
        error_message: str = "",
    ) -> None:
        self.watcher_telemetry_repository.record_watcher_result(
            watcher_id=watcher_id,
            workflow_id=workflow_id,
            device_id=device_id,
            outcome=outcome,
            error_message=error_message,
        )

    def summary(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        return self.watcher_telemetry_repository.summary(
            workflow_id=workflow_id,
            device_id=device_id,
            limit=limit,
        )


class WatcherService:
    def __init__(
        self,
        watcher_repository: WatcherRepository,
        device_repository: DeviceRepository,
        device_service: DeviceService | None,
        log_service: LogService,
        watcher_telemetry_service: WatcherTelemetryService,
    ) -> None:
        self.watcher_repository = watcher_repository
        self.device_repository = device_repository
        self.device_service = device_service
        self.log_service = log_service
        self.watcher_telemetry_service = watcher_telemetry_service

    def list_watchers(self) -> list[dict[str, Any]]:
        return self.watcher_repository.list_watchers()

    def get_watcher(self, watcher_id: int) -> dict[str, Any] | None:
        return self.watcher_repository.get_watcher(watcher_id)

    def save_watcher(
        self,
        watcher_id: int | None,
        name: str,
        scope_type: str,
        scope_id: int | None,
        condition_type: str,
        condition_text: str,
        action_type: str,
        action_text: str,
        policy_text: str,
        is_enabled: bool = True,
        priority: int = 100,
    ) -> int:
        condition = json.loads(condition_text or "{}")
        action = json.loads(action_text or "{}")
        policy = default_watcher_policy()
        policy.update(json.loads(policy_text or "{}"))
        normalized_scope_id = None if scope_type == "global" else int(scope_id) if scope_id else None
        errors = validate_watcher_config(
            name=name,
            scope_type=scope_type,
            scope_id=normalized_scope_id,
            condition_type=condition_type,
            condition=condition,
            action_type=action_type,
            action=action,
            policy=policy,
        )
        if errors:
            raise ValueError("\n".join(errors))
        return self.watcher_repository.upsert_watcher(
            watcher_id=watcher_id,
            name=name.strip(),
            scope_type=scope_type,
            scope_id=normalized_scope_id,
            condition_type=condition_type,
            condition_json=json.dumps(condition, indent=2, ensure_ascii=False),
            action_type=action_type,
            action_json=json.dumps(action, indent=2, ensure_ascii=False),
            policy_json=json.dumps(policy, indent=2, ensure_ascii=False),
            is_enabled=is_enabled,
            priority=priority,
        )

    def delete_watcher(self, watcher_id: int) -> None:
        self.watcher_repository.delete_watcher(watcher_id)

    def resolve_active_watchers(self, workflow_id: int, device_id: int) -> list[dict[str, Any]]:
        resolved: list[dict[str, Any]] = []
        seen_ids: set[int] = set()
        for watcher in self.watcher_repository.resolve_active_watchers(workflow_id, device_id):
            watcher_id = int(watcher["id"])
            if watcher_id in seen_ids:
                continue
            seen_ids.add(watcher_id)
            resolved.append(watcher)
        for watcher in self.watcher_repository.resolve_profile_watchers(workflow_id):
            watcher_id = int(watcher["id"])
            if watcher_id in seen_ids:
                continue
            seen_ids.add(watcher_id)
            resolved.append(watcher)
        resolved.sort(key=lambda item: (int(item.get("priority", 100)), int(item["id"])))
        return resolved

    def list_watchers_for_workflow(self, workflow_id: int) -> list[dict[str, Any]]:
        linked: list[dict[str, Any]] = []
        for watcher in self.list_watchers():
            scope_type = str(watcher.get("scope_type") or "")
            scope_id = watcher.get("scope_id")
            if scope_type == "global" or (scope_type == "workflow" and int(scope_id or 0) == int(workflow_id)):
                item = dict(watcher)
                item["source"] = "direct"
                linked.append(item)
        for watcher in self.watcher_repository.resolve_profile_watchers(workflow_id):
            item = dict(watcher)
            item["source"] = "profile"
            linked.append(item)
        return linked

    def list_profiles(self) -> list[dict[str, Any]]:
        return self.watcher_repository.list_profiles()

    def get_profile(self, profile_id: int) -> dict[str, Any] | None:
        return self.watcher_repository.get_profile(profile_id)

    def save_profile(
        self,
        profile_id: int | None,
        name: str,
        description: str,
        watcher_ids: list[int],
        is_active: bool = True,
    ) -> int:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("Profile name is required")
        if not watcher_ids:
            raise ValueError("Select at least one watcher for the profile")
        duplicate_profile = next(
            (
                profile
                for profile in self.list_profiles()
                if str(profile.get("name") or "").strip().casefold() == normalized_name.casefold()
                and int(profile["id"]) != int(profile_id or 0)
            ),
            None,
        )
        if duplicate_profile:
            raise ValueError("Profile name already exists")
        available_ids = {int(watcher["id"]) for watcher in self.list_watchers()}
        invalid_ids = [watcher_id for watcher_id in watcher_ids if int(watcher_id) not in available_ids]
        if invalid_ids:
            raise ValueError(f"Unknown watcher ids in profile: {invalid_ids}")
        saved_profile_id = self.watcher_repository.upsert_profile(
            profile_id=profile_id,
            name=normalized_name,
            description=str(description or "").strip(),
            is_active=is_active,
        )
        self.watcher_repository.save_profile_watchers(saved_profile_id, [int(watcher_id) for watcher_id in watcher_ids])
        return saved_profile_id

    def delete_profile(self, profile_id: int) -> None:
        self.watcher_repository.delete_profile(profile_id)

    def list_profile_watchers(self, profile_id: int) -> list[dict[str, Any]]:
        return self.watcher_repository.list_profile_watchers(profile_id)

    def list_profiles_for_workflow(self, workflow_id: int) -> list[dict[str, Any]]:
        return self.watcher_repository.list_profiles_for_workflow(workflow_id)

    def save_workflow_profiles(self, workflow_id: int, profile_ids: list[int]) -> None:
        workflow_profile_ids = [int(profile_id) for profile_id in profile_ids]
        available_ids = {int(profile["id"]) for profile in self.list_profiles()}
        invalid_ids = [profile_id for profile_id in workflow_profile_ids if profile_id not in available_ids]
        if invalid_ids:
            raise ValueError(f"Unknown profile ids for workflow: {invalid_ids}")
        self.watcher_repository.save_workflow_profiles(workflow_id, workflow_profile_ids)

    def test_condition(
        self,
        device_id: int,
        condition_type: str,
        condition_text: str,
    ) -> tuple[bool, str, dict[str, Any]]:
        device_record = self.device_repository.get_device(device_id)
        if not device_record:
            raise ValueError("Device not found")
        if not self.device_service:
            raise RuntimeError("Device service is not available")
        device = self.device_service.connect_device(device_record["serial"])
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 0, "name": "Watcher Test"},
            device_record=device_record,
            log_service=self.log_service,
        )
        watcher = {
            "id": 0,
            "name": "Watcher Condition Test",
            "scope_type": "device",
            "scope_id": device_id,
            "condition_type": condition_type,
            "condition_json": condition_text,
            "action_type": "press_back",
            "action_json": "{}",
            "policy_json": json.dumps(default_watcher_policy(), ensure_ascii=False),
        }
        runtime = {
            "step": {"id": 0, "name": "Watcher Condition Test", "step_type": "watcher_test", "position": 0},
            "repeat_iteration": 1,
            "repeat_times": 1,
        }
        matched, metadata = executor._watcher_matches(
            watcher,
            "before_step",
            runtime["step"],
            runtime,
            {"trigger_count": 0, "last_triggered_at": 0.0, "consecutive_matches": 0},
        )
        message = "Condition matched on selected device" if matched else "Condition did not match on selected device"
        return matched, message, metadata

    def test_action(
        self,
        device_id: int,
        action_type: str,
        action_text: str,
    ) -> tuple[bool, str, dict[str, Any]]:
        device_record = self.device_repository.get_device(device_id)
        if not device_record:
            raise ValueError("Device not found")
        if not self.device_service:
            raise RuntimeError("Device service is not available")
        device = self.device_service.connect_device(device_record["serial"])
        executor = WorkflowExecutor(
            device=device,
            workflow={"id": 0, "name": "Watcher Test"},
            device_record=device_record,
            log_service=self.log_service,
        )
        watcher = {
            "id": 0,
            "name": "Watcher Action Test",
            "scope_type": "device",
            "scope_id": device_id,
            "condition_type": "expression",
            "condition_json": json.dumps({"expression": "True"}, ensure_ascii=False),
            "action_type": action_type,
            "action_json": action_text,
            "policy_json": json.dumps(default_watcher_policy(), ensure_ascii=False),
        }
        runtime = {
            "step": {"id": 0, "name": "Watcher Action Test", "step_type": "watcher_test", "position": 0},
            "repeat_iteration": 1,
            "repeat_times": 1,
        }
        metadata = executor._execute_watcher_action(watcher, runtime["step"], runtime, {})
        return True, "Action executed on selected device", metadata


class WorkflowService:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        device_repository: DeviceRepository,
        device_service: DeviceService,
        log_service: LogService,
        telemetry_service: TelemetryService,
        watcher_service: WatcherService,
        watcher_telemetry_service: WatcherTelemetryService,
    ) -> None:
        self.workflow_repository = workflow_repository
        self.device_repository = device_repository
        self.device_service = device_service
        self.log_service = log_service
        self.telemetry_service = telemetry_service
        self.watcher_service = watcher_service
        self.watcher_telemetry_service = watcher_telemetry_service

    def list_workflows(self) -> list[dict[str, Any]]:
        return self.workflow_repository.list_workflows()

    def get_workflow(self, workflow_id: int) -> dict[str, Any] | None:
        return self.workflow_repository.get_workflow(workflow_id)

    def save_workflow(
        self,
        workflow_id: int | None,
        name: str,
        description: str,
        is_active: bool = True,
    ) -> int:
        return self.workflow_repository.upsert_workflow(
            workflow_id,
            name,
            description,
            is_active,
            definition_version=WORKFLOW_DEFINITION_VERSION,
        )

    def delete_workflow(self, workflow_id: int) -> None:
        self.workflow_repository.delete_workflow(workflow_id)

    def list_steps(self, workflow_id: int) -> list[dict[str, Any]]:
        return self.workflow_repository.list_steps(workflow_id)

    def save_step(
        self,
        step_id: int | None,
        workflow_id: int,
        position: int,
        name: str,
        step_type: str,
        parameters_text: str,
        is_enabled: bool = True,
    ) -> int:
        parsed = json.loads(parameters_text or "{}")
        parsed = migrate_step_parameters(step_type, parsed, STEP_SCHEMA_VERSION)
        errors = validate_step_parameters(step_type, parsed)
        if errors:
            raise ValueError("\n".join(errors))
        normalized = json.dumps(parsed, indent=2, ensure_ascii=False)
        saved_step_id = self.workflow_repository.upsert_step(
            step_id,
            workflow_id,
            position,
            name,
            step_type,
            normalized,
            is_enabled,
            schema_version=STEP_SCHEMA_VERSION,
        )
        self._normalize_step_positions(workflow_id)
        return saved_step_id

    def delete_step(self, step_id: int) -> None:
        self.workflow_repository.delete_step(step_id)

    def reorder_steps(self, workflow_id: int, ordered_step_ids: list[int]) -> None:
        self.workflow_repository.reorder_steps(workflow_id, ordered_step_ids)

    def _normalize_step_positions(self, workflow_id: int) -> None:
        steps = self.workflow_repository.list_steps(workflow_id)
        ordered_ids = [step["id"] for step in steps]
        if ordered_ids:
            self.workflow_repository.reorder_steps(workflow_id, ordered_ids)

    def validate_workflow_steps(self, workflow_id: int) -> list[str]:
        steps = self.workflow_repository.list_steps(workflow_id)
        return self._validate_steps(steps, include_structure=True)

    def _validate_steps(
        self,
        steps: list[dict[str, Any]],
        include_structure: bool = True,
    ) -> list[str]:
        errors: list[str] = []
        for step in steps:
            if not step["is_enabled"]:
                continue
            try:
                parameters = json.loads(step["parameters"] or "{}")
                parameters = migrate_step_parameters(
                    step["step_type"],
                    parameters,
                    int(step.get("schema_version", 1) or 1),
                )
            except json.JSONDecodeError as exc:
                errors.append(f"Step {step['position']} '{step['name']}': invalid JSON ({exc})")
                continue
            step_errors = validate_step_parameters(step["step_type"], parameters)
            for error in step_errors:
                errors.append(f"Step {step['position']} '{step['name']}': {error}")
        if include_structure:
            errors.extend(validate_workflow_structure(steps))
        return errors

    def _resolve_execution_steps(
        self,
        workflow_id: int,
        step_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        steps = self.workflow_repository.list_steps(workflow_id)
        if not step_ids:
            return steps
        requested_ids = {int(step_id) for step_id in step_ids}
        return [step for step in steps if int(step["id"]) in requested_ids]

    def export_workflow_definition(self, workflow_id: int) -> dict[str, Any]:
        workflow = self.workflow_repository.get_workflow(workflow_id)
        if not workflow:
            raise ValueError("Workflow not found")
        steps = self.workflow_repository.list_steps(workflow_id)
        return {
            "schema_version": 1,
            "exported_at": datetime.now().isoformat(timespec="seconds"),
            "workflow": {
                "name": workflow["name"],
                "description": workflow.get("description", ""),
                "is_active": bool(workflow.get("is_active", 1)),
                "definition_version": int(workflow.get("definition_version", WORKFLOW_DEFINITION_VERSION)),
            },
            "steps": [
                {
                    "position": int(step["position"]),
                    "name": step["name"],
                    "step_type": step["step_type"],
                    "parameters": migrate_step_parameters(
                        step["step_type"],
                        json.loads(step["parameters"] or "{}"),
                        int(step.get("schema_version", 1) or 1),
                    ),
                    "is_enabled": bool(step["is_enabled"]),
                    "schema_version": int(step.get("schema_version", STEP_SCHEMA_VERSION)),
                }
                for step in steps
            ],
        }

    def import_workflow_definition(self, payload: dict[str, Any]) -> int:
        workflow_payload = payload.get("workflow")
        steps_payload = payload.get("steps", [])
        if not isinstance(workflow_payload, dict):
            raise ValueError("Import JSON must contain a workflow object")
        if not isinstance(steps_payload, list):
            raise ValueError("Import JSON must contain a steps list")

        workflow_id = self.save_workflow(
            None,
            str(workflow_payload.get("name") or "Imported Workflow"),
            str(workflow_payload.get("description") or ""),
            bool(workflow_payload.get("is_active", True)),
        )

        normalized_steps: list[dict[str, Any]] = []
        for index, step in enumerate(steps_payload, start=1):
            if not isinstance(step, dict):
                raise ValueError(f"Imported step #{index} must be an object")
            parameters = step.get("parameters", {})
            if not isinstance(parameters, dict):
                raise ValueError(f"Imported step #{index} parameters must be an object")
            step_type = str(step.get("step_type") or "").strip()
            name = str(step.get("name") or step_type or f"Imported Step {index}").strip()
            position = int(step.get("position") or index)
            schema_version = int(step.get("schema_version") or STEP_SCHEMA_VERSION)
            normalized_steps.append(
                {
                    "position": position,
                    "name": name,
                    "step_type": step_type,
                    "parameters": migrate_step_parameters(step_type, parameters, schema_version),
                    "is_enabled": bool(step.get("is_enabled", True)),
                }
            )

        normalized_steps.sort(key=lambda item: item["position"])
        try:
            for position, step in enumerate(normalized_steps, start=1):
                self.save_step(
                    None,
                    workflow_id,
                    position,
                    step["name"],
                    step["step_type"],
                    json.dumps(step["parameters"], ensure_ascii=False),
                    step["is_enabled"],
                )

            validation_errors = self.validate_workflow_steps(workflow_id)
            if validation_errors:
                raise ValueError("\n".join(validation_errors))
        except Exception:
            self.delete_workflow(workflow_id)
            raise

        return workflow_id

    def execute_workflow(self, workflow_id: int, device_id: int) -> dict[str, Any]:
        return self._execute_workflow_run(workflow_id, device_id, step_ids=None)

    def execute_step(self, workflow_id: int, step_id: int, device_id: int) -> dict[str, Any]:
        return self._execute_workflow_run(workflow_id, device_id, step_ids=[step_id])

    def _execute_workflow_run(
        self,
        workflow_id: int,
        device_id: int,
        step_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        workflow = self.workflow_repository.get_workflow(workflow_id)
        device_record = self.device_repository.get_device(device_id)
        if not workflow:
            return {"success": False, "message": "Workflow not found"}
        if not device_record:
            return {"success": False, "message": "Device not found"}

        steps = self._resolve_execution_steps(workflow_id, step_ids)
        if not steps:
            return {"success": False, "message": "Selected step was not found in this workflow" if step_ids else "Workflow has no steps"}

        validation_errors = self._validate_steps(steps, include_structure=not step_ids)
        if validation_errors:
            scope_label = "Selected step validation failed" if step_ids else "Pre-run validation failed"
            message = scope_label + ":\n" + "\n".join(validation_errors)
            self.log_service.add(
                workflow_id,
                device_id,
                "ERROR",
                "validation_failed",
                f"Workflow '{workflow['name']}' validation failed",
                {"errors": validation_errors},
            )
            return {"success": False, "message": message}

        try:
            device = self.device_service.connect_device(device_record["serial"])
        except Exception as exc:
            self.log_service.add(
                workflow_id,
                device_id,
                "ERROR",
                "failed",
                f"Device connection failed: {exc}",
                {"serial": device_record["serial"]},
            )
            return {"success": False, "message": f"Device connection failed: {exc}"}

        executor = WorkflowExecutor(
            device=device,
            workflow=workflow,
            device_record=device_record,
            log_service=self.log_service,
            telemetry_service=self.telemetry_service,
            watchers=self.watcher_service.resolve_active_watchers(workflow_id, device_id),
            watcher_telemetry_service=self.watcher_telemetry_service,
        )

        execution_scope = "selected_step" if step_ids else "workflow"
        execution_name = (
            f"step '{steps[0]['name']}'"
            if step_ids and len(steps) == 1
            else f"{len(steps)} selected steps"
            if step_ids
            else f"workflow '{workflow['name']}'"
        )

        self.log_service.add(
            workflow_id,
            device_id,
            "INFO",
            "workflow_started",
            f"Started {execution_name}",
            {
                "step_count": len(steps),
                "watcher_count": len(executor.watchers),
                "device_serial": device_record["serial"],
                "run_id": executor.run_id,
                "artifact_dir": str(executor.run_artifact_dir),
                "execution_scope": execution_scope,
                "selected_step_ids": [int(step["id"]) for step in steps] if step_ids else [],
            },
        )

        try:
            summary = executor.run(steps)
            if summary.get("stopped_by_watcher"):
                self.log_service.add(
                    workflow_id,
                    device_id,
                    "WARNING",
                    "workflow_stopped",
                    f"{execution_name.capitalize()} stopped by watcher",
                    summary,
                )
                return {
                    "success": True,
                    "message": str(summary.get("stop_reason") or "Workflow stopped by watcher"),
                }
            self.log_service.add(
                workflow_id,
                device_id,
                "INFO",
                "workflow_success",
                f"Completed {execution_name}",
                summary,
            )
            continued = int(summary.get("continued_failures", 0))
            skipped = int(summary.get("skipped_failures", 0))
            message = (
                f"Selected step completed ({summary['executed_steps']} step)"
                if step_ids and len(steps) == 1
                else f"Selected steps completed ({summary['executed_steps']} steps)"
                if step_ids
                else f"Workflow completed ({summary['executed_steps']} steps)"
            )
            if continued or skipped:
                message += f" with {continued} continued failure(s) and {skipped} skipped failure(s)"
            return {
                "success": True,
                "message": message,
            }
        except Exception as exc:
            self.log_service.add(
                workflow_id,
                device_id,
                "ERROR",
                "workflow_failed",
                f"{execution_name.capitalize()} failed: {exc}",
                {
                    "run_id": executor.run_id,
                    "artifact_dir": str(executor.run_artifact_dir),
                    "execution_scope": execution_scope,
                },
            )
            return {"success": False, "message": str(exc)}
