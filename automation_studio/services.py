from __future__ import annotations

import importlib
import json
from datetime import datetime
from typing import Any

from automation_studio.automation.engine import WorkflowExecutor
from automation_studio.models import (
    STEP_SCHEMA_VERSION,
    WORKFLOW_DEFINITION_VERSION,
    migrate_step_parameters,
    validate_step_parameters,
    validate_workflow_structure,
)
from automation_studio.repositories import DeviceRepository, LogRepository, TelemetryRepository, WorkflowRepository


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
    ) -> int:
        return self.log_repository.add_log(workflow_id, device_id, level, status, message, metadata)

    def list_logs(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
        status: str | None = None,
        limit: int = 300,
        ) -> list[dict[str, Any]]:
        return self.log_repository.list_logs(workflow_id, device_id, status, limit)


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


class WorkflowService:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        device_repository: DeviceRepository,
        device_service: DeviceService,
        log_service: LogService,
        telemetry_service: TelemetryService,
    ) -> None:
        self.workflow_repository = workflow_repository
        self.device_repository = device_repository
        self.device_service = device_service
        self.log_service = log_service
        self.telemetry_service = telemetry_service

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
        errors.extend(validate_workflow_structure(steps))
        return errors

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
        workflow = self.workflow_repository.get_workflow(workflow_id)
        device_record = self.device_repository.get_device(device_id)
        if not workflow:
            return {"success": False, "message": "Workflow not found"}
        if not device_record:
            return {"success": False, "message": "Device not found"}

        steps = self.workflow_repository.list_steps(workflow_id)
        if not steps:
            return {"success": False, "message": "Workflow has no steps"}

        validation_errors = self.validate_workflow_steps(workflow_id)
        if validation_errors:
            message = "Pre-run validation failed:\n" + "\n".join(validation_errors)
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
        )

        self.log_service.add(
            workflow_id,
            device_id,
            "INFO",
            "workflow_started",
            f"Workflow '{workflow['name']}' started",
            {
                "step_count": len(steps),
                "device_serial": device_record["serial"],
                "run_id": executor.run_id,
                "artifact_dir": str(executor.run_artifact_dir),
            },
        )

        try:
            summary = executor.run(steps)
            self.log_service.add(
                workflow_id,
                device_id,
                "INFO",
                "workflow_success",
                f"Workflow '{workflow['name']}' completed successfully",
                summary,
            )
            continued = int(summary.get("continued_failures", 0))
            skipped = int(summary.get("skipped_failures", 0))
            message = f"Workflow completed ({summary['executed_steps']} steps)"
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
                f"Workflow '{workflow['name']}' failed: {exc}",
                {
                    "run_id": executor.run_id,
                    "artifact_dir": str(executor.run_artifact_dir),
                },
            )
            return {"success": False, "message": str(exc)}
