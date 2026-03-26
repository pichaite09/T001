from __future__ import annotations

import importlib
import json
from typing import Any

from automation_studio.automation.engine import WorkflowExecutor
from automation_studio.repositories import DeviceRepository, LogRepository, WorkflowRepository


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


class WorkflowService:
    def __init__(
        self,
        workflow_repository: WorkflowRepository,
        device_repository: DeviceRepository,
        device_service: DeviceService,
        log_service: LogService,
    ) -> None:
        self.workflow_repository = workflow_repository
        self.device_repository = device_repository
        self.device_service = device_service
        self.log_service = log_service

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
        return self.workflow_repository.upsert_workflow(workflow_id, name, description, is_active)

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
        normalized = json.dumps(parsed, indent=2, ensure_ascii=False)
        return self.workflow_repository.upsert_step(
            step_id,
            workflow_id,
            position,
            name,
            step_type,
            normalized,
            is_enabled,
        )

    def delete_step(self, step_id: int) -> None:
        self.workflow_repository.delete_step(step_id)

    def reorder_steps(self, workflow_id: int, ordered_step_ids: list[int]) -> None:
        self.workflow_repository.reorder_steps(workflow_id, ordered_step_ids)

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

        self.log_service.add(
            workflow_id,
            device_id,
            "INFO",
            "started",
            f"Workflow '{workflow['name']}' started",
            {"step_count": len(steps), "device_serial": device_record["serial"]},
        )

        executor = WorkflowExecutor(
            device=device,
            workflow=workflow,
            device_record=device_record,
            log_service=self.log_service,
        )

        try:
            executed_steps = executor.run(steps)
            self.log_service.add(
                workflow_id,
                device_id,
                "INFO",
                "success",
                f"Workflow '{workflow['name']}' completed successfully",
                {"executed_steps": executed_steps},
            )
            return {
                "success": True,
                "message": f"Workflow completed ({executed_steps} steps)",
            }
        except Exception as exc:
            self.log_service.add(
                workflow_id,
                device_id,
                "ERROR",
                "failed",
                f"Workflow '{workflow['name']}' failed: {exc}",
                {},
            )
            return {"success": False, "message": str(exc)}
