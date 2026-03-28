from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    AccountRepository,
    DeviceRepository,
    LogRepository,
    TelemetryRepository,
    UploadRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)
from automation_studio.services import (
    AccountService,
    DeviceService,
    LogService,
    TelemetryService,
    UploadService,
    WatcherService,
    WatcherTelemetryService,
    WorkflowService,
)


class UploadApiApplication:
    def __init__(
        self,
        *,
        device_service: DeviceService,
        account_service: AccountService,
        workflow_service: WorkflowService,
        upload_service: UploadService,
        api_key: str = "",
    ) -> None:
        self.device_service = device_service
        self.account_service = account_service
        self.workflow_service = workflow_service
        self.upload_service = upload_service
        self.api_key = str(api_key or "").strip()
        self._run_lock = threading.Lock()

    def authorize(self, headers) -> tuple[bool, str]:
        if not self.api_key:
            return True, ""
        header_value = str(headers.get("X-API-Key") or "").strip()
        if header_value == self.api_key:
            return True, ""
        return False, "Unauthorized"

    def handle_request(
        self,
        method: str,
        raw_path: str,
        headers,
        body_text: str,
    ) -> tuple[int, dict[str, Any]]:
        authorized, message = self.authorize(headers)
        if not authorized:
            return 401, {"success": False, "message": message}

        parsed = urlparse(raw_path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query, keep_blank_values=False)
        if path == "/api/health" and method == "GET":
            return 200, {"success": True, "status": "ok"}
        if path == "/api/devices" and method == "GET":
            return 200, {"success": True, "items": [self._serialize_device(item) for item in self.device_service.list_devices()]}
        if path == "/api/workflows" and method == "GET":
            return 200, {"success": True, "items": [self._serialize_workflow(item) for item in self.workflow_service.list_workflows()]}

        if path.startswith("/api/devices/") and path.endswith("/platforms") and method == "GET":
            device_id = self._extract_numeric_path_part(path, prefix="/api/devices/", suffix="/platforms")
            if device_id is None:
                return 404, {"success": False, "message": "Not found"}
            items = self.account_service.list_device_platforms(device_id)
            return 200, {"success": True, "items": [self._serialize_platform(item) for item in items]}

        if path.startswith("/api/device-platforms/") and path.endswith("/accounts") and method == "GET":
            platform_id = self._extract_numeric_path_part(path, prefix="/api/device-platforms/", suffix="/accounts")
            if platform_id is None:
                return 404, {"success": False, "message": "Not found"}
            items = self.account_service.list_accounts(platform_id)
            return 200, {"success": True, "items": [self._serialize_account(item) for item in items]}

        if path == "/api/uploads":
            if method == "GET":
                items = [self._serialize_upload_job(item) for item in self._filter_upload_jobs(query)]
                return 200, {"success": True, "items": items}
            if method == "POST":
                payload = self._require_json_object(body_text)
                upload_job_id = self._save_upload_job(None, payload)
                item = self.upload_service.get_upload_job(upload_job_id)
                return 201, {"success": True, "item": self._serialize_upload_job(item)}
            return 405, {"success": False, "message": "Method not allowed"}

        if path == "/api/uploads/summary" and method == "GET":
            return 200, {"success": True, "item": self.upload_service.upload_summary()}

        if path == "/api/uploads/from-template" and method == "POST":
            payload = self._require_json_object(body_text)
            template_id = self._optional_int(payload.get("template_id"))
            if not template_id:
                raise ValueError("template_id is required")
            template = self.upload_service.get_upload_template(template_id)
            if not template:
                return 404, {"success": False, "message": "Upload template not found"}
            merged = self._build_upload_payload_from_template(template, payload)
            upload_job_id = self._save_upload_job(None, merged)
            item = self.upload_service.get_upload_job(upload_job_id)
            return 201, {"success": True, "item": self._serialize_upload_job(item)}

        if path == "/api/uploads/run-batch" and method == "POST":
            payload = self._require_json_object(body_text)
            upload_job_ids = payload.get("upload_job_ids")
            if not isinstance(upload_job_ids, list) or not upload_job_ids:
                raise ValueError("upload_job_ids must be a non-empty list")
            continue_on_error = bool(payload.get("continue_on_error", True))
            with self._run_lock:
                result = self.upload_service.execute_upload_jobs(
                    [int(upload_job_id) for upload_job_id in upload_job_ids],
                    continue_on_error=continue_on_error,
                )
            refreshed_items = []
            for upload_job_id in upload_job_ids:
                item = self.upload_service.get_upload_job(int(upload_job_id) or 0)
                if item:
                    refreshed_items.append(self._serialize_upload_job(item))
            return 200, {"success": True, "result": result, "items": refreshed_items}

        if path == "/api/uploads/export" and method == "POST":
            payload = self._require_json_object(body_text)
            upload_job_ids = payload.get("upload_job_ids")
            selected_ids = None
            if upload_job_ids is not None:
                if not isinstance(upload_job_ids, list):
                    raise ValueError("upload_job_ids must be a list")
                selected_ids = [int(upload_job_id) for upload_job_id in upload_job_ids]
            return 200, {"success": True, "item": self.upload_service.export_upload_jobs(selected_ids)}

        if path == "/api/uploads/import" and method == "POST":
            payload = self._require_json_object(body_text)
            created_ids = self.upload_service.import_upload_jobs(payload)
            items = []
            for upload_job_id in created_ids:
                item = self.upload_service.get_upload_job(upload_job_id)
                if item:
                    items.append(self._serialize_upload_job(item))
            return 201, {"success": True, "created_ids": created_ids, "items": items}

        if path == "/api/upload-templates":
            if method == "GET":
                items = [self._serialize_upload_template(item) for item in self._filter_upload_templates(query)]
                return 200, {"success": True, "items": items}
            if method == "POST":
                payload = self._require_json_object(body_text)
                template_id = self._save_upload_template(None, payload)
                item = self.upload_service.get_upload_template(template_id)
                return 201, {"success": True, "item": self._serialize_upload_template(item)}
            return 405, {"success": False, "message": "Method not allowed"}

        if path.startswith("/api/upload-templates/"):
            template_id = self._extract_numeric_path_part(path, prefix="/api/upload-templates/", suffix="")
            if template_id is None:
                return 404, {"success": False, "message": "Not found"}
            if method == "GET":
                item = self.upload_service.get_upload_template(template_id)
                if not item:
                    return 404, {"success": False, "message": "Upload template not found"}
                return 200, {"success": True, "item": self._serialize_upload_template(item)}
            if method == "PUT":
                current = self.upload_service.get_upload_template(template_id)
                if not current:
                    return 404, {"success": False, "message": "Upload template not found"}
                payload = self._require_json_object(body_text)
                merged = self._merge_upload_template_payload(current, payload)
                saved_id = self._save_upload_template(template_id, merged)
                item = self.upload_service.get_upload_template(saved_id)
                return 200, {"success": True, "item": self._serialize_upload_template(item)}
            if method == "DELETE":
                if not self.upload_service.get_upload_template(template_id):
                    return 404, {"success": False, "message": "Upload template not found"}
                self.upload_service.delete_upload_template(template_id)
                return 200, {"success": True, "deleted_id": template_id}
            return 405, {"success": False, "message": "Method not allowed"}

        if path.startswith("/api/uploads/"):
            upload_job_id, action = self._parse_upload_path(path)
            if upload_job_id is None:
                return 404, {"success": False, "message": "Not found"}

            if action == "" and method == "GET":
                item = self.upload_service.get_upload_job(upload_job_id)
                if not item:
                    return 404, {"success": False, "message": "Upload job not found"}
                return 200, {"success": True, "item": self._serialize_upload_job(item)}

            if action == "" and method == "PUT":
                current = self.upload_service.get_upload_job(upload_job_id)
                if not current:
                    return 404, {"success": False, "message": "Upload job not found"}
                payload = self._require_json_object(body_text)
                merged = self._merge_upload_payload(current, payload)
                saved_id = self._save_upload_job(upload_job_id, merged)
                item = self.upload_service.get_upload_job(saved_id)
                return 200, {"success": True, "item": self._serialize_upload_job(item)}

            if action == "" and method == "DELETE":
                if not self.upload_service.get_upload_job(upload_job_id):
                    return 404, {"success": False, "message": "Upload job not found"}
                self.upload_service.delete_upload_job(upload_job_id)
                return 200, {"success": True, "deleted_id": upload_job_id}

            if action == "run" and method == "POST":
                if not self.upload_service.get_upload_job(upload_job_id):
                    return 404, {"success": False, "message": "Upload job not found"}
                with self._run_lock:
                    result = self.upload_service.execute_upload_job(upload_job_id)
                item = self.upload_service.get_upload_job(upload_job_id)
                return 200, {"success": True, "result": result, "item": self._serialize_upload_job(item)}

            if action == "retry" and method == "POST":
                if not self.upload_service.get_upload_job(upload_job_id):
                    return 404, {"success": False, "message": "Upload job not found"}
                with self._run_lock:
                    result = self.upload_service.execute_upload_job(upload_job_id)
                item = self.upload_service.get_upload_job(upload_job_id)
                return 200, {"success": True, "result": result, "item": self._serialize_upload_job(item)}

            if action == "result" and method == "GET":
                item = self.upload_service.get_upload_job(upload_job_id)
                if not item:
                    return 404, {"success": False, "message": "Upload job not found"}
                return 200, {
                    "success": True,
                    "item": {
                        "id": upload_job_id,
                        "status": str(item.get("status") or ""),
                        "last_error": str(item.get("last_error") or ""),
                        "result": self._parse_json_text(item.get("result_json"), {}),
                    },
                }

            return 405, {"success": False, "message": "Method not allowed"}

        return 404, {"success": False, "message": "Not found"}

    def _save_upload_job(self, upload_job_id: int | None, payload: dict[str, Any]) -> int:
        tags_value = payload.get("tags", payload.get("tags_text", ""))
        if isinstance(tags_value, list):
            tags_text = "\n".join(str(item) for item in tags_value)
        else:
            tags_text = str(tags_value or "")

        metadata_value = payload.get("metadata", payload.get("metadata_text", ""))
        if isinstance(metadata_value, dict):
            metadata_text = json.dumps(metadata_value, ensure_ascii=False)
        else:
            metadata_text = str(metadata_value or "")

        return self.upload_service.save_upload_job(
            upload_job_id,
            device_id=int(payload.get("device_id") or 0),
            device_platform_id=self._optional_int(payload.get("device_platform_id")),
            account_id=self._optional_int(payload.get("account_id")),
            workflow_id=int(payload.get("workflow_id") or 0),
            code_product=str(payload.get("code_product") or ""),
            link_product=str(payload.get("link_product") or ""),
            title=str(payload.get("title") or ""),
            description=str(payload.get("description") or ""),
            tags_text=tags_text,
            video_url=str(payload.get("video_url") or ""),
            cover_url=str(payload.get("cover_url") or ""),
            local_video_path=str(payload.get("local_video_path") or ""),
            metadata_text=metadata_text,
        )

    def _save_upload_template(self, template_id: int | None, payload: dict[str, Any]) -> int:
        tags_value = payload.get("tags", payload.get("tags_text", ""))
        if isinstance(tags_value, list):
            tags_text = "\n".join(str(item) for item in tags_value)
        else:
            tags_text = str(tags_value or "")

        metadata_value = payload.get("metadata", payload.get("metadata_text", ""))
        if isinstance(metadata_value, dict):
            metadata_text = json.dumps(metadata_value, ensure_ascii=False)
        else:
            metadata_text = str(metadata_value or "")

        return self.upload_service.save_upload_template(
            template_id,
            name=str(payload.get("name") or ""),
            description=str(payload.get("description") or ""),
            device_id=self._optional_int(payload.get("device_id")),
            device_platform_id=self._optional_int(payload.get("device_platform_id")),
            account_id=self._optional_int(payload.get("account_id")),
            workflow_id=self._optional_int(payload.get("workflow_id")),
            code_product=str(payload.get("code_product") or ""),
            link_product=str(payload.get("link_product") or ""),
            title=str(payload.get("title") or ""),
            upload_description=str(payload.get("upload_description", payload.get("description_template", "")) or ""),
            tags_text=tags_text,
            video_url=str(payload.get("video_url") or ""),
            cover_url=str(payload.get("cover_url") or ""),
            local_video_path=str(payload.get("local_video_path") or ""),
            metadata_text=metadata_text,
        )

    def _build_upload_payload_from_template(
        self,
        template: dict[str, Any],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        merged = {
            "device_id": template.get("device_id"),
            "device_platform_id": template.get("device_platform_id"),
            "account_id": template.get("account_id"),
            "workflow_id": template.get("workflow_id"),
            "code_product": str(template.get("code_product") or ""),
            "link_product": str(template.get("link_product") or ""),
            "title": str(template.get("title") or ""),
            "description": str(template.get("description_template") or ""),
            "tags": self._parse_json_text(template.get("tags_json"), []),
            "video_url": str(template.get("video_url") or ""),
            "cover_url": str(template.get("cover_url") or ""),
            "local_video_path": str(template.get("local_video_path") or ""),
            "metadata": self._parse_json_text(template.get("metadata_json"), {}),
        }
        overrides = dict(payload)
        overrides.pop("template_id", None)
        merged.update(overrides)
        return merged

    def _merge_upload_payload(self, current: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        merged = {
            "device_id": int(current["device_id"]),
            "device_platform_id": current.get("device_platform_id"),
            "account_id": current.get("account_id"),
            "workflow_id": int(current["workflow_id"]),
            "code_product": str(current.get("code_product") or ""),
            "link_product": str(current.get("link_product") or ""),
            "title": str(current.get("title") or ""),
            "description": str(current.get("description") or ""),
            "tags": self._parse_json_text(current.get("tags_json"), []),
            "video_url": str(current.get("video_url") or ""),
            "cover_url": str(current.get("cover_url") or ""),
            "local_video_path": str(current.get("local_video_path") or ""),
            "metadata": self._parse_json_text(current.get("metadata_json"), {}),
        }
        merged.update(incoming)
        return merged

    def _merge_upload_template_payload(self, current: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
        merged = {
            "name": str(current.get("name") or ""),
            "description": str(current.get("description") or ""),
            "device_id": current.get("device_id"),
            "device_platform_id": current.get("device_platform_id"),
            "account_id": current.get("account_id"),
            "workflow_id": current.get("workflow_id"),
            "code_product": str(current.get("code_product") or ""),
            "link_product": str(current.get("link_product") or ""),
            "title": str(current.get("title") or ""),
            "description_template": str(current.get("description_template") or ""),
            "tags": self._parse_json_text(current.get("tags_json"), []),
            "video_url": str(current.get("video_url") or ""),
            "cover_url": str(current.get("cover_url") or ""),
            "local_video_path": str(current.get("local_video_path") or ""),
            "metadata": self._parse_json_text(current.get("metadata_json"), {}),
        }
        merged.update(incoming)
        return merged

    def _serialize_upload_job(self, upload_job: dict[str, Any] | None) -> dict[str, Any]:
        if not upload_job:
            return {}
        payload = dict(upload_job)
        payload["tags"] = self._parse_json_text(upload_job.get("tags_json"), [])
        payload["metadata"] = self._parse_json_text(upload_job.get("metadata_json"), {})
        payload["result"] = self._parse_json_text(upload_job.get("result_json"), {})
        return payload

    def _serialize_upload_template(self, upload_template: dict[str, Any] | None) -> dict[str, Any]:
        if not upload_template:
            return {}
        payload = dict(upload_template)
        payload["tags"] = self._parse_json_text(upload_template.get("tags_json"), [])
        payload["metadata"] = self._parse_json_text(upload_template.get("metadata_json"), {})
        return payload

    def _serialize_device(self, device: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(device["id"]),
            "name": str(device.get("name") or ""),
            "serial": str(device.get("serial") or ""),
            "last_status": str(device.get("last_status") or ""),
            "last_seen": str(device.get("last_seen") or ""),
        }

    def _serialize_workflow(self, workflow: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(workflow["id"]),
            "name": str(workflow.get("name") or ""),
            "description": str(workflow.get("description") or ""),
            "is_active": bool(workflow.get("is_active", 1)),
        }

    def _serialize_platform(self, platform: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(platform["id"]),
            "device_id": int(platform["device_id"]),
            "platform_key": str(platform.get("platform_key") or ""),
            "platform_name": str(platform.get("platform_name") or ""),
            "package_name": str(platform.get("package_name") or ""),
            "current_account_id": int(platform.get("current_account_id") or 0) or None,
            "is_enabled": bool(platform.get("is_enabled", 1)),
        }

    def _serialize_account(self, account: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(account["id"]),
            "device_platform_id": int(account["device_platform_id"]),
            "display_name": str(account.get("display_name") or ""),
            "username": str(account.get("username") or ""),
            "login_id": str(account.get("login_id") or ""),
            "is_enabled": bool(account.get("is_enabled", 1)),
        }

    def _filter_upload_jobs(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        items = self.upload_service.list_upload_jobs()
        status_filter = self._query_value(query, "status")
        device_id = self._optional_int(self._query_value(query, "device_id"))
        workflow_id = self._optional_int(self._query_value(query, "workflow_id"))
        account_id = self._optional_int(self._query_value(query, "account_id"))
        platform_id = self._optional_int(self._query_value(query, "device_platform_id"))
        if status_filter:
            items = [item for item in items if str(item.get("status") or "") == status_filter]
        if device_id:
            items = [item for item in items if int(item.get("device_id") or 0) == device_id]
        if workflow_id:
            items = [item for item in items if int(item.get("workflow_id") or 0) == workflow_id]
        if account_id:
            items = [item for item in items if int(item.get("account_id") or 0) == account_id]
        if platform_id:
            items = [item for item in items if int(item.get("device_platform_id") or 0) == platform_id]
        return items

    def _filter_upload_templates(self, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        items = self.upload_service.list_upload_templates()
        device_id = self._optional_int(self._query_value(query, "device_id"))
        workflow_id = self._optional_int(self._query_value(query, "workflow_id"))
        active_only = self._query_value(query, "active_only")
        if device_id:
            items = [item for item in items if int(item.get("device_id") or 0) == device_id]
        if workflow_id:
            items = [item for item in items if int(item.get("workflow_id") or 0) == workflow_id]
        if active_only is not None:
            active_flag = str(active_only).strip().lower() in {"1", "true", "yes", "on"}
            items = [item for item in items if bool(item.get("is_active", 1)) == active_flag]
        return items

    def _query_value(self, query: dict[str, list[str]], key: str) -> str | None:
        values = query.get(key) or []
        if not values:
            return None
        value = str(values[0] or "").strip()
        return value or None

    def _extract_numeric_path_part(self, path: str, *, prefix: str, suffix: str) -> int | None:
        if not path.startswith(prefix):
            return None
        if suffix:
            if not path.endswith(suffix):
                return None
            raw = path[len(prefix) : len(path) - len(suffix)]
        else:
            raw = path[len(prefix) :]
        return self._optional_int(raw)

    def _parse_upload_path(self, path: str) -> tuple[int | None, str]:
        raw = path[len("/api/uploads/") :]
        parts = [part for part in raw.split("/") if part]
        if not parts:
            return None, ""
        upload_job_id = self._optional_int(parts[0])
        action = parts[1] if len(parts) > 1 else ""
        return upload_job_id, action

    def _require_json_object(self, body_text: str) -> dict[str, Any]:
        try:
            payload = json.loads(body_text or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON body: {exc}") from exc
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def _optional_int(self, value: Any) -> int | None:
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            return None
        return normalized or None

    def _parse_json_text(self, value: Any, default: Any) -> Any:
        try:
            return json.loads(value or json.dumps(default, ensure_ascii=False))
        except Exception:
            return default


def create_api_application(
    db_path: str | Path,
    *,
    api_key: str = "",
    device_service: DeviceService | None = None,
) -> UploadApiApplication:
    db = DatabaseManager(db_path)
    db.init_schema()

    device_repository = DeviceRepository(db)
    account_repository = AccountRepository(db)
    workflow_repository = WorkflowRepository(db)
    upload_repository = UploadRepository(db)
    log_repository = LogRepository(db)
    telemetry_repository = TelemetryRepository(db)
    watcher_repository = WatcherRepository(db)
    watcher_telemetry_repository = WatcherTelemetryRepository(db)

    device_service = device_service or DeviceService(device_repository)
    account_service = AccountService(
        account_repository,
        device_repository,
        workflow_repository,
    )
    log_service = LogService(log_repository)
    telemetry_service = TelemetryService(telemetry_repository)
    watcher_telemetry_service = WatcherTelemetryService(watcher_telemetry_repository)
    watcher_service = WatcherService(
        watcher_repository,
        device_repository,
        device_service,
        log_service,
        watcher_telemetry_service,
    )
    workflow_service = WorkflowService(
        workflow_repository,
        device_repository,
        device_service,
        log_service,
        telemetry_service,
        watcher_service,
        watcher_telemetry_service,
        account_service,
    )
    upload_service = UploadService(
        upload_repository,
        device_repository,
        workflow_repository,
        account_service,
        workflow_service,
    )
    workflow_service.bind_upload_service(upload_service)
    upload_service.bind_workflow_service(workflow_service)

    return UploadApiApplication(
        device_service=device_service,
        account_service=account_service,
        workflow_service=workflow_service,
        upload_service=upload_service,
        api_key=api_key,
    )


def create_http_server(app: UploadApiApplication, host: str = "127.0.0.1", port: int = 8000) -> ThreadingHTTPServer:
    class UploadApiHandler(BaseHTTPRequestHandler):
        server_version = "AutomationStudioUploadAPI/1.0"

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

        def do_PUT(self) -> None:
            self._handle("PUT")

        def do_DELETE(self) -> None:
            self._handle("DELETE")

        def log_message(self, format: str, *args) -> None:
            return

        def _handle(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode("utf-8") if length > 0 else ""
            try:
                status, payload = app.handle_request(method, self.path, self.headers, body)
            except ValueError as exc:
                status, payload = 400, {"success": False, "message": str(exc)}
            except Exception as exc:
                status, payload = 500, {"success": False, "message": str(exc)}

            response = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)

    return ThreadingHTTPServer((host, int(port)), UploadApiHandler)


def run_api_server(
    db_path: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    api_key: str = "",
) -> None:
    app = create_api_application(db_path, api_key=api_key)
    server = create_http_server(app, host=host, port=port)
    print(f"Upload API listening on http://{host}:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Automation Studio Upload API server.")
    parser.add_argument("--db-path", default=str(Path.cwd() / "automation_studio.db"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--api-key", default="")
    args = parser.parse_args()
    run_api_server(
        args.db_path,
        host=args.host,
        port=args.port,
        api_key=args.api_key,
    )


if __name__ == "__main__":
    main()
