from __future__ import annotations

import importlib
import json
import random
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
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
    AccountRepository,
    DeviceRepository,
    LogRepository,
    ScheduleGroupRepository,
    ScheduleRepository,
    ScheduleRunRepository,
    TelemetryRepository,
    UploadRepository,
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

    def _save_runtime_info(self, device_id: int | None, status: str, snapshot: dict[str, Any]) -> None:
        if not device_id:
            return
        self.device_repository.update_runtime_info(
            int(device_id),
            status,
            json.dumps(snapshot, ensure_ascii=False),
        )

    def _safe_window_size(self, device) -> dict[str, int]:
        try:
            width, height = device.window_size()
            return {"width": int(width), "height": int(height)}
        except Exception:
            return {}

    def _safe_current_app(self, device) -> dict[str, Any]:
        try:
            current = device.app_current()
            return current if isinstance(current, dict) else {}
        except Exception:
            return {}

    def _safe_screen_on(self, device) -> bool | None:
        try:
            value = getattr(device, "screen_on")
            return bool(value() if callable(value) else value)
        except Exception:
            return None

    def refresh_runtime_info(
        self,
        serial: str,
        device_id: int | None = None,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        try:
            device = self.connect_device(serial)
            base_info = device.info if isinstance(device.info, dict) else {}
            snapshot = dict(base_info)

            window_size = self._safe_window_size(device)
            if window_size:
                snapshot["window_size"] = window_size

            current_app = self._safe_current_app(device)
            if current_app:
                snapshot["current_app"] = current_app

            screen_on = self._safe_screen_on(device)
            if screen_on is not None:
                snapshot["screen_on"] = screen_on

            manufacturer = snapshot.get("manufacturer") or snapshot.get("brand") or "Unknown"
            model = snapshot.get("model") or snapshot.get("marketName") or snapshot.get("device") or "Unknown"
            android_version = snapshot.get("version") or snapshot.get("release") or snapshot.get("sdkInt") or "Unknown"
            current_package = (
                current_app.get("package")
                or current_app.get("packageName")
                or snapshot.get("currentPackageName")
                or "-"
            )
            message = f"Connected: {manufacturer} {model} / Android {android_version} / App {current_package}"
            self._save_runtime_info(device_id, "connected", snapshot)
            return True, message, snapshot
        except Exception as exc:
            if device_id:
                self.device_repository.update_status(device_id, "failed")
            return False, f"Connection failed: {exc}", None

    def test_connection(
        self,
        serial: str,
        device_id: int | None = None,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        return self.refresh_runtime_info(serial, device_id)

    def capture_screenshot(
        self,
        serial: str,
        output_path: str | Path,
        device_id: int | None = None,
    ) -> tuple[bool, str, str | None]:
        try:
            device = self.connect_device(serial)
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            result = device.screenshot(str(path))
            if not path.exists() and hasattr(result, "save"):
                result.save(str(path))
            if not path.exists():
                raise RuntimeError("Screenshot was not written to disk")
            if device_id:
                self.device_repository.update_status(device_id, "connected")
            return True, f"Saved screenshot to {path}", str(path)
        except Exception as exc:
            if device_id:
                self.device_repository.update_status(device_id, "failed")
            return False, f"Screenshot failed: {exc}", None

    def dump_hierarchy(
        self,
        serial: str,
        output_path: str | Path,
        device_id: int | None = None,
    ) -> tuple[bool, str, str | None]:
        try:
            device = self.connect_device(serial)
            path = Path(output_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            hierarchy = str(device.dump_hierarchy())
            path.write_text(hierarchy, encoding="utf-8")
            if device_id:
                self.device_repository.update_status(device_id, "connected")
            return True, f"Saved hierarchy to {path}", str(path)
        except Exception as exc:
            if device_id:
                self.device_repository.update_status(device_id, "failed")
            return False, f"Hierarchy dump failed: {exc}", None


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
        platform_key: str | None = None,
        account_id: int | None = None,
        limit: int = 300,
        ) -> list[dict[str, Any]]:
        fetch_limit = max(limit, 2000) if platform_key or account_id else limit
        logs = self.log_repository.list_logs(workflow_id, device_id, watcher_id, status, fetch_limit)
        if not platform_key and not account_id:
            return logs

        normalized_platform_key = str(platform_key or "").strip().lower()
        normalized_account_id = int(account_id or 0) or None
        filtered_logs: list[dict[str, Any]] = []
        for log in logs:
            try:
                metadata = json.loads(log.get("metadata") or "{}")
            except Exception:
                metadata = {}
            if normalized_platform_key and str(metadata.get("platform_key") or "").strip().lower() != normalized_platform_key:
                continue
            if normalized_account_id is not None and int(metadata.get("account_id") or 0) != normalized_account_id:
                continue
            filtered_logs.append(log)
            if len(filtered_logs) >= limit:
                break
        return filtered_logs


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


class AccountService:
    def __init__(
        self,
        account_repository: AccountRepository,
        device_repository: DeviceRepository,
        workflow_repository: WorkflowRepository,
    ) -> None:
        self.account_repository = account_repository
        self.device_repository = device_repository
        self.workflow_repository = workflow_repository

    def list_device_platforms(self, device_id: int) -> list[dict[str, Any]]:
        return self.account_repository.list_device_platforms(device_id)

    def get_device_platform(self, device_platform_id: int) -> dict[str, Any] | None:
        return self.account_repository.get_device_platform(device_platform_id)

    def save_device_platform(
        self,
        device_platform_id: int | None,
        device_id: int,
        platform_key: str,
        platform_name: str,
        package_name: str,
        switch_workflow_id: int | None,
        is_enabled: bool = True,
    ) -> int:
        normalized_key = str(platform_key or "").strip().lower()
        normalized_name = str(platform_name or "").strip()
        if not normalized_key:
            raise ValueError("Platform key is required")
        if not normalized_name:
            raise ValueError("Platform name is required")
        if not self.device_repository.get_device(device_id):
            raise ValueError("Device not found")
        if switch_workflow_id and not self.workflow_repository.get_workflow(int(switch_workflow_id)):
            raise ValueError("Switch workflow not found")
        existing = self.account_repository.get_device_platform_by_key(device_id, normalized_key)
        if existing and int(existing["id"]) != int(device_platform_id or 0):
            raise ValueError("Platform key already exists for this device")
        return self.account_repository.upsert_device_platform(
            device_platform_id=device_platform_id,
            device_id=device_id,
            platform_key=normalized_key,
            platform_name=normalized_name,
            package_name=str(package_name or "").strip(),
            switch_workflow_id=int(switch_workflow_id) if switch_workflow_id else None,
            is_enabled=is_enabled,
        )

    def delete_device_platform(self, device_platform_id: int) -> None:
        self.account_repository.delete_device_platform(device_platform_id)

    def list_accounts(self, device_platform_id: int) -> list[dict[str, Any]]:
        return self.account_repository.list_accounts(device_platform_id)

    def get_account(self, account_id: int) -> dict[str, Any] | None:
        return self.account_repository.get_account(account_id)

    def save_account(
        self,
        account_id: int | None,
        device_platform_id: int,
        display_name: str,
        username: str,
        login_id: str,
        notes: str,
        metadata_text: str,
        is_enabled: bool = True,
        aliases_text: str = "",
    ) -> int:
        platform = self.account_repository.get_device_platform(device_platform_id)
        if not platform:
            raise ValueError("Device platform not found")
        normalized_display_name = str(display_name or "").strip()
        if not normalized_display_name:
            raise ValueError("Account display name is required")
        normalized_username = str(username or "").strip()
        normalized_login_id = str(login_id or "").strip()
        base_identity_values = {
            self._normalize_identity(normalized_display_name),
            self._normalize_identity(normalized_username),
            self._normalize_identity(normalized_login_id),
        }
        aliases = [
            alias_name
            for alias_name in self._parse_aliases_text(aliases_text)
            if self._normalize_identity(alias_name) not in {value for value in base_identity_values if value}
        ]
        candidate_identities = self._identity_values(
            normalized_display_name,
            normalized_username,
            normalized_login_id,
            aliases,
        )
        if not candidate_identities:
            raise ValueError("Account must provide at least one display name or alias")
        try:
            parsed_metadata = json.loads(metadata_text or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError(f"Metadata JSON is invalid: {exc}") from exc
        conflicting_account = self._find_account_identity_conflict(
            device_platform_id=device_platform_id,
            identity_values=candidate_identities,
            exclude_account_id=account_id,
        )
        if conflicting_account:
            conflict_name = str(conflicting_account.get("display_name") or f"#{conflicting_account['id']}")
            raise ValueError(f"Account name or alias already belongs to '{conflict_name}' on this platform")

        saved_account_id = self.account_repository.upsert_account(
            account_id=account_id,
            device_platform_id=device_platform_id,
            display_name=normalized_display_name,
            display_name_normalized=self._normalize_identity(normalized_display_name),
            username=normalized_username,
            username_normalized=self._normalize_identity(normalized_username),
            login_id=normalized_login_id,
            login_id_normalized=self._normalize_identity(normalized_login_id),
            notes=str(notes or "").strip(),
            metadata_json=json.dumps(parsed_metadata, indent=2, ensure_ascii=False),
            is_enabled=is_enabled,
        )
        self.account_repository.replace_account_aliases(
            saved_account_id,
            device_platform_id,
            [(alias_name, self._normalize_identity(alias_name)) for alias_name in aliases],
        )
        return saved_account_id

    def delete_account(self, account_id: int) -> None:
        self.account_repository.delete_account(account_id)

    def set_current_account(self, device_platform_id: int, account_id: int | None) -> None:
        self.account_repository.update_current_account(device_platform_id, account_id)

    def resolve_switch_target(
        self,
        device_id: int,
        platform_key: str,
        account_id: int | None = None,
        account_name: str | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        device_platform = self.account_repository.get_device_platform_by_key(device_id, str(platform_key or "").strip().lower())
        if not device_platform:
            raise ValueError("Platform not found on this device")
        if not bool(device_platform.get("is_enabled", 1)):
            raise ValueError("Platform is disabled")
        if account_id:
            account = self.account_repository.get_account(int(account_id))
            if not account or int(account["device_platform_id"]) != int(device_platform["id"]):
                raise ValueError("Account not found for this platform")
        else:
            normalized_account_name = self._normalize_identity(account_name)
            if not normalized_account_name:
                raise ValueError("Account name is required")
            account = self.account_repository.get_account_by_identity(int(device_platform["id"]), normalized_account_name)
            if not account:
                raise ValueError("Account not found for this platform")
        if not bool(account.get("is_enabled", 1)):
            raise ValueError("Account is disabled")
        return device_platform, account

    def list_accounts_for_platform(
        self,
        device_id: int,
        platform_key: str,
        *,
        only_enabled: bool = True,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        device_platform = self.account_repository.get_device_platform_by_key(device_id, str(platform_key or "").strip().lower())
        if not device_platform:
            raise ValueError("Platform not found on this device")
        if not bool(device_platform.get("is_enabled", 1)):
            raise ValueError("Platform is disabled")
        accounts = self.account_repository.list_accounts(int(device_platform["id"]))
        if only_enabled:
            accounts = [account for account in accounts if bool(account.get("is_enabled", 1))]
        return device_platform, accounts

    def resolve_runtime_context(
        self,
        device_id: int,
        device_platform_id: int | None = None,
        account_id: int | None = None,
        use_current_account: bool = False,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not device_platform_id:
            return {}, {}

        device_platform = self.account_repository.get_device_platform(int(device_platform_id))
        if not device_platform or int(device_platform["device_id"]) != int(device_id):
            raise ValueError("Platform not found on this device")
        if not bool(device_platform.get("is_enabled", 1)):
            raise ValueError("Platform is disabled")

        platform_context = {
            "id": int(device_platform["id"]),
            "key": str(device_platform["platform_key"] or ""),
            "name": str(device_platform["platform_name"] or ""),
            "package_name": str(device_platform.get("package_name") or ""),
            "switch_workflow_id": int(device_platform.get("switch_workflow_id") or 0) or None,
        }
        runtime_context: dict[str, Any] = {
            "platform": platform_context,
            "vars": {
                "current_platform_key": platform_context["key"],
                "current_platform_name": platform_context["name"],
            },
        }
        metadata: dict[str, Any] = {
            "device_platform_id": platform_context["id"],
            "platform_key": platform_context["key"],
            "platform_name": platform_context["name"],
        }

        resolved_account: dict[str, Any] | None = None
        if account_id:
            resolved_account = self.account_repository.get_account(int(account_id))
            if not resolved_account or int(resolved_account["device_platform_id"]) != int(device_platform["id"]):
                raise ValueError("Account not found for this platform")
            if not bool(resolved_account.get("is_enabled", 1)):
                raise ValueError("Account is disabled")
        elif use_current_account and int(device_platform.get("current_account_id") or 0) > 0:
            resolved_account = self.account_repository.get_account(int(device_platform["current_account_id"]))
            if resolved_account and not bool(resolved_account.get("is_enabled", 1)):
                resolved_account = None

        if resolved_account:
            account_context = {
                "id": int(resolved_account["id"]),
                "device_platform_id": int(resolved_account["device_platform_id"]),
                "display_name": str(resolved_account.get("display_name") or ""),
                "username": str(resolved_account.get("username") or ""),
                "login_id": str(resolved_account.get("login_id") or ""),
                "aliases": self._parse_aliases_text(str(resolved_account.get("alias_names") or "")),
                "notes": str(resolved_account.get("notes") or ""),
                "metadata": json.loads(resolved_account.get("metadata_json") or "{}"),
            }
            runtime_context["account"] = account_context
            runtime_context["vars"].update(
                {
                    "current_account_id": account_context["id"],
                    "current_account_name": account_context["display_name"],
                    "current_account_username": account_context["username"],
                    "current_account_login_id": account_context["login_id"],
                }
            )
            metadata.update(
                {
                    "account_id": account_context["id"],
                    "account_name": account_context["display_name"],
                    "account_username": account_context["username"],
                    "account_login_id": account_context["login_id"],
                }
            )

        return runtime_context, metadata

    def _normalize_identity(self, value: str | None) -> str:
        return str(value or "").strip().lstrip("@").strip()

    def _parse_aliases_text(self, aliases_text: str | None) -> list[str]:
        parts = re.split(r"[\r\n,;]+", str(aliases_text or ""))
        aliases: list[str] = []
        seen: set[str] = set()
        for part in parts:
            alias_name = str(part or "").strip()
            if not alias_name:
                continue
            normalized = self._normalize_identity(alias_name)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            aliases.append(alias_name)
        return aliases

    def _identity_values(
        self,
        display_name: str,
        username: str,
        login_id: str,
        aliases: list[str],
    ) -> set[str]:
        values = {
            self._normalize_identity(display_name),
            self._normalize_identity(username),
            self._normalize_identity(login_id),
        }
        values.update(self._normalize_identity(alias_name) for alias_name in aliases)
        return {value for value in values if value}

    def _account_identity_values(self, account: dict[str, Any]) -> set[str]:
        return self._identity_values(
            str(account.get("display_name") or ""),
            str(account.get("username") or ""),
            str(account.get("login_id") or ""),
            self._parse_aliases_text(str(account.get("alias_names") or "")),
        )

    def _find_account_identity_conflict(
        self,
        *,
        device_platform_id: int,
        identity_values: set[str],
        exclude_account_id: int | None = None,
    ) -> dict[str, Any] | None:
        if not identity_values:
            return None
        for account in self.account_repository.list_accounts(device_platform_id):
            if int(account["id"]) == int(exclude_account_id or 0):
                continue
            if self._account_identity_values(account) & identity_values:
                return account
        return None


class UploadService:
    def __init__(
        self,
        upload_repository: UploadRepository,
        device_repository: DeviceRepository,
        workflow_repository: WorkflowRepository,
        account_service: AccountService,
        workflow_service: WorkflowService | None = None,
    ) -> None:
        self.upload_repository = upload_repository
        self.device_repository = device_repository
        self.workflow_repository = workflow_repository
        self.account_service = account_service
        self.workflow_service = workflow_service

    def bind_workflow_service(self, workflow_service: WorkflowService) -> None:
        self.workflow_service = workflow_service

    def list_upload_jobs(self) -> list[dict[str, Any]]:
        return self.upload_repository.list_upload_jobs()

    def get_upload_job(self, upload_job_id: int) -> dict[str, Any] | None:
        return self.upload_repository.get_upload_job(upload_job_id)

    def list_upload_templates(self) -> list[dict[str, Any]]:
        return self.upload_repository.list_upload_templates()

    def get_upload_template(self, template_id: int) -> dict[str, Any] | None:
        return self.upload_repository.get_upload_template(template_id)

    def save_upload_job(
        self,
        upload_job_id: int | None,
        *,
        device_id: int,
        device_platform_id: int | None,
        account_id: int | None,
        workflow_id: int,
        code_product: str,
        link_product: str,
        title: str,
        description: str,
        tags_text: str,
        video_url: str,
        cover_url: str = "",
        local_video_path: str = "",
        metadata_text: str = "",
    ) -> int:
        if not self.device_repository.get_device(int(device_id)):
            raise ValueError("Device not found")
        if not self.workflow_repository.get_workflow(int(workflow_id)):
            raise ValueError("Workflow not found")

        normalized_platform_id = int(device_platform_id) if device_platform_id else None
        normalized_account_id = int(account_id) if account_id else None
        if normalized_platform_id:
            platform = self.account_service.get_device_platform(normalized_platform_id)
            if not platform or int(platform["device_id"]) != int(device_id):
                raise ValueError("Platform does not belong to the selected device")
        elif normalized_account_id:
            raise ValueError("Select a platform before choosing an account")

        if normalized_account_id:
            account = self.account_service.get_account(normalized_account_id)
            if not account or int(account["device_platform_id"]) != int(normalized_platform_id or 0):
                raise ValueError("Account does not belong to the selected platform")

        normalized_title = str(title or "").strip()
        normalized_video_url = str(video_url or "").strip()
        normalized_local_video_path = str(local_video_path or "").strip()
        if not normalized_title:
            raise ValueError("Title is required")
        if not normalized_video_url and not normalized_local_video_path:
            raise ValueError("Video URL or Local Video Path is required")

        tags = self._parse_tags_text(tags_text)
        metadata_json = self._normalize_metadata_text(metadata_text)
        return self.upload_repository.upsert_upload_job(
            upload_job_id,
            device_id=int(device_id),
            device_platform_id=normalized_platform_id,
            account_id=normalized_account_id,
            workflow_id=int(workflow_id),
            code_product=str(code_product or "").strip(),
            link_product=str(link_product or "").strip(),
            title=normalized_title,
            description=str(description or "").strip(),
            tags_json=json.dumps(tags, ensure_ascii=False),
            video_url=normalized_video_url,
            cover_url=str(cover_url or "").strip(),
            local_video_path=normalized_local_video_path,
            metadata_json=metadata_json,
            status="draft",
        )

    def save_upload_template(
        self,
        template_id: int | None,
        *,
        name: str,
        description: str,
        device_id: int | None,
        device_platform_id: int | None,
        account_id: int | None,
        workflow_id: int | None,
        code_product: str,
        link_product: str,
        title: str,
        upload_description: str,
        tags_text: str,
        video_url: str,
        cover_url: str = "",
        local_video_path: str = "",
        metadata_text: str = "",
    ) -> int:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("Template name is required")

        normalized_device_id = int(device_id) if device_id else None
        normalized_platform_id = int(device_platform_id) if device_platform_id else None
        normalized_account_id = int(account_id) if account_id else None
        normalized_workflow_id = int(workflow_id) if workflow_id else None

        if normalized_device_id and not self.device_repository.get_device(normalized_device_id):
            raise ValueError("Template device not found")
        if normalized_workflow_id and not self.workflow_repository.get_workflow(normalized_workflow_id):
            raise ValueError("Template workflow not found")
        if normalized_platform_id:
            platform = self.account_service.get_device_platform(normalized_platform_id)
            if not platform or int(platform["device_id"]) != int(normalized_device_id or 0):
                raise ValueError("Template platform does not belong to the selected device")
        elif normalized_account_id:
            raise ValueError("Select a platform before choosing an account")
        if normalized_account_id:
            account = self.account_service.get_account(normalized_account_id)
            if not account or int(account["device_platform_id"]) != int(normalized_platform_id or 0):
                raise ValueError("Template account does not belong to the selected platform")

        tags = self._parse_tags_text(tags_text)
        metadata_json = self._normalize_metadata_text(metadata_text)
        return self.upload_repository.upsert_upload_template(
            template_id,
            name=normalized_name,
            description=str(description or "").strip(),
            device_id=normalized_device_id,
            device_platform_id=normalized_platform_id,
            account_id=normalized_account_id,
            workflow_id=normalized_workflow_id,
            code_product=str(code_product or "").strip(),
            link_product=str(link_product or "").strip(),
            title=str(title or "").strip(),
            description_template=str(upload_description or "").strip(),
            tags_json=json.dumps(tags, ensure_ascii=False),
            video_url=str(video_url or "").strip(),
            cover_url=str(cover_url or "").strip(),
            local_video_path=str(local_video_path or "").strip(),
            metadata_json=metadata_json,
            is_active=True,
        )

    def delete_upload_template(self, template_id: int) -> None:
        self.upload_repository.delete_upload_template(template_id)

    def delete_upload_job(self, upload_job_id: int) -> None:
        self.upload_repository.delete_upload_job(upload_job_id)

    def execute_upload_job(self, upload_job_id: int) -> dict[str, Any]:
        if not self.workflow_service:
            raise RuntimeError("Workflow service is not available")
        upload_job = self.upload_repository.get_upload_job(upload_job_id)
        if not upload_job:
            return {"success": False, "message": "Upload job not found"}

        self.upload_repository.mark_upload_started(upload_job_id)
        upload_context = self._build_upload_context(upload_job)
        self.workflow_service.log_service.add(
            int(upload_job["workflow_id"]),
            int(upload_job["device_id"]),
            "INFO",
            "upload_started",
            f"Started upload job #{upload_job_id}",
            {
                "upload_job_id": upload_context["id"],
                "upload_code_product": upload_context["code_product"],
                "upload_title": upload_context["title"],
                "upload_video_url": upload_context["video_url"],
            },
        )
        extra_context = {
            "upload": upload_context,
            "vars": {
                "upload_job_id": upload_context["id"],
                "upload_code_product": upload_context["code_product"],
                "upload_link_product": upload_context["link_product"],
                "upload_title": upload_context["title"],
                "upload_description": upload_context["description"],
                "upload_tags": upload_context["tags"],
                "upload_video_url": upload_context["video_url"],
                "upload_cover_url": upload_context["cover_url"],
                "upload_local_video_path": upload_context["local_video_path"],
                "upload_metadata": upload_context["metadata"],
            },
        }
        extra_metadata = {
            "upload_job_id": upload_context["id"],
            "upload_code_product": upload_context["code_product"],
            "upload_title": upload_context["title"],
            "upload_video_url": upload_context["video_url"],
            "upload_local_video_path": upload_context["local_video_path"],
        }
        result = self.workflow_service.execute_workflow(
            int(upload_job["workflow_id"]),
            int(upload_job["device_id"]),
            device_platform_id=int(upload_job.get("device_platform_id") or 0) or None,
            account_id=int(upload_job.get("account_id") or 0) or None,
            extra_context=extra_context,
            extra_metadata=extra_metadata,
        )
        final_status = "success" if bool(result.get("success")) else "failed"
        self.upload_repository.mark_upload_finished(
            upload_job_id,
            status=final_status,
            last_error="" if result.get("success") else str(result.get("message") or ""),
            result_json=json.dumps(result, ensure_ascii=False),
        )
        self.workflow_service.log_service.add(
            int(upload_job["workflow_id"]),
            int(upload_job["device_id"]),
            "INFO" if result.get("success") else "ERROR",
            "upload_success" if result.get("success") else "upload_failed",
            (
                f"Upload job #{upload_job_id} completed"
                if result.get("success")
                else f"Upload job #{upload_job_id} failed: {result.get('message') or '-'}"
            ),
            {
                "upload_job_id": upload_context["id"],
                "upload_code_product": upload_context["code_product"],
                "upload_title": upload_context["title"],
                "upload_video_url": upload_context["video_url"],
                "result": result,
            },
        )
        return result

    def execute_upload_jobs(
        self,
        upload_job_ids: list[int],
        *,
        continue_on_error: bool = True,
    ) -> dict[str, Any]:
        ordered_ids = [int(upload_job_id) for upload_job_id in upload_job_ids if int(upload_job_id) > 0]
        results: list[dict[str, Any]] = []
        success_count = 0
        failure_count = 0
        stopped = False
        for upload_job_id in ordered_ids:
            result = self.execute_upload_job(upload_job_id)
            results.append({"upload_job_id": upload_job_id, "result": result})
            if result.get("success"):
                success_count += 1
            else:
                failure_count += 1
                if not continue_on_error:
                    stopped = True
                    break
        return {
            "success": failure_count == 0,
            "total": len(ordered_ids),
            "success_count": success_count,
            "failure_count": failure_count,
            "stopped": stopped,
            "continue_on_error": continue_on_error,
            "results": results,
        }

    def export_upload_jobs(self, upload_job_ids: list[int] | None = None) -> dict[str, Any]:
        selected_ids = {int(upload_job_id) for upload_job_id in (upload_job_ids or []) if int(upload_job_id) > 0}
        jobs = self.list_upload_jobs()
        if selected_ids:
            jobs = [job for job in jobs if int(job["id"]) in selected_ids]
        return {
            "schema_version": 1,
            "exported_at": datetime.now().isoformat(timespec="seconds"),
            "jobs": [self._export_job_payload(job) for job in jobs],
        }

    def import_upload_jobs(self, payload: dict[str, Any]) -> list[int]:
        jobs_payload = payload.get("jobs")
        if not isinstance(jobs_payload, list) or not jobs_payload:
            raise ValueError("Upload import must contain a non-empty jobs list")
        imported_ids: list[int] = []
        for index, item in enumerate(jobs_payload, start=1):
            if not isinstance(item, dict):
                raise ValueError(f"Imported upload job #{index} must be an object")
            imported_ids.append(
                self.save_upload_job(
                    None,
                    device_id=int(item.get("device_id") or 0),
                    device_platform_id=int(item.get("device_platform_id") or 0) or None,
                    account_id=int(item.get("account_id") or 0) or None,
                    workflow_id=int(item.get("workflow_id") or 0),
                    code_product=str(item.get("code_product") or ""),
                    link_product=str(item.get("link_product") or ""),
                    title=str(item.get("title") or ""),
                    description=str(item.get("description") or ""),
                    tags_text=self.tags_to_text(json.dumps(item.get("tags") or [], ensure_ascii=False)),
                    video_url=str(item.get("video_url") or ""),
                    cover_url=str(item.get("cover_url") or ""),
                    local_video_path=str(item.get("local_video_path") or ""),
                    metadata_text=json.dumps(item.get("metadata") or {}, ensure_ascii=False),
                )
            )
        return imported_ids

    def upload_summary(self) -> dict[str, Any]:
        jobs = self.list_upload_jobs()
        templates = self.list_upload_templates()
        status_counter = Counter(str(job.get("status") or "draft") for job in jobs)
        workflow_counter = Counter(
            str(job.get("workflow_name") or "")
            for job in jobs
            if str(job.get("workflow_name") or "").strip()
        )
        platform_counter = Counter(
            str(job.get("platform_name") or job.get("platform_key") or "")
            for job in jobs
            if str(job.get("platform_name") or job.get("platform_key") or "").strip()
        )
        account_counter = Counter(
            str(job.get("account_name") or "")
            for job in jobs
            if str(job.get("account_name") or "").strip()
        )
        return {
            "total_jobs": len(jobs),
            "draft_count": status_counter.get("draft", 0),
            "running_count": status_counter.get("running", 0),
            "success_count": status_counter.get("success", 0),
            "failed_count": status_counter.get("failed", 0),
            "template_count": len(templates),
            "top_workflow": workflow_counter.most_common(1)[0][0] if workflow_counter else "-",
            "top_platform": platform_counter.most_common(1)[0][0] if platform_counter else "-",
            "top_account": account_counter.most_common(1)[0][0] if account_counter else "-",
        }

    def _parse_tags_text(self, tags_text: str | None) -> list[str]:
        parts = re.split(r"[\r\n,;#]+", str(tags_text or ""))
        tags: list[str] = []
        seen: set[str] = set()
        for part in parts:
            normalized = str(part or "").strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            tags.append(normalized)
        return tags

    def tags_to_text(self, tags_json: str | None) -> str:
        try:
            parsed = json.loads(tags_json or "[]")
        except Exception:
            parsed = []
        if isinstance(parsed, list):
            return ", ".join(str(item).strip() for item in parsed if str(item).strip())
        return ""

    def metadata_to_text(self, metadata_json: str | None) -> str:
        try:
            parsed = json.loads(metadata_json or "{}")
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            return json.dumps(parsed, indent=2, ensure_ascii=False)
        return "{}"

    def _normalize_metadata_text(self, metadata_text: str | None) -> str:
        text = str(metadata_text or "").strip()
        if not text:
            return "{}"
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(f"Metadata must be valid JSON: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError("Metadata JSON must be an object")
        return json.dumps(parsed, ensure_ascii=False)

    def _build_upload_context(self, upload_job: dict[str, Any]) -> dict[str, Any]:
        try:
            tags = json.loads(upload_job.get("tags_json") or "[]")
        except Exception:
            tags = []
        if not isinstance(tags, list):
            tags = []
        try:
            metadata = json.loads(upload_job.get("metadata_json") or "{}")
        except Exception:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        return {
            "id": int(upload_job["id"]),
            "device_id": int(upload_job["device_id"]),
            "device_platform_id": int(upload_job.get("device_platform_id") or 0) or None,
            "account_id": int(upload_job.get("account_id") or 0) or None,
            "workflow_id": int(upload_job["workflow_id"]),
            "code_product": str(upload_job.get("code_product") or ""),
            "link_product": str(upload_job.get("link_product") or ""),
            "title": str(upload_job.get("title") or ""),
            "description": str(upload_job.get("description") or ""),
            "tags": [str(item) for item in tags],
            "video_url": str(upload_job.get("video_url") or ""),
            "cover_url": str(upload_job.get("cover_url") or ""),
            "local_video_path": str(upload_job.get("local_video_path") or ""),
            "metadata": metadata,
            "status": str(upload_job.get("status") or "draft"),
        }

    def _export_job_payload(self, upload_job: dict[str, Any]) -> dict[str, Any]:
        try:
            tags = json.loads(upload_job.get("tags_json") or "[]")
        except Exception:
            tags = []
        if not isinstance(tags, list):
            tags = []
        try:
            metadata = json.loads(upload_job.get("metadata_json") or "{}")
        except Exception:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        return {
            "device_id": int(upload_job["device_id"]),
            "device_platform_id": int(upload_job.get("device_platform_id") or 0) or None,
            "account_id": int(upload_job.get("account_id") or 0) or None,
            "workflow_id": int(upload_job["workflow_id"]),
            "code_product": str(upload_job.get("code_product") or ""),
            "link_product": str(upload_job.get("link_product") or ""),
            "title": str(upload_job.get("title") or ""),
            "description": str(upload_job.get("description") or ""),
            "tags": [str(item) for item in tags],
            "video_url": str(upload_job.get("video_url") or ""),
            "cover_url": str(upload_job.get("cover_url") or ""),
            "local_video_path": str(upload_job.get("local_video_path") or ""),
            "metadata": metadata,
        }


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
        account_service: AccountService | None = None,
        upload_service: UploadService | None = None,
    ) -> None:
        self.workflow_repository = workflow_repository
        self.device_repository = device_repository
        self.device_service = device_service
        self.log_service = log_service
        self.telemetry_service = telemetry_service
        self.watcher_service = watcher_service
        self.watcher_telemetry_service = watcher_telemetry_service
        self.account_service = account_service
        self.upload_service = upload_service

    def bind_upload_service(self, upload_service: UploadService) -> None:
        self.upload_service = upload_service

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

    def _executor_for_runtime(
        self,
        *,
        device,
        workflow: dict[str, Any],
        device_record: dict[str, Any],
        shared_context: dict[str, Any] | None = None,
    ) -> WorkflowExecutor:
        return WorkflowExecutor(
            device=device,
            workflow=workflow,
            device_record=device_record,
            log_service=self.log_service,
            telemetry_service=self.telemetry_service,
            watchers=self.watcher_service.resolve_active_watchers(int(workflow["id"]), int(device_record["id"])),
            watcher_telemetry_service=self.watcher_telemetry_service,
            switch_account_handler=self._execute_switch_account_step,
            run_for_each_account_handler=self._execute_run_for_each_account_step,
            prepare_upload_context_handler=self._execute_prepare_upload_context_step,
            shared_context=shared_context,
        )

    def _merge_runtime_context(self, executor: WorkflowExecutor, shared_context: dict[str, Any]) -> None:
        if not shared_context:
            return
        vars_payload = shared_context.get("vars")
        if isinstance(vars_payload, dict):
            executor.context["vars"].update(vars_payload)
        if "platform" in shared_context:
            executor.context["platform"] = shared_context["platform"]
        if "account" in shared_context:
            executor.context["account"] = shared_context["account"]
        if "upload" in shared_context:
            executor.context["upload"] = shared_context["upload"]

    def _run_nested_workflow(
        self,
        *,
        executor: WorkflowExecutor,
        workflow_id: int,
        shared_context: dict[str, Any],
        label: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if int(workflow_id) == int(executor.workflow["id"]):
            raise RuntimeError(f"{label} cannot target the current workflow")
        nested_workflow = self.workflow_repository.get_workflow(int(workflow_id))
        if not nested_workflow:
            raise RuntimeError(f"{label} not found")
        nested_steps = self.workflow_repository.list_steps(int(workflow_id))
        if not nested_steps:
            raise RuntimeError(f"{label} has no steps")

        validation_errors = self._validate_steps(nested_steps, include_structure=True)
        if validation_errors:
            raise RuntimeError(f"{label} validation failed: " + "; ".join(validation_errors))

        nested_executor = self._executor_for_runtime(
            device=executor.device,
            workflow=nested_workflow,
            device_record=executor.device_record,
            shared_context=shared_context,
        )
        summary = nested_executor.run(nested_steps)
        if summary.get("stopped_by_watcher"):
            raise RuntimeError(str(summary.get("stop_reason") or f"{label} stopped by watcher"))
        return nested_workflow, summary

    def _execute_switch_account_step(
        self,
        executor: WorkflowExecutor,
        parameters: dict[str, Any],
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.account_service:
            raise RuntimeError("Account service is not available")

        platform_key = str(parameters.get("platform_key", "") or "").strip().lower()
        account_id = int(parameters.get("account_id", 0) or 0) or None
        account_name = str(parameters.get("account_name", "") or "").strip() or None
        if not platform_key:
            raise RuntimeError("switch_account requires platform_key")
        if not account_id and not account_name:
            raise RuntimeError("switch_account requires account_id or account_name")

        device_platform, account = self.account_service.resolve_switch_target(
            int(executor.device_record["id"]),
            platform_key,
            account_id=account_id,
            account_name=account_name,
        )

        switch_workflow_id = int(device_platform.get("switch_workflow_id") or 0)
        if switch_workflow_id <= 0:
            raise RuntimeError("No switch workflow configured for this platform")

        if bool(parameters.get("launch_package_first")) and str(device_platform.get("package_name") or "").strip():
            executor.device.app_start(str(device_platform["package_name"]))

        shared_context, _ = self.account_service.resolve_runtime_context(
            int(executor.device_record["id"]),
            int(device_platform["id"]),
            account_id=int(account["id"]),
        )
        self._merge_runtime_context(executor, shared_context)

        switch_workflow, summary = self._run_nested_workflow(
            executor=executor,
            workflow_id=switch_workflow_id,
            shared_context=executor.context,
            label="Switch workflow",
        )

        self.account_service.set_current_account(int(device_platform["id"]), int(account["id"]))
        return {
            "platform_key": executor.context["platform"]["key"],
            "platform_name": executor.context["platform"]["name"],
            "account_id": executor.context["account"]["id"],
            "account_name": executor.context["account"]["display_name"],
            "switch_workflow_id": switch_workflow_id,
            "switch_workflow_name": switch_workflow.get("name") or "",
            "switch_summary": summary,
        }

    def _execute_run_for_each_account_step(
        self,
        executor: WorkflowExecutor,
        parameters: dict[str, Any],
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.account_service:
            raise RuntimeError("Account service is not available")

        platform_key = str(parameters.get("platform_key", "") or "").strip().lower()
        target_workflow_id = int(parameters.get("target_workflow_id", 0) or 0)
        only_enabled = bool(parameters.get("only_enabled", True))
        launch_package_first = bool(parameters.get("launch_package_first", True))
        continue_on_account_error = bool(parameters.get("continue_on_account_error", True))

        if not platform_key:
            raise RuntimeError("run_for_each_account requires platform_key")
        if target_workflow_id <= 0:
            raise RuntimeError("run_for_each_account requires target_workflow_id")

        device_platform, accounts = self.account_service.list_accounts_for_platform(
            int(executor.device_record["id"]),
            platform_key,
            only_enabled=only_enabled,
        )
        if not accounts:
            raise RuntimeError("No accounts available for this platform")
        if int(device_platform.get("switch_workflow_id") or 0) <= 0:
            raise RuntimeError("No switch workflow configured for this platform")

        account_results: list[dict[str, Any]] = []
        success_count = 0
        failure_count = 0

        for account_index, account in enumerate(accounts, start=1):
            executor.context["vars"]["foreach_account_index"] = account_index
            executor.context["vars"]["foreach_account_total"] = len(accounts)
            executor.context["vars"]["foreach_account_name"] = str(account.get("display_name") or "")
            executor.context["vars"]["foreach_account_id"] = int(account["id"])
            try:
                switch_result = self._execute_switch_account_step(
                    executor,
                    {
                        "platform_key": platform_key,
                        "account_id": int(account["id"]),
                        "launch_package_first": launch_package_first,
                    },
                    runtime,
                )
                target_workflow, target_summary = self._run_nested_workflow(
                    executor=executor,
                    workflow_id=target_workflow_id,
                    shared_context=executor.context,
                    label="Target workflow",
                )
                success_count += 1
                account_results.append(
                    {
                        "account_id": int(account["id"]),
                        "account_name": str(account.get("display_name") or ""),
                        "success": True,
                        "switch_result": switch_result,
                        "target_workflow_id": int(target_workflow["id"]),
                        "target_workflow_name": str(target_workflow.get("name") or ""),
                        "target_summary": target_summary,
                    }
                )
            except Exception as exc:
                failure_count += 1
                account_results.append(
                    {
                        "account_id": int(account["id"]),
                        "account_name": str(account.get("display_name") or ""),
                        "success": False,
                        "error": str(exc),
                    }
                )
                if not continue_on_account_error:
                    raise RuntimeError(f"Account '{account.get('display_name')}' failed: {exc}") from exc

        return {
            "platform_key": str(device_platform["platform_key"]),
            "platform_name": str(device_platform["platform_name"]),
            "target_workflow_id": target_workflow_id,
            "processed_accounts": len(accounts),
            "success_count": success_count,
            "failure_count": failure_count,
            "continue_on_account_error": continue_on_account_error,
            "account_results": account_results,
        }

    def _execute_prepare_upload_context_step(
        self,
        executor: WorkflowExecutor,
        parameters: dict[str, Any],
        runtime: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.upload_service:
            raise RuntimeError("Upload service is not available")

        upload_job_id = int(parameters.get("upload_job_id") or 0)
        if upload_job_id <= 0:
            upload_job_id = int(executor.context.get("vars", {}).get("upload_job_id") or 0)
        if upload_job_id <= 0:
            existing_upload = executor.context.get("upload", {})
            upload_job_id = int(existing_upload.get("id") or 0) if isinstance(existing_upload, dict) else 0
        if upload_job_id <= 0:
            raise RuntimeError("prepare_upload_context requires upload_job_id or existing upload context")

        upload_job = self.upload_service.get_upload_job(upload_job_id)
        if not upload_job:
            raise RuntimeError("Upload job not found")

        upload_context = self.upload_service._build_upload_context(upload_job)
        executor.context["upload"] = upload_context
        executor.context.setdefault("vars", {}).update(
            {
                "upload_job_id": upload_context["id"],
                "upload_code_product": upload_context["code_product"],
                "upload_link_product": upload_context["link_product"],
                "upload_title": upload_context["title"],
                "upload_description": upload_context["description"],
                "upload_tags": upload_context["tags"],
                "upload_video_url": upload_context["video_url"],
                "upload_cover_url": upload_context["cover_url"],
                "upload_local_video_path": upload_context["local_video_path"],
                "upload_metadata": upload_context["metadata"],
            }
        )
        return {
            "upload_job_id": upload_context["id"],
            "title": upload_context["title"],
            "code_product": upload_context["code_product"],
            "video_url": upload_context["video_url"],
            "local_video_path": upload_context["local_video_path"],
        }

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

    def execute_workflow(
        self,
        workflow_id: int,
        device_id: int,
        device_platform_id: int | None = None,
        account_id: int | None = None,
        use_current_account: bool = False,
        extra_context: dict[str, Any] | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._execute_workflow_run(
            workflow_id,
            device_id,
            step_ids=None,
            device_platform_id=device_platform_id,
            account_id=account_id,
            use_current_account=use_current_account,
            extra_context=extra_context,
            extra_metadata=extra_metadata,
        )

    def execute_step(
        self,
        workflow_id: int,
        step_id: int,
        device_id: int,
        device_platform_id: int | None = None,
        account_id: int | None = None,
        use_current_account: bool = False,
        extra_context: dict[str, Any] | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._execute_workflow_run(
            workflow_id,
            device_id,
            step_ids=[step_id],
            device_platform_id=device_platform_id,
            account_id=account_id,
            use_current_account=use_current_account,
            extra_context=extra_context,
            extra_metadata=extra_metadata,
        )

    def _execute_workflow_run(
        self,
        workflow_id: int,
        device_id: int,
        step_ids: list[int] | None = None,
        device_platform_id: int | None = None,
        account_id: int | None = None,
        use_current_account: bool = False,
        extra_context: dict[str, Any] | None = None,
        extra_metadata: dict[str, Any] | None = None,
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

        shared_context: dict[str, Any] | None = None
        context_metadata: dict[str, Any] = {}
        if self.account_service and device_platform_id:
            try:
                shared_context, context_metadata = self.account_service.resolve_runtime_context(
                    int(device_id),
                    int(device_platform_id),
                    account_id=account_id,
                    use_current_account=use_current_account,
                )
            except Exception as exc:
                return {"success": False, "message": str(exc)}

        if extra_context:
            shared_context = self._combine_shared_context(shared_context, extra_context)
        if extra_metadata:
            context_metadata.update(dict(extra_metadata))

        executor = self._executor_for_runtime(
            device=device,
            workflow=workflow,
            device_record=device_record,
            shared_context=shared_context,
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
                **context_metadata,
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
                    {**summary, **context_metadata},
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
                {**summary, **context_metadata},
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
                    **context_metadata,
                },
            )
            return {"success": False, "message": str(exc)}

    def _combine_shared_context(
        self,
        base_context: dict[str, Any] | None,
        extra_context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for source in (base_context or {}, extra_context or {}):
            for key, value in source.items():
                if key == "vars" and isinstance(value, dict):
                    merged.setdefault("vars", {})
                    merged["vars"].update(value)
                else:
                    merged[key] = value
        return merged


class SchedulerService:
    SCHEDULE_TYPES = {"once", "interval", "daily"}
    TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        schedule_repository: ScheduleRepository,
        schedule_run_repository: ScheduleRunRepository,
        workflow_repository: WorkflowRepository,
        device_repository: DeviceRepository,
        workflow_service: WorkflowService,
        log_service: LogService,
        account_service: AccountService | None = None,
        schedule_group_repository: ScheduleGroupRepository | None = None,
    ) -> None:
        self.schedule_repository = schedule_repository
        self.schedule_run_repository = schedule_run_repository
        self.workflow_repository = workflow_repository
        self.device_repository = device_repository
        self.workflow_service = workflow_service
        self.log_service = log_service
        self.account_service = account_service
        self.schedule_group_repository = schedule_group_repository

    def list_schedules(self) -> list[dict[str, Any]]:
        schedules = self.schedule_repository.list_schedules()
        for schedule in schedules:
            schedule["schedule_config"] = self._load_schedule_config(schedule.get("schedule_json"))
            schedule["schedule_summary"] = self.describe_schedule(
                str(schedule.get("schedule_type") or ""),
                schedule["schedule_config"],
            )
        return schedules

    def get_schedule(self, schedule_id: int) -> dict[str, Any] | None:
        schedule = self.schedule_repository.get_schedule(schedule_id)
        if not schedule:
            return None
        schedule["schedule_config"] = self._load_schedule_config(schedule.get("schedule_json"))
        schedule["schedule_summary"] = self.describe_schedule(
            str(schedule.get("schedule_type") or ""),
            schedule["schedule_config"],
        )
        return schedule

    def list_runs(self, schedule_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.schedule_run_repository.list_runs(schedule_id=schedule_id, limit=limit)

    def list_groups(self) -> list[dict[str, Any]]:
        if not self.schedule_group_repository:
            return []
        return self.schedule_group_repository.list_groups()

    def get_group(self, group_id: int) -> dict[str, Any] | None:
        if not self.schedule_group_repository:
            return None
        return self.schedule_group_repository.get_group(group_id)

    def list_due_schedules(self, now: datetime | None = None) -> list[dict[str, Any]]:
        due_schedules = self.schedule_repository.due_schedules(self._format_timestamp(now or datetime.now().astimezone()))
        for schedule in due_schedules:
            schedule["schedule_config"] = self._load_schedule_config(schedule.get("schedule_json"))
        return due_schedules

    def save_schedule(
        self,
        schedule_id: int | None,
        name: str,
        workflow_id: int,
        device_id: int,
        device_platform_id: int | None,
        account_id: int | None,
        use_current_account: bool,
        schedule_type: str,
        schedule_config: dict[str, Any],
        is_enabled: bool = True,
        schedule_group_id: int | None = None,
        priority: int = 100,
    ) -> int:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("Schedule name is required")
        normalized_schedule_type = str(schedule_type or "").strip().lower()
        if normalized_schedule_type not in self.SCHEDULE_TYPES:
            raise ValueError("Unsupported schedule type")
        workflow = self.workflow_repository.get_workflow(int(workflow_id))
        if not workflow:
            raise ValueError("Workflow not found")
        device = self.device_repository.get_device(int(device_id))
        if not device:
            raise ValueError("Device not found")

        normalized_schedule_group_id = int(schedule_group_id or 0) or None
        normalized_device_platform_id = int(device_platform_id or 0) or None
        normalized_account_id = int(account_id or 0) or None
        normalized_priority = max(1, min(999, int(priority or 100)))
        if normalized_schedule_group_id:
            if not self.schedule_group_repository:
                raise ValueError("Schedule groups are not available")
            group = self.schedule_group_repository.get_group(normalized_schedule_group_id)
            if not group:
                raise ValueError("Schedule group not found")
        if normalized_device_platform_id and self.account_service:
            platform = self.account_service.get_device_platform(normalized_device_platform_id)
            if not platform or int(platform.get("device_id") or device_id) != int(device_id):
                raise ValueError("Platform not found on this device")
            if normalized_account_id:
                account = self.account_service.get_account(normalized_account_id)
                if not account or int(account.get("device_platform_id") or 0) != normalized_device_platform_id:
                    raise ValueError("Account not found for this platform")
        elif normalized_account_id:
            raise ValueError("Account requires a selected platform")

        validated_config = self._validate_schedule_config(normalized_schedule_type, schedule_config)
        next_run_at = (
            self._format_timestamp(self._compute_next_run(normalized_schedule_type, validated_config))
            if is_enabled
            else None
        )
        return self.schedule_repository.upsert_schedule(
            schedule_id=schedule_id,
            name=normalized_name,
            workflow_id=int(workflow_id),
            device_id=int(device_id),
            device_platform_id=normalized_device_platform_id,
            account_id=None if use_current_account else normalized_account_id,
            use_current_account=bool(use_current_account),
            schedule_type=normalized_schedule_type,
            schedule_json=json.dumps(validated_config, ensure_ascii=False, indent=2),
            next_run_at=next_run_at,
            is_enabled=is_enabled,
        )

    def delete_schedule(self, schedule_id: int) -> None:
        self.schedule_repository.delete_schedule(schedule_id)

    def execute_schedule(
        self,
        schedule_id: int,
        *,
        trigger_source: str = "manual",
        advance_schedule: bool = False,
    ) -> dict[str, Any]:
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return {"success": False, "message": "Schedule not found"}
        if not bool(schedule.get("is_enabled", 1)) and trigger_source != "manual":
            return {"success": False, "message": "Schedule is disabled"}

        started_at_dt = datetime.now().astimezone()
        started_at = self._format_timestamp(started_at_dt)
        self.log_service.add(
            int(schedule["workflow_id"]),
            int(schedule["device_id"]),
            "INFO",
            "schedule_started",
            f"Started schedule '{schedule['name']}'",
            {
                "schedule_id": int(schedule["id"]),
                "schedule_name": str(schedule["name"]),
                "trigger_source": trigger_source,
            },
        )

        result = self.workflow_service.execute_workflow(
            int(schedule["workflow_id"]),
            int(schedule["device_id"]),
            device_platform_id=int(schedule.get("device_platform_id") or 0) or None,
            account_id=int(schedule.get("account_id") or 0) or None,
            use_current_account=bool(schedule.get("use_current_account", 0)),
        )

        finished_at_dt = datetime.now().astimezone()
        finished_at = self._format_timestamp(finished_at_dt)
        was_successful = bool(result.get("success"))
        run_status = "success" if was_successful else "failed"
        next_run_at: str | None = schedule.get("next_run_at")
        schedule_enabled = bool(schedule.get("is_enabled", 1))

        if advance_schedule:
            computed_next_run = self._compute_next_run(
                str(schedule.get("schedule_type") or ""),
                schedule.get("schedule_config") or self._load_schedule_config(schedule.get("schedule_json")),
                reference=finished_at_dt,
            )
            if str(schedule.get("schedule_type") or "") == "once":
                schedule_enabled = False
                next_run_at = None
            else:
                next_run_at = self._format_timestamp(computed_next_run) if computed_next_run else None

        self.schedule_repository.update_schedule_state(
            int(schedule["id"]),
            next_run_at=next_run_at,
            last_run_at=finished_at,
            last_status=run_status,
            is_enabled=schedule_enabled,
        )
        self.schedule_run_repository.add_run(
            schedule_id=int(schedule["id"]),
            workflow_id=int(schedule["workflow_id"]),
            device_id=int(schedule["device_id"]),
            trigger_source=str(trigger_source),
            status=run_status,
            message=str(result.get("message") or ""),
            metadata={
                "schedule_name": str(schedule["name"]),
                "advance_schedule": bool(advance_schedule),
                "next_run_at": next_run_at,
            },
            started_at=started_at,
            finished_at=finished_at,
        )
        self.log_service.add(
            int(schedule["workflow_id"]),
            int(schedule["device_id"]),
            "INFO" if was_successful else "ERROR",
            "schedule_success" if was_successful else "schedule_failed",
            f"Schedule '{schedule['name']}' {'completed' if was_successful else 'failed'}",
            {
                "schedule_id": int(schedule["id"]),
                "schedule_name": str(schedule["name"]),
                "trigger_source": trigger_source,
                "next_run_at": next_run_at,
                "result_message": str(result.get("message") or ""),
            },
        )
        return {
            "success": was_successful,
            "message": str(result.get("message") or ""),
            "schedule_id": int(schedule["id"]),
            "schedule_name": str(schedule["name"]),
            "run_status": run_status,
            "trigger_source": trigger_source,
            "next_run_at": next_run_at,
        }

    def describe_schedule(self, schedule_type: str, schedule_config: dict[str, Any]) -> str:
        normalized_schedule_type = str(schedule_type or "").strip().lower()
        if normalized_schedule_type == "once":
            return f"Once at {schedule_config.get('run_at', '-')}"
        if normalized_schedule_type == "interval":
            minutes = int(schedule_config.get("every_minutes") or 0)
            return f"Every {minutes} minute{'s' if minutes != 1 else ''}"
        if normalized_schedule_type == "daily":
            return f"Daily at {schedule_config.get('time', '-')}"
        return normalized_schedule_type or "-"

    def _load_schedule_config(self, payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            return dict(payload)
        try:
            loaded = json.loads(payload or "{}")
        except Exception:
            loaded = {}
        return loaded if isinstance(loaded, dict) else {}

    def _validate_schedule_config(self, schedule_type: str, schedule_config: dict[str, Any]) -> dict[str, Any]:
        config = dict(schedule_config or {})
        if schedule_type == "once":
            run_at = str(config.get("run_at") or "").strip()
            if not run_at:
                raise ValueError("Run once schedule requires run_at")
            parsed = self._parse_timestamp(run_at)
            return {"run_at": self._format_timestamp(parsed)}
        if schedule_type == "interval":
            every_minutes = int(config.get("every_minutes") or 0)
            if every_minutes <= 0:
                raise ValueError("Interval schedule requires every_minutes > 0")
            return {"every_minutes": every_minutes}
        if schedule_type == "daily":
            time_text = str(config.get("time") or "").strip()
            try:
                parsed_time = datetime.strptime(time_text, "%H:%M")
            except ValueError as exc:
                raise ValueError("Daily schedule requires time in HH:MM") from exc
            return {"time": parsed_time.strftime("%H:%M")}
        raise ValueError("Unsupported schedule type")

    def _compute_next_run(
        self,
        schedule_type: str,
        schedule_config: dict[str, Any],
        *,
        reference: datetime | None = None,
    ) -> datetime | None:
        now = reference or datetime.now().astimezone()
        if schedule_type == "once":
            return self._parse_timestamp(str(schedule_config.get("run_at") or ""))
        if schedule_type == "interval":
            every_minutes = int(schedule_config.get("every_minutes") or 0)
            return now + timedelta(minutes=every_minutes)
        if schedule_type == "daily":
            time_text = str(schedule_config.get("time") or "00:00")
            scheduled_time = datetime.strptime(time_text, "%H:%M")
            candidate = now.replace(
                hour=scheduled_time.hour,
                minute=scheduled_time.minute,
                second=0,
                microsecond=0,
            )
            if candidate <= now:
                candidate += timedelta(days=1)
            return candidate
        return None

    def _parse_timestamp(self, value: str) -> datetime:
        for fmt in (self.TIMESTAMP_FORMAT, "%Y-%m-%d %H:%M"):
            try:
                parsed = datetime.strptime(value, fmt)
                return parsed.astimezone() if parsed.tzinfo else parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
            except ValueError:
                continue
        raise ValueError("Timestamp must be in YYYY-MM-DD HH:MM or YYYY-MM-DD HH:MM:SS format")

    def _format_timestamp(self, value: datetime) -> str:
        localized = value if value.tzinfo else value.replace(tzinfo=datetime.now().astimezone().tzinfo)
        return localized.astimezone().strftime(self.TIMESTAMP_FORMAT)


class SchedulerService:
    SCHEDULE_TYPES = {"once", "interval", "daily", "weekly"}
    MISSED_RUN_POLICIES = {"run_immediately", "skip", "reschedule_next"}
    OVERLAP_POLICIES = {"skip_if_running", "queue_next"}
    TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
    WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    def __init__(
        self,
        schedule_repository: ScheduleRepository,
        schedule_run_repository: ScheduleRunRepository,
        workflow_repository: WorkflowRepository,
        device_repository: DeviceRepository,
        workflow_service: WorkflowService,
        log_service: LogService,
        account_service: AccountService | None = None,
        schedule_group_repository: ScheduleGroupRepository | None = None,
    ) -> None:
        self.schedule_repository = schedule_repository
        self.schedule_run_repository = schedule_run_repository
        self.workflow_repository = workflow_repository
        self.device_repository = device_repository
        self.workflow_service = workflow_service
        self.log_service = log_service
        self.account_service = account_service
        self.schedule_group_repository = schedule_group_repository

    def list_schedules(self) -> list[dict[str, Any]]:
        schedules = self.schedule_repository.list_schedules()
        for schedule in schedules:
            schedule["schedule_config"] = self._load_schedule_config(schedule.get("schedule_json"))
            schedule["schedule_summary"] = self.describe_schedule(
                str(schedule.get("schedule_type") or ""),
                schedule["schedule_config"],
            )
        return schedules

    def get_schedule(self, schedule_id: int) -> dict[str, Any] | None:
        schedule = self.schedule_repository.get_schedule(schedule_id)
        if not schedule:
            return None
        schedule["schedule_config"] = self._load_schedule_config(schedule.get("schedule_json"))
        schedule["schedule_summary"] = self.describe_schedule(
            str(schedule.get("schedule_type") or ""),
            schedule["schedule_config"],
        )
        return schedule

    def list_runs(self, schedule_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.schedule_run_repository.list_runs(schedule_id=schedule_id, limit=limit)

    def list_groups(self) -> list[dict[str, Any]]:
        if not self.schedule_group_repository:
            return []
        return self.schedule_group_repository.list_groups()

    def get_group(self, group_id: int) -> dict[str, Any] | None:
        if not self.schedule_group_repository:
            return None
        return self.schedule_group_repository.get_group(group_id)

    def list_due_schedules(self, now: datetime | None = None) -> list[dict[str, Any]]:
        due_schedules = self.schedule_repository.due_schedules(self._format_timestamp(now or datetime.now().astimezone()))
        for schedule in due_schedules:
            schedule["schedule_config"] = self._load_schedule_config(schedule.get("schedule_json"))
            schedule["schedule_summary"] = self.describe_schedule(
                str(schedule.get("schedule_type") or ""),
                schedule["schedule_config"],
            )
        return due_schedules

    def save_schedule(
        self,
        schedule_id: int | None,
        name: str,
        workflow_id: int,
        device_id: int,
        device_platform_id: int | None,
        account_id: int | None,
        use_current_account: bool,
        schedule_type: str,
        schedule_config: dict[str, Any],
        is_enabled: bool = True,
        schedule_group_id: int | None = None,
        priority: int = 100,
    ) -> int:
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("Schedule name is required")
        normalized_schedule_type = str(schedule_type or "").strip().lower()
        if normalized_schedule_type not in self.SCHEDULE_TYPES:
            raise ValueError("Unsupported schedule type")
        if not self.workflow_repository.get_workflow(int(workflow_id)):
            raise ValueError("Workflow not found")
        if not self.device_repository.get_device(int(device_id)):
            raise ValueError("Device not found")

        normalized_schedule_group_id = int(schedule_group_id or 0) or None
        normalized_device_platform_id = int(device_platform_id or 0) or None
        normalized_account_id = int(account_id or 0) or None
        normalized_priority = max(1, min(999, int(priority or 100)))
        if normalized_schedule_group_id:
            if not self.schedule_group_repository:
                raise ValueError("Schedule groups are not available")
            group = self.schedule_group_repository.get_group(normalized_schedule_group_id)
            if not group:
                raise ValueError("Schedule group not found")
        if normalized_device_platform_id and self.account_service:
            platform = self.account_service.get_device_platform(normalized_device_platform_id)
            if not platform or int(platform.get("device_id") or device_id) != int(device_id):
                raise ValueError("Platform not found on this device")
            if normalized_account_id:
                account = self.account_service.get_account(normalized_account_id)
                if not account or int(account.get("device_platform_id") or 0) != normalized_device_platform_id:
                    raise ValueError("Account not found for this platform")
        elif normalized_account_id:
            raise ValueError("Account requires a selected platform")

        validated_config = self._validate_schedule_config(normalized_schedule_type, schedule_config)
        next_run_dt = self._compute_next_run(normalized_schedule_type, validated_config) if is_enabled else None
        return self.schedule_repository.upsert_schedule(
            schedule_id=schedule_id,
            name=normalized_name,
            workflow_id=int(workflow_id),
            device_id=int(device_id),
            schedule_group_id=normalized_schedule_group_id,
            device_platform_id=normalized_device_platform_id,
            account_id=None if use_current_account else normalized_account_id,
            use_current_account=bool(use_current_account),
            schedule_type=normalized_schedule_type,
            schedule_json=json.dumps(validated_config, ensure_ascii=False, indent=2),
            next_run_at=self._format_timestamp(next_run_dt) if next_run_dt else None,
            priority=normalized_priority,
            is_enabled=is_enabled,
        )

    def save_group(
        self,
        group_id: int | None,
        name: str,
        description: str,
        is_enabled: bool = True,
    ) -> int:
        if not self.schedule_group_repository:
            raise ValueError("Schedule groups are not available")
        normalized_name = str(name or "").strip()
        if not normalized_name:
            raise ValueError("Group name is required")
        duplicate = next(
            (
                group
                for group in self.schedule_group_repository.list_groups()
                if str(group.get("name") or "").strip().casefold() == normalized_name.casefold()
                and int(group.get("id") or 0) != int(group_id or 0)
            ),
            None,
        )
        if duplicate:
            raise ValueError("Group name already exists")
        return self.schedule_group_repository.upsert_group(
            group_id,
            normalized_name,
            str(description or "").strip(),
            is_enabled=is_enabled,
        )

    def delete_group(self, group_id: int) -> None:
        if not self.schedule_group_repository:
            raise ValueError("Schedule groups are not available")
        self.schedule_group_repository.delete_group(group_id)

    def set_group_enabled(self, group_id: int, enabled: bool) -> dict[str, Any]:
        if not self.schedule_group_repository:
            raise ValueError("Schedule groups are not available")
        group = self.schedule_group_repository.get_group(group_id)
        if not group:
            raise ValueError("Schedule group not found")
        self.schedule_group_repository.set_group_enabled(group_id, enabled)
        return self.schedule_group_repository.get_group(group_id) or {}

    def delete_schedule(self, schedule_id: int) -> None:
        self.schedule_repository.delete_schedule(schedule_id)

    def set_schedule_enabled(self, schedule_id: int, enabled: bool) -> dict[str, Any]:
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            raise ValueError("Schedule not found")
        config = schedule["schedule_config"]
        next_run_dt = self._compute_next_run(str(schedule["schedule_type"]), config) if enabled else None
        self.schedule_repository.update_schedule_state(
            schedule_id,
            next_run_at=self._format_timestamp(next_run_dt) if next_run_dt else None,
            last_status="idle" if enabled else "paused",
            is_enabled=enabled,
        )
        self.log_service.add(
            int(schedule["workflow_id"]),
            int(schedule["device_id"]),
            "INFO",
            "schedule_resumed" if enabled else "schedule_paused",
            f"Schedule '{schedule['name']}' {'resumed' if enabled else 'paused'}",
            {"schedule_id": schedule_id, "schedule_name": str(schedule["name"])},
        )
        updated = self.get_schedule(schedule_id)
        if not updated:
            raise ValueError("Schedule not found after update")
        return updated

    def resolve_run_request(
        self,
        schedule_id: int,
        *,
        trigger_source: str,
        advance_schedule: bool,
        startup_recovery: bool,
        is_running: bool,
    ) -> dict[str, Any]:
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return {"action": "skip", "reason": "schedule_not_found"}
        config = schedule["schedule_config"]
        if trigger_source != "manual" and not bool(schedule.get("is_enabled", 1)):
            return {"action": "skip", "reason": "disabled", "schedule": schedule}
        if trigger_source != "manual" and schedule.get("schedule_group_id") and not bool(schedule.get("group_is_enabled", 1)):
            return {"action": "skip", "reason": "group_disabled", "schedule": schedule}

        effective_trigger_source = trigger_source
        if trigger_source == "timer" and startup_recovery:
            missed_policy = str(config.get("missed_run_policy") or "run_immediately")
            if missed_policy == "skip":
                self._record_non_run(
                    schedule,
                    status="missed_skipped",
                    trigger_source="missed_run",
                    message=f"Skipped missed schedule '{schedule['name']}' on startup",
                    advance_schedule=True,
                )
                return {"action": "skip", "reason": "missed_skipped", "schedule": schedule}
            if missed_policy == "reschedule_next":
                self._record_non_run(
                    schedule,
                    status="missed_rescheduled",
                    trigger_source="missed_run",
                    message=f"Rescheduled missed schedule '{schedule['name']}' on startup",
                    advance_schedule=True,
                )
                return {"action": "skip", "reason": "missed_rescheduled", "schedule": schedule}
            effective_trigger_source = "missed_run"

        if is_running:
            overlap_policy = str(config.get("overlap_policy") or "skip_if_running")
            if overlap_policy == "queue_next":
                self.log_service.add(
                    int(schedule["workflow_id"]),
                    int(schedule["device_id"]),
                    "INFO",
                    "schedule_queued",
                    f"Queued schedule '{schedule['name']}' while previous run is still active",
                    {"schedule_id": int(schedule["id"]), "schedule_name": str(schedule["name"]), "trigger_source": effective_trigger_source},
                )
                return {
                    "action": "queue",
                    "schedule": schedule,
                    "trigger_source": effective_trigger_source,
                    "advance_schedule": advance_schedule,
                }
            self._record_non_run(
                schedule,
                status="skipped_overlap",
                trigger_source=effective_trigger_source,
                message=f"Skipped schedule '{schedule['name']}' because a previous run is still active",
                advance_schedule=advance_schedule,
            )
            return {"action": "skip", "reason": "overlap", "schedule": schedule}

        return {
            "action": "run",
            "schedule": schedule,
            "trigger_source": effective_trigger_source,
            "advance_schedule": advance_schedule,
        }

    def dashboard_snapshot(
        self,
        *,
        running_schedule_ids: set[int] | None = None,
        queued_schedule_ids: set[int] | None = None,
        next_limit: int = 5,
        failure_limit: int = 5,
    ) -> dict[str, Any]:
        schedules = self.list_schedules()
        running_ids = {int(schedule_id) for schedule_id in (running_schedule_ids or set())}
        queued_ids = {int(schedule_id) for schedule_id in (queued_schedule_ids or set())}
        due_now = self.list_due_schedules()
        due_id_set = {int(schedule["id"]) for schedule in due_now}

        running_rows = [schedule for schedule in schedules if int(schedule["id"]) in running_ids]
        queued_rows = [schedule for schedule in schedules if int(schedule["id"]) in queued_ids]
        next_runs = sorted(
            [schedule for schedule in schedules if schedule.get("next_run_at")],
            key=lambda item: (str(item.get("next_run_at") or ""), int(item.get("priority") or 100), int(item["id"])),
        )[: max(1, int(next_limit))]
        recent_failures = self.schedule_run_repository.list_recent_failed_runs(limit=max(1, int(failure_limit)))

        group_rows: list[dict[str, Any]] = []
        groups_by_id = {int(group["id"]): dict(group) for group in self.list_groups()}
        for group_id, group in groups_by_id.items():
            members = [schedule for schedule in schedules if int(schedule.get("schedule_group_id") or 0) == group_id]
            group_rows.append(
                {
                    **group,
                    "running_count": sum(1 for schedule in members if int(schedule["id"]) in running_ids),
                    "queued_count": sum(1 for schedule in members if int(schedule["id"]) in queued_ids),
                    "due_count": sum(1 for schedule in members if int(schedule["id"]) in due_id_set),
                }
            )
        group_rows.sort(key=lambda item: (str(item.get("name") or "").casefold(), int(item.get("id") or 0)))

        return {
            "counts": {
                "total": len(schedules),
                "enabled": sum(1 for schedule in schedules if bool(schedule.get("is_enabled", 1))),
                "paused": sum(1 for schedule in schedules if not bool(schedule.get("is_enabled", 1))),
                "running": len(running_rows),
                "queued": len(queued_rows),
                "due_now": len(due_now),
                "groups": len(group_rows),
            },
            "running": running_rows,
            "queued": queued_rows,
            "next_runs": next_runs,
            "recent_failures": recent_failures,
            "groups": group_rows,
        }

    def execute_schedule(
        self,
        schedule_id: int,
        *,
        trigger_source: str = "manual",
        advance_schedule: bool = False,
    ) -> dict[str, Any]:
        schedule = self.get_schedule(schedule_id)
        if not schedule:
            return {"success": False, "message": "Schedule not found"}
        if not bool(schedule.get("is_enabled", 1)) and trigger_source != "manual":
            return {"success": False, "message": "Schedule is disabled"}

        config = schedule["schedule_config"]
        started_at_dt = datetime.now().astimezone()
        started_at = self._format_timestamp(started_at_dt)
        self.log_service.add(
            int(schedule["workflow_id"]),
            int(schedule["device_id"]),
            "INFO",
            "schedule_started",
            f"Started schedule '{schedule['name']}'",
            {"schedule_id": int(schedule["id"]), "schedule_name": str(schedule["name"]), "trigger_source": trigger_source},
        )

        retry_limit = int(config.get("retry_on_failure") or 0)
        retry_delay_seconds = int(config.get("retry_delay_seconds") or 0)
        attempts = 0
        result: dict[str, Any] = {"success": False, "message": "Schedule did not execute"}
        while True:
            attempts += 1
            result = self.workflow_service.execute_workflow(
                int(schedule["workflow_id"]),
                int(schedule["device_id"]),
                device_platform_id=int(schedule.get("device_platform_id") or 0) or None,
                account_id=int(schedule.get("account_id") or 0) or None,
                use_current_account=bool(schedule.get("use_current_account", 0)),
            )
            if bool(result.get("success")) or attempts > retry_limit:
                break
            self.log_service.add(
                int(schedule["workflow_id"]),
                int(schedule["device_id"]),
                "WARNING",
                "schedule_retry",
                f"Retrying schedule '{schedule['name']}' after failure",
                {
                    "schedule_id": int(schedule["id"]),
                    "schedule_name": str(schedule["name"]),
                    "trigger_source": trigger_source,
                    "attempt": attempts,
                    "retry_limit": retry_limit,
                    "retry_delay_seconds": retry_delay_seconds,
                },
            )
            if retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)

        finished_at_dt = datetime.now().astimezone()
        finished_at = self._format_timestamp(finished_at_dt)
        was_successful = bool(result.get("success"))
        run_status = "success" if was_successful else "failed"
        next_run_at = schedule.get("next_run_at")
        schedule_enabled = bool(schedule.get("is_enabled", 1))

        if advance_schedule:
            computed_next_run = self._compute_next_run(
                str(schedule.get("schedule_type") or ""),
                config,
                reference=finished_at_dt,
            )
            if str(schedule.get("schedule_type") or "") == "once":
                schedule_enabled = False
                next_run_at = None
            else:
                next_run_at = self._format_timestamp(computed_next_run) if computed_next_run else None

        self.schedule_repository.update_schedule_state(
            int(schedule["id"]),
            next_run_at=next_run_at,
            last_run_at=finished_at,
            last_status=run_status,
            is_enabled=schedule_enabled,
        )
        self.schedule_run_repository.add_run(
            schedule_id=int(schedule["id"]),
            workflow_id=int(schedule["workflow_id"]),
            device_id=int(schedule["device_id"]),
            trigger_source=str(trigger_source),
            status=run_status,
            message=str(result.get("message") or ""),
            metadata={
                "schedule_name": str(schedule["name"]),
                "advance_schedule": bool(advance_schedule),
                "attempts": attempts,
                "retry_limit": retry_limit,
                "next_run_at": next_run_at,
            },
            started_at=started_at,
            finished_at=finished_at,
        )
        self.log_service.add(
            int(schedule["workflow_id"]),
            int(schedule["device_id"]),
            "INFO" if was_successful else "ERROR",
            "schedule_success" if was_successful else "schedule_failed",
            f"Schedule '{schedule['name']}' {'completed' if was_successful else 'failed'}",
            {
                "schedule_id": int(schedule["id"]),
                "schedule_name": str(schedule["name"]),
                "trigger_source": trigger_source,
                "attempts": attempts,
                "next_run_at": next_run_at,
                "result_message": str(result.get("message") or ""),
            },
        )
        return {
            "success": was_successful,
            "message": str(result.get("message") or ""),
            "schedule_id": int(schedule["id"]),
            "schedule_name": str(schedule["name"]),
            "run_status": run_status,
            "trigger_source": trigger_source,
            "attempts": attempts,
            "next_run_at": next_run_at,
        }

    def describe_schedule(self, schedule_type: str, schedule_config: dict[str, Any]) -> str:
        normalized_schedule_type = str(schedule_type or "").strip().lower()
        if normalized_schedule_type == "once":
            summary = f"Once at {schedule_config.get('run_at', '-')}"
        elif normalized_schedule_type == "interval":
            minutes = int(schedule_config.get("every_minutes") or 0)
            summary = f"Every {minutes} minute{'s' if minutes != 1 else ''}"
        elif normalized_schedule_type == "daily":
            summary = f"Daily at {schedule_config.get('time', '-')}"
        elif normalized_schedule_type == "weekly":
            weekdays = [self.WEEKDAY_NAMES[int(day)] for day in schedule_config.get("weekdays", []) if 0 <= int(day) <= 6]
            summary = f"Weekly {', '.join(weekdays) or '-'} at {schedule_config.get('time', '-')}"
        else:
            summary = normalized_schedule_type or "-"
        if bool(schedule_config.get("active_window_enabled")):
            summary += f" / Window {schedule_config.get('window_start', '-')} - {schedule_config.get('window_end', '-')}"
        jitter_seconds = int(schedule_config.get("jitter_seconds") or 0)
        if jitter_seconds > 0:
            summary += f" / +{jitter_seconds}s jitter"
        return summary

    def _load_schedule_config(self, payload: Any) -> dict[str, Any]:
        if isinstance(payload, dict):
            return dict(payload)
        try:
            loaded = json.loads(payload or "{}")
        except Exception:
            loaded = {}
        return loaded if isinstance(loaded, dict) else {}

    def _validate_schedule_config(self, schedule_type: str, schedule_config: dict[str, Any]) -> dict[str, Any]:
        config = dict(schedule_config or {})
        validated: dict[str, Any] = {
            "jitter_seconds": max(0, int(config.get("jitter_seconds") or 0)),
            "missed_run_policy": str(config.get("missed_run_policy") or "run_immediately"),
            "overlap_policy": str(config.get("overlap_policy") or "skip_if_running"),
            "retry_on_failure": max(0, int(config.get("retry_on_failure") or 0)),
            "retry_delay_seconds": max(0, int(config.get("retry_delay_seconds") or 0)),
            "active_window_enabled": bool(config.get("active_window_enabled", False)),
            "window_start": str(config.get("window_start") or "09:00"),
            "window_end": str(config.get("window_end") or "18:00"),
        }
        if validated["missed_run_policy"] not in self.MISSED_RUN_POLICIES:
            raise ValueError("Invalid missed run policy")
        if validated["overlap_policy"] not in self.OVERLAP_POLICIES:
            raise ValueError("Invalid overlap policy")
        if validated["active_window_enabled"]:
            start_minutes, end_minutes = self._parse_clock_range(validated["window_start"], validated["window_end"])
            if end_minutes <= start_minutes:
                raise ValueError("Active window end time must be after start time")

        if schedule_type == "once":
            run_at = str(config.get("run_at") or "").strip()
            if not run_at:
                raise ValueError("Run once schedule requires run_at")
            validated["run_at"] = self._format_timestamp(self._parse_timestamp(run_at))
            return validated

        if schedule_type == "interval":
            every_minutes = int(config.get("every_minutes") or 0)
            if every_minutes <= 0:
                raise ValueError("Interval schedule requires every_minutes > 0")
            validated["every_minutes"] = every_minutes
            return validated

        time_text = str(config.get("time") or "").strip()
        try:
            parsed_time = datetime.strptime(time_text, "%H:%M")
        except ValueError as exc:
            raise ValueError(f"{schedule_type.title()} schedule requires time in HH:MM") from exc
        validated["time"] = parsed_time.strftime("%H:%M")

        if schedule_type == "weekly":
            weekdays = sorted({int(day) for day in config.get("weekdays", []) if 0 <= int(day) <= 6})
            if not weekdays:
                raise ValueError("Weekly schedule requires at least one weekday")
            validated["weekdays"] = weekdays

        if validated["active_window_enabled"]:
            start_minutes, end_minutes = self._parse_clock_range(validated["window_start"], validated["window_end"])
            scheduled_minutes = parsed_time.hour * 60 + parsed_time.minute
            if scheduled_minutes < start_minutes or scheduled_minutes > end_minutes:
                raise ValueError(f"{schedule_type.title()} schedule time must be inside the active window")
            if validated["jitter_seconds"] > max(0, (end_minutes - scheduled_minutes) * 60):
                raise ValueError("Jitter pushes scheduled time outside the active window")
        return validated

    def _compute_next_run(
        self,
        schedule_type: str,
        schedule_config: dict[str, Any],
        *,
        reference: datetime | None = None,
    ) -> datetime | None:
        now = reference or datetime.now().astimezone()
        normalized_schedule_type = str(schedule_type or "").strip().lower()
        if normalized_schedule_type == "once":
            return self._parse_timestamp(str(schedule_config.get("run_at") or ""))
        if normalized_schedule_type == "interval":
            candidate = now + timedelta(minutes=int(schedule_config.get("every_minutes") or 0))
            return self._adjust_to_active_window(self._apply_jitter(candidate, schedule_config), schedule_config)
        if normalized_schedule_type == "daily":
            candidate = self._next_daily_candidate(now, str(schedule_config.get("time") or "00:00"))
            return self._adjust_to_active_window(self._apply_jitter(candidate, schedule_config), schedule_config)
        if normalized_schedule_type == "weekly":
            candidate = self._next_weekly_candidate(now, str(schedule_config.get("time") or "00:00"), list(schedule_config.get("weekdays") or []))
            return self._adjust_to_active_window(self._apply_jitter(candidate, schedule_config), schedule_config)
        return None

    def _record_non_run(
        self,
        schedule: dict[str, Any],
        *,
        status: str,
        trigger_source: str,
        message: str,
        advance_schedule: bool,
    ) -> None:
        now_dt = datetime.now().astimezone()
        now_text = self._format_timestamp(now_dt)
        config = schedule["schedule_config"]
        schedule_type = str(schedule.get("schedule_type") or "")
        schedule_enabled = bool(schedule.get("is_enabled", 1))
        next_run_at = schedule.get("next_run_at")
        if advance_schedule:
            next_run_dt = self._compute_next_run(schedule_type, config, reference=now_dt)
            if schedule_type == "once":
                schedule_enabled = False
                next_run_at = None
            else:
                next_run_at = self._format_timestamp(next_run_dt) if next_run_dt else None
        self.schedule_repository.update_schedule_state(
            int(schedule["id"]),
            next_run_at=next_run_at,
            last_run_at=now_text,
            last_status=status,
            is_enabled=schedule_enabled,
        )
        self.schedule_run_repository.add_run(
            schedule_id=int(schedule["id"]),
            workflow_id=int(schedule["workflow_id"]),
            device_id=int(schedule["device_id"]),
            trigger_source=trigger_source,
            status=status,
            message=message,
            metadata={"schedule_name": str(schedule["name"]), "next_run_at": next_run_at},
            started_at=now_text,
            finished_at=now_text,
        )
        log_status_map = {
            "skipped_overlap": "schedule_skipped_overlap",
            "missed_skipped": "schedule_missed_skipped",
            "missed_rescheduled": "schedule_missed_rescheduled",
        }
        self.log_service.add(
            int(schedule["workflow_id"]),
            int(schedule["device_id"]),
            "WARNING",
            log_status_map.get(status, "schedule_info"),
            message,
            {"schedule_id": int(schedule["id"]), "schedule_name": str(schedule["name"]), "trigger_source": trigger_source, "next_run_at": next_run_at},
        )

    def _next_daily_candidate(self, reference: datetime, time_text: str) -> datetime:
        scheduled_time = datetime.strptime(time_text, "%H:%M")
        candidate = reference.replace(hour=scheduled_time.hour, minute=scheduled_time.minute, second=0, microsecond=0)
        if candidate <= reference:
            candidate += timedelta(days=1)
        return candidate

    def _next_weekly_candidate(self, reference: datetime, time_text: str, weekdays: list[int]) -> datetime:
        scheduled_time = datetime.strptime(time_text, "%H:%M")
        normalized_days = sorted({int(day) for day in weekdays if 0 <= int(day) <= 6})
        for day_offset in range(0, 14):
            candidate_day = reference + timedelta(days=day_offset)
            if candidate_day.weekday() not in normalized_days:
                continue
            candidate = candidate_day.replace(hour=scheduled_time.hour, minute=scheduled_time.minute, second=0, microsecond=0)
            if candidate > reference:
                return candidate
        return reference + timedelta(days=7)

    def _apply_jitter(self, candidate: datetime, schedule_config: dict[str, Any]) -> datetime:
        jitter_seconds = int(schedule_config.get("jitter_seconds") or 0)
        if jitter_seconds <= 0:
            return candidate
        return candidate + timedelta(seconds=random.randint(0, jitter_seconds))

    def _adjust_to_active_window(self, candidate: datetime, schedule_config: dict[str, Any]) -> datetime:
        if not bool(schedule_config.get("active_window_enabled", False)):
            return candidate
        start_minutes, end_minutes = self._parse_clock_range(
            str(schedule_config.get("window_start") or "09:00"),
            str(schedule_config.get("window_end") or "18:00"),
        )
        candidate_minutes = candidate.hour * 60 + candidate.minute
        if candidate_minutes < start_minutes:
            return candidate.replace(hour=start_minutes // 60, minute=start_minutes % 60, second=0, microsecond=0)
        if candidate_minutes > end_minutes:
            next_day = candidate + timedelta(days=1)
            return next_day.replace(hour=start_minutes // 60, minute=start_minutes % 60, second=0, microsecond=0)
        return candidate

    def _parse_clock_range(self, start_text: str, end_text: str) -> tuple[int, int]:
        start_time = datetime.strptime(start_text, "%H:%M")
        end_time = datetime.strptime(end_text, "%H:%M")
        return start_time.hour * 60 + start_time.minute, end_time.hour * 60 + end_time.minute

    def _parse_timestamp(self, value: str) -> datetime:
        timezone = datetime.now().astimezone().tzinfo
        for fmt in (self.TIMESTAMP_FORMAT, "%Y-%m-%d %H:%M"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone)
            except ValueError:
                continue
        raise ValueError("Timestamp must be in YYYY-MM-DD HH:MM or YYYY-MM-DD HH:MM:SS format")

    def _format_timestamp(self, value: datetime) -> str:
        localized = value if value.tzinfo else value.replace(tzinfo=datetime.now().astimezone().tzinfo)
        return localized.astimezone().strftime(self.TIMESTAMP_FORMAT)
