from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any

from automation_studio.database import DatabaseManager


def row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row) if row is not None else {}


class DeviceRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def list_devices(self) -> list[dict[str, Any]]:
        query = "SELECT * FROM devices ORDER BY name COLLATE NOCASE, id"
        with self.db.connection() as connection:
            rows = connection.execute(query).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_device(self, device_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM devices WHERE id = ?",
                (device_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_device(self, device_id: int | None, name: str, serial: str, notes: str) -> int:
        timestamp = self.db.local_timestamp()
        if device_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE devices
                    SET name = ?, serial = ?, notes = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (name, serial, notes, timestamp, device_id),
                )
            return device_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO devices (name, serial, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, serial, notes, timestamp, timestamp),
            )
            return int(cursor.lastrowid)

    def delete_device(self, device_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM devices WHERE id = ?", (device_id,))

    def update_status(self, device_id: int, status: str) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE devices
                SET last_status = ?, last_seen = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, timestamp, timestamp, device_id),
            )

    def update_runtime_info(self, device_id: int, status: str, info_json: str) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE devices
                SET last_status = ?, last_seen = ?, last_info_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, timestamp, info_json, timestamp, device_id),
            )


class WorkflowRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def list_workflows(self) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM workflows ORDER BY name COLLATE NOCASE, id"
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_workflow(self, workflow_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM workflows WHERE id = ?",
                (workflow_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_workflow(
        self,
        workflow_id: int | None,
        name: str,
        description: str,
        is_active: bool = True,
        definition_version: int = 1,
    ) -> int:
        timestamp = self.db.local_timestamp()
        if workflow_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE workflows
                    SET name = ?, description = ?, is_active = ?, definition_version = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (name, description, int(is_active), int(definition_version), timestamp, workflow_id),
                )
            return workflow_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO workflows (name, description, is_active, definition_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (name, description, int(is_active), int(definition_version), timestamp, timestamp),
            )
            return int(cursor.lastrowid)

    def delete_workflow(self, workflow_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM workflows WHERE id = ?", (workflow_id,))

    def list_steps(self, workflow_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT * FROM steps
                WHERE workflow_id = ?
                ORDER BY position, id
                """,
                (workflow_id,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def upsert_step(
        self,
        step_id: int | None,
        workflow_id: int,
        position: int,
        name: str,
        step_type: str,
        parameters: str,
        is_enabled: bool = True,
        schema_version: int = 1,
    ) -> int:
        timestamp = self.db.local_timestamp()
        if step_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE steps
                    SET position = ?, name = ?, step_type = ?, parameters = ?, is_enabled = ?,
                        schema_version = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (position, name, step_type, parameters, int(is_enabled), int(schema_version), timestamp, step_id),
                )
            return step_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO steps (
                    workflow_id, position, name, step_type, parameters, is_enabled, schema_version, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    workflow_id,
                    position,
                    name,
                    step_type,
                    parameters,
                    int(is_enabled),
                    int(schema_version),
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def delete_step(self, step_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM steps WHERE id = ?", (step_id,))

    def reorder_steps(self, workflow_id: int, ordered_step_ids: list[int]) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            existing_rows = connection.execute(
                "SELECT id FROM steps WHERE workflow_id = ?",
                (workflow_id,),
            ).fetchall()
            existing_ids = {int(row["id"]) for row in existing_rows}

            if existing_ids != set(ordered_step_ids):
                raise ValueError("Step order does not match workflow steps")

            for position, step_id in enumerate(ordered_step_ids, start=1):
                connection.execute(
                    """
                    UPDATE steps
                    SET position = ?, updated_at = ?
                    WHERE id = ? AND workflow_id = ?
                    """,
                    (position, timestamp, step_id, workflow_id),
                )


class LogRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def add_log(
        self,
        workflow_id: int | None,
        device_id: int | None,
        level: str,
        status: str,
        message: str,
        metadata: dict[str, Any] | None = None,
        watcher_id: int | None = None,
    ) -> int:
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO logs (workflow_id, device_id, watcher_id, level, status, message, metadata, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (workflow_id, device_id, watcher_id, level, status, message, payload, timestamp),
            )
            return int(cursor.lastrowid)

    def list_logs(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
        watcher_id: int | None = None,
        status: str | None = None,
        limit: int = 300,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        values: list[Any] = []
        if workflow_id:
            conditions.append("logs.workflow_id = ?")
            values.append(workflow_id)
        if device_id:
            conditions.append("logs.device_id = ?")
            values.append(device_id)
        if watcher_id:
            conditions.append("logs.watcher_id = ?")
            values.append(watcher_id)
        if status and status != "all":
            conditions.append("logs.status = ?")
            values.append(status)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                logs.*,
                workflows.name AS workflow_name,
                devices.name AS device_name,
                watchers.name AS watcher_name
            FROM logs
            LEFT JOIN workflows ON workflows.id = logs.workflow_id
            LEFT JOIN devices ON devices.id = logs.device_id
            LEFT JOIN watchers ON watchers.id = logs.watcher_id
            {where_clause}
            ORDER BY logs.created_at DESC, logs.id DESC
            LIMIT ?
        """
        values.append(limit)

        with self.db.connection() as connection:
            rows = connection.execute(query, values).fetchall()
        return [row_to_dict(row) for row in rows]


class TelemetryRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def record_step_result(
        self,
        workflow_id: int | None,
        device_id: int | None,
        step_type: str,
        outcome: str,
        duration_ms: int,
        error_message: str = "",
    ) -> None:
        timestamp = self.db.local_timestamp()
        if outcome == "success":
            counters = ("success_count", 1, "failure_count", 0, "continued_failure_count", 0, "skipped_count", 0)
        elif outcome == "continued_failure":
            counters = ("success_count", 0, "failure_count", 0, "continued_failure_count", 1, "skipped_count", 0)
        elif outcome in {"skipped_failure", "skipped"}:
            counters = ("success_count", 0, "failure_count", 0, "continued_failure_count", 0, "skipped_count", 1)
        else:
            counters = ("success_count", 0, "failure_count", 1, "continued_failure_count", 0, "skipped_count", 0)

        with self.db.connection() as connection:
            connection.execute(
                f"""
                INSERT INTO step_telemetry (
                    workflow_id, device_id, step_type,
                    success_count, failure_count, continued_failure_count, skipped_count,
                    total_duration_ms, last_error, last_run_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workflow_id, device_id, step_type)
                DO UPDATE SET
                    {counters[0]} = {counters[0]} + excluded.{counters[0]},
                    {counters[2]} = {counters[2]} + excluded.{counters[2]},
                    {counters[4]} = {counters[4]} + excluded.{counters[4]},
                    {counters[6]} = {counters[6]} + excluded.{counters[6]},
                    total_duration_ms = total_duration_ms + excluded.total_duration_ms,
                    last_error = CASE
                        WHEN excluded.last_error <> '' THEN excluded.last_error
                        ELSE last_error
                    END,
                    last_run_at = excluded.last_run_at
                """,
                (
                    workflow_id,
                    device_id,
                    step_type,
                    counters[1],
                    counters[3],
                    counters[5],
                    counters[7],
                    int(duration_ms),
                    error_message,
                    timestamp,
                ),
            )

    def summary(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        values: list[Any] = []
        if workflow_id:
            conditions.append("step_telemetry.workflow_id = ?")
            values.append(workflow_id)
        if device_id:
            conditions.append("step_telemetry.device_id = ?")
            values.append(device_id)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                step_telemetry.*,
                workflows.name AS workflow_name,
                devices.name AS device_name,
                CASE
                    WHEN (success_count + failure_count + continued_failure_count + skipped_count) = 0 THEN 0
                    ELSE ROUND(
                        (failure_count + continued_failure_count) * 100.0 /
                        (success_count + failure_count + continued_failure_count + skipped_count),
                        2
                    )
                END AS failure_rate
            FROM step_telemetry
            LEFT JOIN workflows ON workflows.id = step_telemetry.workflow_id
            LEFT JOIN devices ON devices.id = step_telemetry.device_id
            {where_clause}
            ORDER BY failure_rate DESC, failure_count DESC, continued_failure_count DESC, step_type ASC
            LIMIT ?
        """
        values.append(limit)
        with self.db.connection() as connection:
            rows = connection.execute(query, values).fetchall()
        return [row_to_dict(row) for row in rows]


class WatcherRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def list_watchers(self) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM watchers
                ORDER BY priority ASC, name COLLATE NOCASE, id
                """
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_watcher(self, watcher_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT * FROM watchers WHERE id = ?",
                (watcher_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_watcher(
        self,
        watcher_id: int | None,
        name: str,
        scope_type: str,
        scope_id: int | None,
        condition_type: str,
        condition_json: str,
        action_type: str,
        action_json: str,
        policy_json: str,
        is_enabled: bool = True,
        priority: int = 100,
    ) -> int:
        timestamp = self.db.local_timestamp()
        normalized_scope_id = int(scope_id) if scope_id else None
        if watcher_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE watchers
                    SET name = ?, scope_type = ?, scope_id = ?, condition_type = ?, condition_json = ?,
                        action_type = ?, action_json = ?, policy_json = ?, is_enabled = ?, priority = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        name,
                        scope_type,
                        normalized_scope_id,
                        condition_type,
                        condition_json,
                        action_type,
                        action_json,
                        policy_json,
                        int(is_enabled),
                        int(priority),
                        timestamp,
                        watcher_id,
                    ),
                )
            return watcher_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO watchers (
                    name, scope_type, scope_id, condition_type, condition_json,
                    action_type, action_json, policy_json, is_enabled, priority,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    scope_type,
                    normalized_scope_id,
                    condition_type,
                    condition_json,
                    action_type,
                    action_json,
                    policy_json,
                    int(is_enabled),
                    int(priority),
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def delete_watcher(self, watcher_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM watchers WHERE id = ?", (watcher_id,))

    def resolve_active_watchers(self, workflow_id: int, device_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM watchers
                WHERE is_enabled = 1
                  AND (
                    scope_type = 'global'
                    OR (scope_type = 'workflow' AND scope_id = ?)
                    OR (scope_type = 'device' AND scope_id = ?)
                  )
                ORDER BY priority ASC, id ASC
                """,
                (workflow_id, device_id),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def list_profiles(self) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    watcher_profiles.*,
                    COUNT(watcher_profile_items.watcher_id) AS watcher_count
                FROM watcher_profiles
                LEFT JOIN watcher_profile_items
                    ON watcher_profile_items.profile_id = watcher_profiles.id
                GROUP BY watcher_profiles.id
                ORDER BY watcher_profiles.name COLLATE NOCASE, watcher_profiles.id
                """
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_profile(self, profile_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    watcher_profiles.*,
                    COUNT(watcher_profile_items.watcher_id) AS watcher_count
                FROM watcher_profiles
                LEFT JOIN watcher_profile_items
                    ON watcher_profile_items.profile_id = watcher_profiles.id
                WHERE watcher_profiles.id = ?
                GROUP BY watcher_profiles.id
                """,
                (profile_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_profile(
        self,
        profile_id: int | None,
        name: str,
        description: str,
        is_active: bool = True,
    ) -> int:
        timestamp = self.db.local_timestamp()
        if profile_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE watcher_profiles
                    SET name = ?, description = ?, is_active = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (name, description, int(is_active), timestamp, profile_id),
                )
            return profile_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO watcher_profiles (name, description, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, description, int(is_active), timestamp, timestamp),
            )
            return int(cursor.lastrowid)

    def delete_profile(self, profile_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM watcher_profiles WHERE id = ?", (profile_id,))

    def list_profile_watchers(self, profile_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    watchers.*,
                    watcher_profile_items.position,
                    watcher_profiles.name AS profile_name
                FROM watcher_profile_items
                INNER JOIN watchers ON watchers.id = watcher_profile_items.watcher_id
                INNER JOIN watcher_profiles ON watcher_profiles.id = watcher_profile_items.profile_id
                WHERE watcher_profile_items.profile_id = ?
                ORDER BY watcher_profile_items.position, watchers.priority, watchers.id
                """,
                (profile_id,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def save_profile_watchers(self, profile_id: int, watcher_ids: list[int]) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM watcher_profile_items WHERE profile_id = ?", (profile_id,))
            for position, watcher_id in enumerate(watcher_ids, start=1):
                connection.execute(
                    """
                    INSERT INTO watcher_profile_items (profile_id, watcher_id, position)
                    VALUES (?, ?, ?)
                    """,
                    (profile_id, watcher_id, position),
                )

    def list_profiles_for_workflow(self, workflow_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    watcher_profiles.*,
                    COUNT(watcher_profile_items.watcher_id) AS watcher_count
                FROM workflow_watcher_profiles
                INNER JOIN watcher_profiles ON watcher_profiles.id = workflow_watcher_profiles.profile_id
                LEFT JOIN watcher_profile_items
                    ON watcher_profile_items.profile_id = watcher_profiles.id
                WHERE workflow_watcher_profiles.workflow_id = ?
                GROUP BY watcher_profiles.id
                ORDER BY watcher_profiles.name COLLATE NOCASE, watcher_profiles.id
                """,
                (workflow_id,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def save_workflow_profiles(self, workflow_id: int, profile_ids: list[int]) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM workflow_watcher_profiles WHERE workflow_id = ?", (workflow_id,))
            for profile_id in profile_ids:
                connection.execute(
                    """
                    INSERT INTO workflow_watcher_profiles (workflow_id, profile_id)
                    VALUES (?, ?)
                    """,
                    (workflow_id, profile_id),
                )

    def resolve_profile_watchers(self, workflow_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    watchers.*,
                    watcher_profiles.id AS profile_id,
                    watcher_profiles.name AS profile_name
                FROM workflow_watcher_profiles
                INNER JOIN watcher_profiles
                    ON watcher_profiles.id = workflow_watcher_profiles.profile_id
                INNER JOIN watcher_profile_items
                    ON watcher_profile_items.profile_id = watcher_profiles.id
                INNER JOIN watchers
                    ON watchers.id = watcher_profile_items.watcher_id
                WHERE workflow_watcher_profiles.workflow_id = ?
                  AND watcher_profiles.is_active = 1
                  AND watchers.is_enabled = 1
                ORDER BY watchers.priority ASC, watcher_profile_items.position ASC, watchers.id ASC
                """,
                (workflow_id,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]


class WatcherTelemetryRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def record_watcher_result(
        self,
        watcher_id: int,
        workflow_id: int | None,
        device_id: int | None,
        outcome: str,
        error_message: str = "",
    ) -> None:
        timestamp = self.db.local_timestamp()
        success_count = 1 if outcome == "success" else 0
        failure_count = 1 if outcome == "failure" else 0

        with self.db.connection() as connection:
            connection.execute(
                """
                INSERT INTO watcher_telemetry (
                    watcher_id, workflow_id, device_id, trigger_count, success_count,
                    failure_count, last_error, last_triggered_at
                )
                VALUES (?, ?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(watcher_id, workflow_id, device_id)
                DO UPDATE SET
                    trigger_count = trigger_count + 1,
                    success_count = success_count + excluded.success_count,
                    failure_count = failure_count + excluded.failure_count,
                    last_error = CASE
                        WHEN excluded.last_error <> '' THEN excluded.last_error
                        ELSE last_error
                    END,
                    last_triggered_at = excluded.last_triggered_at
                """,
                (
                    watcher_id,
                    workflow_id,
                    device_id,
                    success_count,
                    failure_count,
                    error_message,
                    timestamp,
                ),
            )

    def summary(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        conditions: list[str] = []
        values: list[Any] = []
        if workflow_id:
            conditions.append("watcher_telemetry.workflow_id = ?")
            values.append(workflow_id)
        if device_id:
            conditions.append("watcher_telemetry.device_id = ?")
            values.append(device_id)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                watcher_telemetry.*,
                watchers.name AS watcher_name,
                workflows.name AS workflow_name,
                devices.name AS device_name,
                CASE
                    WHEN trigger_count = 0 THEN 0
                    ELSE ROUND(success_count * 100.0 / trigger_count, 2)
                END AS success_rate,
                CASE
                    WHEN trigger_count = 0 THEN 0
                    ELSE ROUND(failure_count * 100.0 / trigger_count, 2)
                END AS failure_rate
            FROM watcher_telemetry
            LEFT JOIN watchers ON watchers.id = watcher_telemetry.watcher_id
            LEFT JOIN workflows ON workflows.id = watcher_telemetry.workflow_id
            LEFT JOIN devices ON devices.id = watcher_telemetry.device_id
            {where_clause}
            ORDER BY failure_rate DESC, trigger_count DESC, watcher_telemetry.id DESC
            LIMIT ?
        """
        values.append(limit)

        with self.db.connection() as connection:
            rows = connection.execute(query, values).fetchall()
        return [row_to_dict(row) for row in rows]


class AccountRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def _account_alias_summary_join(self) -> str:
        return """
            LEFT JOIN (
                SELECT
                    account_id,
                    GROUP_CONCAT(alias_name, '\n') AS alias_names,
                    COUNT(*) AS alias_count
                FROM account_aliases
                GROUP BY account_id
            ) AS account_alias_summary
                ON account_alias_summary.account_id = accounts.id
        """

    def list_device_platforms(self, device_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    device_platforms.*,
                    workflows.name AS switch_workflow_name,
                    accounts.display_name AS current_account_name,
                    COUNT(platform_accounts.id) AS account_count
                FROM device_platforms
                LEFT JOIN workflows ON workflows.id = device_platforms.switch_workflow_id
                LEFT JOIN accounts ON accounts.id = device_platforms.current_account_id
                LEFT JOIN accounts AS platform_accounts
                    ON platform_accounts.device_platform_id = device_platforms.id
                WHERE device_platforms.device_id = ?
                GROUP BY device_platforms.id
                ORDER BY device_platforms.platform_name COLLATE NOCASE, device_platforms.id
                """,
                (device_id,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_device_platform(self, device_platform_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    device_platforms.*,
                    workflows.name AS switch_workflow_name,
                    accounts.display_name AS current_account_name
                FROM device_platforms
                LEFT JOIN workflows ON workflows.id = device_platforms.switch_workflow_id
                LEFT JOIN accounts ON accounts.id = device_platforms.current_account_id
                WHERE device_platforms.id = ?
                """,
                (device_platform_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def get_device_platform_by_key(self, device_id: int, platform_key: str) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    device_platforms.*,
                    workflows.name AS switch_workflow_name,
                    accounts.display_name AS current_account_name
                FROM device_platforms
                LEFT JOIN workflows ON workflows.id = device_platforms.switch_workflow_id
                LEFT JOIN accounts ON accounts.id = device_platforms.current_account_id
                WHERE device_platforms.device_id = ? AND LOWER(device_platforms.platform_key) = LOWER(?)
                """,
                (device_id, platform_key),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_device_platform(
        self,
        device_platform_id: int | None,
        device_id: int,
        platform_key: str,
        platform_name: str,
        package_name: str,
        switch_workflow_id: int | None,
        is_enabled: bool = True,
    ) -> int:
        timestamp = self.db.local_timestamp()
        normalized_workflow_id = int(switch_workflow_id) if switch_workflow_id else None
        if device_platform_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE device_platforms
                    SET platform_key = ?, platform_name = ?, package_name = ?, switch_workflow_id = ?,
                        is_enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        platform_key,
                        platform_name,
                        package_name,
                        normalized_workflow_id,
                        int(is_enabled),
                        timestamp,
                        device_platform_id,
                    ),
                )
            return device_platform_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO device_platforms (
                    device_id, platform_key, platform_name, package_name, switch_workflow_id,
                    is_enabled, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,
                    platform_key,
                    platform_name,
                    package_name,
                    normalized_workflow_id,
                    int(is_enabled),
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def delete_device_platform(self, device_platform_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM device_platforms WHERE id = ?", (device_platform_id,))

    def update_current_account(self, device_platform_id: int, account_id: int | None) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE device_platforms
                SET current_account_id = ?, updated_at = ?
                WHERE id = ?
                """,
                (int(account_id) if account_id else None, timestamp, device_platform_id),
            )

    def list_accounts(self, device_platform_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    accounts.*,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    COALESCE(account_alias_summary.alias_names, '') AS alias_names,
                    COALESCE(account_alias_summary.alias_count, 0) AS alias_count,
                    CASE
                        WHEN device_platforms.current_account_id = accounts.id THEN 1
                        ELSE 0
                    END AS is_current
                FROM accounts
                INNER JOIN device_platforms ON device_platforms.id = accounts.device_platform_id
                {self._account_alias_summary_join()}
                WHERE accounts.device_platform_id = ?
                ORDER BY accounts.display_name COLLATE NOCASE, accounts.id
                """,
                (device_platform_id,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_account(self, account_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                f"""
                SELECT
                    accounts.*,
                    device_platforms.device_id,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    device_platforms.package_name,
                    device_platforms.switch_workflow_id,
                    COALESCE(account_alias_summary.alias_names, '') AS alias_names,
                    COALESCE(account_alias_summary.alias_count, 0) AS alias_count,
                    CASE
                        WHEN device_platforms.current_account_id = accounts.id THEN 1
                        ELSE 0
                    END AS is_current
                FROM accounts
                INNER JOIN device_platforms ON device_platforms.id = accounts.device_platform_id
                {self._account_alias_summary_join()}
                WHERE accounts.id = ?
                """,
                (account_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def get_account_by_name(self, device_platform_id: int, display_name: str) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                f"""
                SELECT
                    accounts.*,
                    device_platforms.device_id,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    device_platforms.package_name,
                    device_platforms.switch_workflow_id,
                    COALESCE(account_alias_summary.alias_names, '') AS alias_names,
                    COALESCE(account_alias_summary.alias_count, 0) AS alias_count,
                    CASE
                        WHEN device_platforms.current_account_id = accounts.id THEN 1
                        ELSE 0
                    END AS is_current
                FROM accounts
                INNER JOIN device_platforms ON device_platforms.id = accounts.device_platform_id
                {self._account_alias_summary_join()}
                WHERE accounts.device_platform_id = ? AND accounts.display_name = ?
                """,
                (device_platform_id, display_name),
            ).fetchone()
        return row_to_dict(row) if row else None

    def get_account_by_identity(self, device_platform_id: int, identity_normalized: str) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                f"""
                SELECT
                    accounts.*,
                    device_platforms.device_id,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    device_platforms.package_name,
                    device_platforms.switch_workflow_id,
                    COALESCE(account_alias_summary.alias_names, '') AS alias_names,
                    COALESCE(account_alias_summary.alias_count, 0) AS alias_count,
                    CASE
                        WHEN device_platforms.current_account_id = accounts.id THEN 1
                        ELSE 0
                    END AS is_current
                FROM accounts
                INNER JOIN device_platforms ON device_platforms.id = accounts.device_platform_id
                {self._account_alias_summary_join()}
                LEFT JOIN account_aliases
                    ON account_aliases.account_id = accounts.id
                WHERE accounts.device_platform_id = ?
                  AND (
                    accounts.display_name_normalized = ?
                    OR accounts.username_normalized = ?
                    OR accounts.login_id_normalized = ?
                    OR account_aliases.alias_normalized = ?
                  )
                ORDER BY accounts.id
                LIMIT 1
                """,
                (
                    device_platform_id,
                    identity_normalized,
                    identity_normalized,
                    identity_normalized,
                    identity_normalized,
                ),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_account(
        self,
        account_id: int | None,
        device_platform_id: int,
        display_name: str,
        display_name_normalized: str,
        username: str,
        username_normalized: str,
        login_id: str,
        login_id_normalized: str,
        notes: str,
        metadata_json: str,
        is_enabled: bool = True,
    ) -> int:
        timestamp = self.db.local_timestamp()
        if account_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE accounts
                    SET display_name = ?, display_name_normalized = ?, username = ?, username_normalized = ?,
                        login_id = ?, login_id_normalized = ?, notes = ?, metadata_json = ?,
                        is_enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        display_name,
                        display_name_normalized,
                        username,
                        username_normalized,
                        login_id,
                        login_id_normalized,
                        notes,
                        metadata_json,
                        int(is_enabled),
                        timestamp,
                        account_id,
                    ),
                )
            return account_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO accounts (
                    device_platform_id, display_name, display_name_normalized, username, username_normalized,
                    login_id, login_id_normalized, notes, metadata_json,
                    is_enabled, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_platform_id,
                    display_name,
                    display_name_normalized,
                    username,
                    username_normalized,
                    login_id,
                    login_id_normalized,
                    notes,
                    metadata_json,
                    int(is_enabled),
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def list_account_aliases(self, account_id: int) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM account_aliases
                WHERE account_id = ?
                ORDER BY alias_name COLLATE NOCASE, id
                """,
                (account_id,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def replace_account_aliases(self, account_id: int, device_platform_id: int, aliases: list[tuple[str, str]]) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute("DELETE FROM account_aliases WHERE account_id = ?", (account_id,))
            for alias_name, alias_normalized in aliases:
                connection.execute(
                    """
                    INSERT INTO account_aliases (
                        account_id, device_platform_id, alias_name, alias_normalized, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (account_id, device_platform_id, alias_name, alias_normalized, timestamp, timestamp),
                )

    def delete_account(self, account_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM accounts WHERE id = ?", (account_id,))


class UploadRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def list_upload_jobs(self) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    upload_jobs.*,
                    devices.name AS device_name,
                    workflows.name AS workflow_name,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    accounts.display_name AS account_name
                FROM upload_jobs
                INNER JOIN devices ON devices.id = upload_jobs.device_id
                INNER JOIN workflows ON workflows.id = upload_jobs.workflow_id
                LEFT JOIN device_platforms ON device_platforms.id = upload_jobs.device_platform_id
                LEFT JOIN accounts ON accounts.id = upload_jobs.account_id
                ORDER BY upload_jobs.updated_at DESC, upload_jobs.id DESC
                """
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_upload_job(self, upload_job_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    upload_jobs.*,
                    devices.name AS device_name,
                    workflows.name AS workflow_name,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    accounts.display_name AS account_name
                FROM upload_jobs
                INNER JOIN devices ON devices.id = upload_jobs.device_id
                INNER JOIN workflows ON workflows.id = upload_jobs.workflow_id
                LEFT JOIN device_platforms ON device_platforms.id = upload_jobs.device_platform_id
                LEFT JOIN accounts ON accounts.id = upload_jobs.account_id
                WHERE upload_jobs.id = ?
                """,
                (upload_job_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_upload_job(
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
        tags_json: str,
        video_url: str,
        cover_url: str,
        local_video_path: str,
        metadata_json: str,
        status: str = "draft",
    ) -> int:
        timestamp = self.db.local_timestamp()
        normalized_platform_id = int(device_platform_id) if device_platform_id else None
        normalized_account_id = int(account_id) if account_id else None
        if upload_job_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE upload_jobs
                    SET device_id = ?, device_platform_id = ?, account_id = ?, workflow_id = ?,
                        code_product = ?, link_product = ?, title = ?, description = ?, tags_json = ?, video_url = ?,
                        cover_url = ?, local_video_path = ?, metadata_json = ?,
                        status = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        device_id,
                        normalized_platform_id,
                        normalized_account_id,
                        workflow_id,
                        code_product,
                        link_product,
                        title,
                        description,
                        tags_json,
                        video_url,
                        cover_url,
                        local_video_path,
                        metadata_json,
                        status,
                        timestamp,
                        upload_job_id,
                    ),
                )
            return upload_job_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO upload_jobs (
                    device_id, device_platform_id, account_id, workflow_id,
                    code_product, link_product, title, description, tags_json, video_url,
                    cover_url, local_video_path, metadata_json,
                    status, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,
                    normalized_platform_id,
                    normalized_account_id,
                    workflow_id,
                    code_product,
                    link_product,
                    title,
                    description,
                    tags_json,
                    video_url,
                    cover_url,
                    local_video_path,
                    metadata_json,
                    status,
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def delete_upload_job(self, upload_job_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM upload_jobs WHERE id = ?", (upload_job_id,))

    def mark_upload_queued(self, upload_job_id: int) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE upload_jobs
                SET status = 'queued', last_error = '', updated_at = ?
                WHERE id = ?
                """,
                (timestamp, upload_job_id),
            )

    def set_upload_status(self, upload_job_id: int, status: str, *, last_error: str = "") -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE upload_jobs
                SET status = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (str(status or "draft"), str(last_error or ""), timestamp, upload_job_id),
            )

    def mark_upload_started(self, upload_job_id: int) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE upload_jobs
                SET status = 'running', last_error = '', started_at = ?, finished_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (timestamp, timestamp, upload_job_id),
            )

    def update_upload_local_video_path(self, upload_job_id: int, local_video_path: str) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE upload_jobs
                SET local_video_path = ?, updated_at = ?
                WHERE id = ?
                """,
                (str(local_video_path or "").strip(), timestamp, upload_job_id),
            )

    def mark_upload_finished(
        self,
        upload_job_id: int,
        *,
        status: str,
        last_error: str = "",
        result_json: str = "{}",
    ) -> None:
        timestamp = self.db.local_timestamp()
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE upload_jobs
                SET status = ?, last_error = ?, result_json = ?, finished_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, last_error, result_json, timestamp, timestamp, upload_job_id),
            )

    def is_upload_job_busy(self, upload_job_id: int) -> bool:
        with self.db.connection() as connection:
            row = connection.execute(
                "SELECT status FROM upload_jobs WHERE id = ?",
                (upload_job_id,),
            ).fetchone()
        if not row:
            return False
        return str(row["status"] or "") in {"queued", "running"}

    def try_acquire_upload_execution(
        self,
        upload_job_id: int,
        device_id: int,
        *,
        owner_id: str,
        lease_seconds: int = 3600,
    ) -> tuple[bool, str]:
        now = self.db.local_timestamp()
        expires_at = (datetime.now().astimezone() + timedelta(seconds=max(int(lease_seconds), 60))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        lock_rows = [
            (
                f"upload_job:{int(upload_job_id)}",
                "upload_execution",
                "upload_job",
                int(upload_job_id),
                owner_id,
                now,
                expires_at,
                json.dumps({"upload_job_id": int(upload_job_id)}, ensure_ascii=False),
            ),
            (
                f"device:{int(device_id)}:upload_execution",
                "upload_execution",
                "device",
                int(device_id),
                owner_id,
                now,
                expires_at,
                json.dumps({"device_id": int(device_id)}, ensure_ascii=False),
            ),
        ]
        try:
            with self.db.connection() as connection:
                connection.execute("DELETE FROM runtime_locks WHERE expires_at <= ?", (now,))
                for row in lock_rows:
                    connection.execute(
                        """
                        INSERT INTO runtime_locks (
                            lock_key, lock_group, resource_type, resource_id,
                            owner_id, acquired_at, expires_at, metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        row,
                    )
        except sqlite3.IntegrityError:
            return False, "Upload job or device is already busy"
        return True, ""

    def release_upload_execution(self, upload_job_id: int, device_id: int, *, owner_id: str) -> None:
        with self.db.connection() as connection:
            connection.execute(
                """
                DELETE FROM runtime_locks
                WHERE owner_id = ?
                  AND lock_key IN (?, ?)
                """,
                (
                    owner_id,
                    f"upload_job:{int(upload_job_id)}",
                    f"device:{int(device_id)}:upload_execution",
                ),
            )

    def list_upload_templates(self) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    upload_templates.*,
                    devices.name AS device_name,
                    workflows.name AS workflow_name,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    accounts.display_name AS account_name
                FROM upload_templates
                LEFT JOIN devices ON devices.id = upload_templates.device_id
                LEFT JOIN workflows ON workflows.id = upload_templates.workflow_id
                LEFT JOIN device_platforms ON device_platforms.id = upload_templates.device_platform_id
                LEFT JOIN accounts ON accounts.id = upload_templates.account_id
                WHERE upload_templates.is_active = 1
                ORDER BY upload_templates.name COLLATE NOCASE, upload_templates.id
                """
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_upload_template(self, template_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    upload_templates.*,
                    devices.name AS device_name,
                    workflows.name AS workflow_name,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    accounts.display_name AS account_name
                FROM upload_templates
                LEFT JOIN devices ON devices.id = upload_templates.device_id
                LEFT JOIN workflows ON workflows.id = upload_templates.workflow_id
                LEFT JOIN device_platforms ON device_platforms.id = upload_templates.device_platform_id
                LEFT JOIN accounts ON accounts.id = upload_templates.account_id
                WHERE upload_templates.id = ?
                """,
                (template_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_upload_template(
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
        description_template: str,
        tags_json: str,
        video_url: str,
        cover_url: str,
        local_video_path: str,
        metadata_json: str,
        is_active: bool = True,
    ) -> int:
        timestamp = self.db.local_timestamp()
        normalized_device_id = int(device_id) if device_id else None
        normalized_platform_id = int(device_platform_id) if device_platform_id else None
        normalized_account_id = int(account_id) if account_id else None
        normalized_workflow_id = int(workflow_id) if workflow_id else None
        if template_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE upload_templates
                    SET name = ?, description = ?, device_id = ?, device_platform_id = ?, account_id = ?, workflow_id = ?,
                        code_product = ?, link_product = ?, title = ?, description_template = ?, tags_json = ?,
                        video_url = ?, cover_url = ?, local_video_path = ?, metadata_json = ?, is_active = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        name,
                        description,
                        normalized_device_id,
                        normalized_platform_id,
                        normalized_account_id,
                        normalized_workflow_id,
                        code_product,
                        link_product,
                        title,
                        description_template,
                        tags_json,
                        video_url,
                        cover_url,
                        local_video_path,
                        metadata_json,
                        int(is_active),
                        timestamp,
                        template_id,
                    ),
                )
            return template_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO upload_templates (
                    name, description, device_id, device_platform_id, account_id, workflow_id,
                    code_product, link_product, title, description_template, tags_json,
                    video_url, cover_url, local_video_path, metadata_json, is_active,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    description,
                    normalized_device_id,
                    normalized_platform_id,
                    normalized_account_id,
                    normalized_workflow_id,
                    code_product,
                    link_product,
                    title,
                    description_template,
                    tags_json,
                    video_url,
                    cover_url,
                    local_video_path,
                    metadata_json,
                    int(is_active),
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def delete_upload_template(self, template_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM upload_templates WHERE id = ?", (template_id,))


class ScheduleRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def list_schedules(self) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    workflow_schedules.*,
                    workflows.name AS workflow_name,
                    devices.name AS device_name,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    accounts.display_name AS account_name,
                    schedule_groups.name AS group_name,
                    schedule_groups.is_enabled AS group_is_enabled
                FROM workflow_schedules
                INNER JOIN workflows ON workflows.id = workflow_schedules.workflow_id
                INNER JOIN devices ON devices.id = workflow_schedules.device_id
                LEFT JOIN device_platforms ON device_platforms.id = workflow_schedules.device_platform_id
                LEFT JOIN accounts ON accounts.id = workflow_schedules.account_id
                LEFT JOIN schedule_groups ON schedule_groups.id = workflow_schedules.schedule_group_id
                ORDER BY workflow_schedules.priority ASC, workflow_schedules.name COLLATE NOCASE, workflow_schedules.id
                """
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_schedule(self, schedule_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    workflow_schedules.*,
                    workflows.name AS workflow_name,
                    devices.name AS device_name,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    accounts.display_name AS account_name,
                    schedule_groups.name AS group_name,
                    schedule_groups.is_enabled AS group_is_enabled
                FROM workflow_schedules
                INNER JOIN workflows ON workflows.id = workflow_schedules.workflow_id
                INNER JOIN devices ON devices.id = workflow_schedules.device_id
                LEFT JOIN device_platforms ON device_platforms.id = workflow_schedules.device_platform_id
                LEFT JOIN accounts ON accounts.id = workflow_schedules.account_id
                LEFT JOIN schedule_groups ON schedule_groups.id = workflow_schedules.schedule_group_id
                WHERE workflow_schedules.id = ?
                """,
                (schedule_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_schedule(
        self,
        schedule_id: int | None,
        name: str,
        workflow_id: int,
        device_id: int,
        schedule_group_id: int | None,
        device_platform_id: int | None,
        account_id: int | None,
        use_current_account: bool,
        schedule_type: str,
        schedule_json: str,
        next_run_at: str | None,
        priority: int,
        is_enabled: bool = True,
    ) -> int:
        timestamp = self.db.local_timestamp()
        normalized_schedule_group_id = int(schedule_group_id) if schedule_group_id else None
        normalized_device_platform_id = int(device_platform_id) if device_platform_id else None
        normalized_account_id = int(account_id) if account_id else None
        if schedule_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE workflow_schedules
                    SET name = ?, workflow_id = ?, device_id = ?, schedule_group_id = ?, device_platform_id = ?, account_id = ?,
                        use_current_account = ?, schedule_type = ?, schedule_json = ?, next_run_at = ?,
                        priority = ?, is_enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        name,
                        workflow_id,
                        device_id,
                        normalized_schedule_group_id,
                        normalized_device_platform_id,
                        normalized_account_id,
                        int(use_current_account),
                        schedule_type,
                        schedule_json,
                        next_run_at,
                        int(priority),
                        int(is_enabled),
                        timestamp,
                        schedule_id,
                    ),
                )
            return schedule_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO workflow_schedules (
                    name, workflow_id, device_id, schedule_group_id, device_platform_id, account_id,
                    use_current_account, schedule_type, schedule_json, next_run_at,
                    priority, is_enabled, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    name,
                    workflow_id,
                    device_id,
                    normalized_schedule_group_id,
                    normalized_device_platform_id,
                    normalized_account_id,
                    int(use_current_account),
                    schedule_type,
                    schedule_json,
                    next_run_at,
                    int(priority),
                    int(is_enabled),
                    timestamp,
                    timestamp,
                ),
            )
            return int(cursor.lastrowid)

    def delete_schedule(self, schedule_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM workflow_schedules WHERE id = ?", (schedule_id,))

    def due_schedules(self, now_timestamp: str) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    workflow_schedules.*,
                    workflows.name AS workflow_name,
                    devices.name AS device_name,
                    device_platforms.platform_key,
                    device_platforms.platform_name,
                    accounts.display_name AS account_name,
                    schedule_groups.name AS group_name,
                    schedule_groups.is_enabled AS group_is_enabled
                FROM workflow_schedules
                INNER JOIN workflows ON workflows.id = workflow_schedules.workflow_id
                INNER JOIN devices ON devices.id = workflow_schedules.device_id
                LEFT JOIN device_platforms ON device_platforms.id = workflow_schedules.device_platform_id
                LEFT JOIN accounts ON accounts.id = workflow_schedules.account_id
                LEFT JOIN schedule_groups ON schedule_groups.id = workflow_schedules.schedule_group_id
                WHERE workflow_schedules.is_enabled = 1
                  AND workflow_schedules.next_run_at IS NOT NULL
                  AND workflow_schedules.next_run_at <= ?
                  AND COALESCE(schedule_groups.is_enabled, 1) = 1
                ORDER BY workflow_schedules.next_run_at ASC, workflow_schedules.priority ASC, workflow_schedules.id ASC
                """,
                (now_timestamp,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def update_schedule_state(
        self,
        schedule_id: int,
        *,
        next_run_at: str | None = None,
        last_run_at: str | None = None,
        last_status: str | None = None,
        is_enabled: bool | None = None,
    ) -> None:
        updates: list[str] = ["updated_at = ?"]
        values: list[Any] = [self.db.local_timestamp()]
        if next_run_at is not None or next_run_at is None:
            updates.append("next_run_at = ?")
            values.append(next_run_at)
        if last_run_at is not None:
            updates.append("last_run_at = ?")
            values.append(last_run_at)
        if last_status is not None:
            updates.append("last_status = ?")
            values.append(last_status)
        if is_enabled is not None:
            updates.append("is_enabled = ?")
            values.append(int(is_enabled))
        values.append(schedule_id)
        with self.db.connection() as connection:
            connection.execute(
                f"""
                UPDATE workflow_schedules
                SET {', '.join(updates)}
                WHERE id = ?
                """,
                values,
            )


class ScheduleGroupRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def list_groups(self) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    schedule_groups.*,
                    COUNT(workflow_schedules.id) AS schedule_count,
                    SUM(CASE WHEN workflow_schedules.is_enabled = 1 THEN 1 ELSE 0 END) AS enabled_schedule_count
                FROM schedule_groups
                LEFT JOIN workflow_schedules ON workflow_schedules.schedule_group_id = schedule_groups.id
                GROUP BY schedule_groups.id
                ORDER BY schedule_groups.name COLLATE NOCASE, schedule_groups.id
                """
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def get_group(self, group_id: int) -> dict[str, Any] | None:
        with self.db.connection() as connection:
            row = connection.execute(
                """
                SELECT
                    schedule_groups.*,
                    COUNT(workflow_schedules.id) AS schedule_count,
                    SUM(CASE WHEN workflow_schedules.is_enabled = 1 THEN 1 ELSE 0 END) AS enabled_schedule_count
                FROM schedule_groups
                LEFT JOIN workflow_schedules ON workflow_schedules.schedule_group_id = schedule_groups.id
                WHERE schedule_groups.id = ?
                GROUP BY schedule_groups.id
                """,
                (group_id,),
            ).fetchone()
        return row_to_dict(row) if row else None

    def upsert_group(self, group_id: int | None, name: str, description: str, is_enabled: bool = True) -> int:
        timestamp = self.db.local_timestamp()
        if group_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE schedule_groups
                    SET name = ?, description = ?, is_enabled = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (name, description, int(is_enabled), timestamp, group_id),
                )
            return int(group_id)
        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO schedule_groups (name, description, is_enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (name, description, int(is_enabled), timestamp, timestamp),
            )
            return int(cursor.lastrowid)

    def delete_group(self, group_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute(
                "UPDATE workflow_schedules SET schedule_group_id = NULL, updated_at = ? WHERE schedule_group_id = ?",
                (self.db.local_timestamp(), group_id),
            )
            connection.execute("DELETE FROM schedule_groups WHERE id = ?", (group_id,))

    def set_group_enabled(self, group_id: int, is_enabled: bool) -> None:
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE schedule_groups
                SET is_enabled = ?, updated_at = ?
                WHERE id = ?
                """,
                (int(is_enabled), self.db.local_timestamp(), group_id),
            )


class ScheduleRunRepository:
    def __init__(self, db: DatabaseManager) -> None:
        self.db = db

    def add_run(
        self,
        schedule_id: int,
        workflow_id: int,
        device_id: int,
        trigger_source: str,
        status: str,
        message: str,
        metadata: dict[str, Any] | None,
        started_at: str,
        finished_at: str | None,
    ) -> int:
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO schedule_runs (
                    schedule_id, workflow_id, device_id, trigger_source, status, message,
                    metadata, started_at, finished_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    schedule_id,
                    workflow_id,
                    device_id,
                    trigger_source,
                    status,
                    message,
                    payload,
                    started_at,
                    finished_at,
                ),
            )
            return int(cursor.lastrowid)

    def list_runs(self, schedule_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
        conditions: list[str] = []
        values: list[Any] = []
        if schedule_id:
            conditions.append("schedule_runs.schedule_id = ?")
            values.append(schedule_id)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        values.append(limit)
        with self.db.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT
                    schedule_runs.*,
                    workflow_schedules.name AS schedule_name,
                    workflow_schedules.priority,
                    workflows.name AS workflow_name,
                    devices.name AS device_name
                FROM schedule_runs
                INNER JOIN workflow_schedules ON workflow_schedules.id = schedule_runs.schedule_id
                INNER JOIN workflows ON workflows.id = schedule_runs.workflow_id
                INNER JOIN devices ON devices.id = schedule_runs.device_id
                {where_clause}
                ORDER BY schedule_runs.started_at DESC, schedule_runs.id DESC
                LIMIT ?
                """,
                values,
            ).fetchall()
        return [row_to_dict(row) for row in rows]

    def list_recent_failed_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        with self.db.connection() as connection:
            rows = connection.execute(
                """
                SELECT
                    schedule_runs.*,
                    workflow_schedules.name AS schedule_name,
                    workflow_schedules.priority,
                    workflows.name AS workflow_name,
                    devices.name AS device_name
                FROM schedule_runs
                INNER JOIN workflow_schedules ON workflow_schedules.id = schedule_runs.schedule_id
                INNER JOIN workflows ON workflows.id = schedule_runs.workflow_id
                INNER JOIN devices ON devices.id = schedule_runs.device_id
                WHERE schedule_runs.status = 'failed'
                ORDER BY schedule_runs.started_at DESC, schedule_runs.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [row_to_dict(row) for row in rows]
