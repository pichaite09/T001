from __future__ import annotations

import json
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
