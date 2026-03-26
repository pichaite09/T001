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
        if device_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE devices
                    SET name = ?, serial = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (name, serial, notes, device_id),
                )
            return device_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO devices (name, serial, notes)
                VALUES (?, ?, ?)
                """,
                (name, serial, notes),
            )
            return int(cursor.lastrowid)

    def delete_device(self, device_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM devices WHERE id = ?", (device_id,))

    def update_status(self, device_id: int, status: str) -> None:
        with self.db.connection() as connection:
            connection.execute(
                """
                UPDATE devices
                SET last_status = ?, last_seen = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (status, device_id),
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
    ) -> int:
        if workflow_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE workflows
                    SET name = ?, description = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (name, description, int(is_active), workflow_id),
                )
            return workflow_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO workflows (name, description, is_active)
                VALUES (?, ?, ?)
                """,
                (name, description, int(is_active)),
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
    ) -> int:
        if step_id:
            with self.db.connection() as connection:
                connection.execute(
                    """
                    UPDATE steps
                    SET position = ?, name = ?, step_type = ?, parameters = ?, is_enabled = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (position, name, step_type, parameters, int(is_enabled), step_id),
                )
            return step_id

        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO steps (workflow_id, position, name, step_type, parameters, is_enabled)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (workflow_id, position, name, step_type, parameters, int(is_enabled)),
            )
            return int(cursor.lastrowid)

    def delete_step(self, step_id: int) -> None:
        with self.db.connection() as connection:
            connection.execute("DELETE FROM steps WHERE id = ?", (step_id,))

    def reorder_steps(self, workflow_id: int, ordered_step_ids: list[int]) -> None:
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
                    SET position = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND workflow_id = ?
                    """,
                    (position, step_id, workflow_id),
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
    ) -> int:
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        with self.db.connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO logs (workflow_id, device_id, level, status, message, metadata)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (workflow_id, device_id, level, status, message, payload),
            )
            return int(cursor.lastrowid)

    def list_logs(
        self,
        workflow_id: int | None = None,
        device_id: int | None = None,
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
        if status and status != "all":
            conditions.append("logs.status = ?")
            values.append(status)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = f"""
            SELECT
                logs.*,
                workflows.name AS workflow_name,
                devices.name AS device_name
            FROM logs
            LEFT JOIN workflows ON workflows.id = logs.workflow_id
            LEFT JOIN devices ON devices.id = logs.device_id
            {where_clause}
            ORDER BY logs.created_at DESC, logs.id DESC
            LIMIT ?
        """
        values.append(limit)

        with self.db.connection() as connection:
            rows = connection.execute(query, values).fetchall()
        return [row_to_dict(row) for row in rows]
