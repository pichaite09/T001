from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator


MigrationFunc = Callable[[sqlite3.Connection], None]


class DatabaseManager:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def init_schema(self) -> None:
        with self.connection() as connection:
            self._ensure_migration_table(connection)
            applied_versions = self._applied_migration_versions(connection)
            for version, migration in self._migrations():
                if version in applied_versions:
                    continue
                migration(connection)
                connection.execute(
                    """
                    INSERT INTO schema_migrations (version, name, applied_at)
                    VALUES (?, ?, ?)
                    """,
                    (version, migration.__name__, self.local_timestamp()),
                )

    def local_timestamp(self) -> str:
        return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")

    def current_schema_version(self) -> int:
        with self.connection() as connection:
            self._ensure_migration_table(connection)
            row = connection.execute("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations").fetchone()
        return int(row["version"]) if row else 0

    def _ensure_migration_table(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
            """
        )

    def _applied_migration_versions(self, connection: sqlite3.Connection) -> set[int]:
        rows = connection.execute("SELECT version FROM schema_migrations").fetchall()
        return {int(row["version"]) for row in rows}

    def _migrations(self) -> list[tuple[int, MigrationFunc]]:
        return [
            (1, self._migration_001_base_schema),
            (2, self._migration_002_workflow_definition_version),
            (3, self._migration_003_step_schema_version),
            (4, self._migration_004_step_telemetry),
            (5, self._migration_005_local_timestamps),
            (6, self._migration_006_watchers),
            (7, self._migration_007_log_watcher_index),
        ]

    def _table_exists(self, connection: sqlite3.Connection, table_name: str) -> bool:
        row = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (table_name,),
        ).fetchone()
        return row is not None

    def _column_exists(self, connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
        if not self._table_exists(connection, table_name):
            return False
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(str(row["name"]) == column_name for row in rows)

    def _migration_001_base_schema(self, connection: sqlite3.Connection) -> None:
        schema = """
        CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            serial TEXT NOT NULL UNIQUE,
            notes TEXT DEFAULT '',
            last_status TEXT DEFAULT 'unknown',
            last_seen TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS workflows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_id INTEGER NOT NULL,
            position INTEGER NOT NULL DEFAULT 1,
            name TEXT NOT NULL,
            step_type TEXT NOT NULL,
            parameters TEXT NOT NULL DEFAULT '{}',
            is_enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_id INTEGER,
            device_id INTEGER,
            level TEXT NOT NULL DEFAULT 'INFO',
            status TEXT NOT NULL DEFAULT 'info',
            message TEXT NOT NULL,
            metadata TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE SET NULL,
            FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE SET NULL
        );
        """
        connection.executescript(schema)

    def _migration_002_workflow_definition_version(self, connection: sqlite3.Connection) -> None:
        if not self._column_exists(connection, "workflows", "definition_version"):
            connection.execute(
                """
                ALTER TABLE workflows
                ADD COLUMN definition_version INTEGER NOT NULL DEFAULT 1
                """
            )

    def _migration_003_step_schema_version(self, connection: sqlite3.Connection) -> None:
        if not self._column_exists(connection, "steps", "schema_version"):
            connection.execute(
                """
                ALTER TABLE steps
                ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1
                """
            )

    def _migration_004_step_telemetry(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS step_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workflow_id INTEGER,
                device_id INTEGER,
                step_type TEXT NOT NULL,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                continued_failure_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0,
                total_duration_ms INTEGER NOT NULL DEFAULT 0,
                last_error TEXT DEFAULT '',
                last_run_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                UNIQUE(workflow_id, device_id, step_type),
                FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
                FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
            )
            """
        )

    def _migration_005_local_timestamps(self, connection: sqlite3.Connection) -> None:
        for table_name, columns in (
            ("schema_migrations", ("applied_at",)),
            ("devices", ("last_seen", "created_at", "updated_at")),
            ("workflows", ("created_at", "updated_at")),
            ("steps", ("created_at", "updated_at")),
            ("logs", ("created_at",)),
            ("step_telemetry", ("last_run_at",)),
        ):
            if not self._table_exists(connection, table_name):
                continue
            for column in columns:
                if not self._column_exists(connection, table_name, column):
                    continue
                connection.execute(
                    f"""
                    UPDATE {table_name}
                    SET {column} = datetime({column}, 'localtime')
                    WHERE {column} IS NOT NULL AND {column} <> ''
                    """
                )

    def _migration_006_watchers(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS watchers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                scope_type TEXT NOT NULL DEFAULT 'global',
                scope_id INTEGER,
                condition_type TEXT NOT NULL,
                condition_json TEXT NOT NULL DEFAULT '{}',
                action_type TEXT NOT NULL,
                action_json TEXT NOT NULL DEFAULT '{}',
                policy_json TEXT NOT NULL DEFAULT '{}',
                is_enabled INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 100,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
            """
        )

    def _migration_007_log_watcher_index(self, connection: sqlite3.Connection) -> None:
        if not self._column_exists(connection, "logs", "watcher_id"):
            connection.execute(
                """
                ALTER TABLE logs
                ADD COLUMN watcher_id INTEGER
                """
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS watcher_telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                watcher_id INTEGER NOT NULL,
                workflow_id INTEGER,
                device_id INTEGER,
                trigger_count INTEGER NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT DEFAULT '',
                last_triggered_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                UNIQUE(watcher_id, workflow_id, device_id),
                FOREIGN KEY(watcher_id) REFERENCES watchers(id) ON DELETE CASCADE,
                FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
                FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
            )
            """
        )
