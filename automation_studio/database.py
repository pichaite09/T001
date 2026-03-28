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
            (8, self._migration_008_watcher_profiles),
            (9, self._migration_009_accounts),
            (10, self._migration_010_account_aliases),
            (11, self._migration_011_account_identity_case_sensitive),
            (12, self._migration_012_workflow_schedules),
            (13, self._migration_013_schedule_groups_and_priority),
            (14, self._migration_014_device_runtime_info),
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

    def _migration_008_watcher_profiles(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS watcher_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL DEFAULT '',
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS watcher_profile_items (
                profile_id INTEGER NOT NULL,
                watcher_id INTEGER NOT NULL,
                position INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (profile_id, watcher_id),
                FOREIGN KEY(profile_id) REFERENCES watcher_profiles(id) ON DELETE CASCADE,
                FOREIGN KEY(watcher_id) REFERENCES watchers(id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS workflow_watcher_profiles (
                workflow_id INTEGER NOT NULL,
                profile_id INTEGER NOT NULL,
                PRIMARY KEY (workflow_id, profile_id),
                FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
                FOREIGN KEY(profile_id) REFERENCES watcher_profiles(id) ON DELETE CASCADE
            )
            """
        )

    def _migration_009_accounts(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS device_platforms (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id INTEGER NOT NULL,
                platform_key TEXT NOT NULL,
                platform_name TEXT NOT NULL,
                package_name TEXT NOT NULL DEFAULT '',
                switch_workflow_id INTEGER,
                current_account_id INTEGER,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                UNIQUE(device_id, platform_key),
                FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE,
                FOREIGN KEY(switch_workflow_id) REFERENCES workflows(id) ON DELETE SET NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_platform_id INTEGER NOT NULL,
                display_name TEXT NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                login_id TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                UNIQUE(device_platform_id, display_name),
                FOREIGN KEY(device_platform_id) REFERENCES device_platforms(id) ON DELETE CASCADE
            )
            """
        )

    def _migration_010_account_aliases(self, connection: sqlite3.Connection) -> None:
        if not self._column_exists(connection, "accounts", "display_name_normalized"):
            connection.execute(
                """
                ALTER TABLE accounts
                ADD COLUMN display_name_normalized TEXT NOT NULL DEFAULT ''
                """
            )
        if not self._column_exists(connection, "accounts", "username_normalized"):
            connection.execute(
                """
                ALTER TABLE accounts
                ADD COLUMN username_normalized TEXT NOT NULL DEFAULT ''
                """
            )
        if not self._column_exists(connection, "accounts", "login_id_normalized"):
            connection.execute(
                """
                ALTER TABLE accounts
                ADD COLUMN login_id_normalized TEXT NOT NULL DEFAULT ''
                """
            )

        if self._table_exists(connection, "accounts"):
            connection.execute(
                """
                UPDATE accounts
                SET
                    display_name_normalized = LOWER(LTRIM(TRIM(display_name), '@')),
                    username_normalized = LOWER(LTRIM(TRIM(username), '@')),
                    login_id_normalized = LOWER(LTRIM(TRIM(login_id), '@'))
                """
            )

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS account_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id INTEGER NOT NULL,
                device_platform_id INTEGER NOT NULL,
                alias_name TEXT NOT NULL,
                alias_normalized TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                UNIQUE(device_platform_id, alias_normalized),
                UNIQUE(account_id, alias_normalized),
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
                FOREIGN KEY(device_platform_id) REFERENCES device_platforms(id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_platform_display_name_normalized
            ON accounts(device_platform_id, display_name_normalized)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_platform_username_normalized
            ON accounts(device_platform_id, username_normalized)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_accounts_platform_login_id_normalized
            ON accounts(device_platform_id, login_id_normalized)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_account_aliases_platform_normalized
            ON account_aliases(device_platform_id, alias_normalized)
            """
        )

    def _migration_011_account_identity_case_sensitive(self, connection: sqlite3.Connection) -> None:
        if self._table_exists(connection, "accounts"):
            connection.execute(
                """
                UPDATE accounts
                SET
                    display_name_normalized = LTRIM(TRIM(display_name), '@'),
                    username_normalized = LTRIM(TRIM(username), '@'),
                    login_id_normalized = LTRIM(TRIM(login_id), '@')
                """
            )
        if self._table_exists(connection, "account_aliases"):
            connection.execute(
                """
                UPDATE account_aliases
                SET alias_normalized = LTRIM(TRIM(alias_name), '@')
                """
            )

    def _migration_012_workflow_schedules(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS workflow_schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                workflow_id INTEGER NOT NULL,
                device_id INTEGER NOT NULL,
                device_platform_id INTEGER,
                account_id INTEGER,
                use_current_account INTEGER NOT NULL DEFAULT 0,
                schedule_type TEXT NOT NULL DEFAULT 'interval',
                schedule_json TEXT NOT NULL DEFAULT '{}',
                next_run_at TEXT,
                last_run_at TEXT,
                last_status TEXT NOT NULL DEFAULT 'idle',
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
                FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE,
                FOREIGN KEY(device_platform_id) REFERENCES device_platforms(id) ON DELETE SET NULL,
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE SET NULL
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                workflow_id INTEGER NOT NULL,
                device_id INTEGER NOT NULL,
                trigger_source TEXT NOT NULL DEFAULT 'timer',
                status TEXT NOT NULL DEFAULT 'running',
                message TEXT NOT NULL DEFAULT '',
                metadata TEXT NOT NULL DEFAULT '{}',
                started_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                finished_at TEXT,
                FOREIGN KEY(schedule_id) REFERENCES workflow_schedules(id) ON DELETE CASCADE,
                FOREIGN KEY(workflow_id) REFERENCES workflows(id) ON DELETE CASCADE,
                FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_workflow_schedules_due
            ON workflow_schedules(is_enabled, next_run_at)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_schedule_runs_schedule_started
            ON schedule_runs(schedule_id, started_at DESC, id DESC)
            """
        )

    def _migration_013_schedule_groups_and_priority(self, connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL DEFAULT '',
                is_enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'localtime'))
            )
            """
        )
        if not self._column_exists(connection, "workflow_schedules", "schedule_group_id"):
            connection.execute(
                """
                ALTER TABLE workflow_schedules
                ADD COLUMN schedule_group_id INTEGER
                """
            )
        if not self._column_exists(connection, "workflow_schedules", "priority"):
            connection.execute(
                """
                ALTER TABLE workflow_schedules
                ADD COLUMN priority INTEGER NOT NULL DEFAULT 100
                """
            )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_workflow_schedules_group_priority
            ON workflow_schedules(schedule_group_id, is_enabled, next_run_at, priority, id)
            """
        )

    def _migration_014_device_runtime_info(self, connection: sqlite3.Connection) -> None:
        if not self._column_exists(connection, "devices", "last_info_json"):
            connection.execute(
                """
                ALTER TABLE devices
                ADD COLUMN last_info_json TEXT NOT NULL DEFAULT '{}'
                """
            )
