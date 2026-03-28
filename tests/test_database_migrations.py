from __future__ import annotations

import os
import tempfile
import unittest

from automation_studio.database import DatabaseManager


class DatabaseMigrationTests(unittest.TestCase):
    def test_init_schema_applies_all_migrations_and_columns(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            db = DatabaseManager(db_path)
            db.init_schema()

            self.assertEqual(db.current_schema_version(), 13)

            with db.connection() as connection:
                workflow_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(workflows)").fetchall()
                }
                step_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(steps)").fetchall()
                }
                log_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(logs)").fetchall()
                }
                telemetry_tables = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='step_telemetry'"
                ).fetchall()
                watcher_tables = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type='table'
                      AND name IN (
                        'watchers',
                        'watcher_telemetry',
                        'watcher_profiles',
                        'watcher_profile_items',
                        'workflow_watcher_profiles',
                        'device_platforms',
                        'accounts',
                        'account_aliases',
                        'schedule_groups',
                        'workflow_schedules',
                        'schedule_runs'
                      )
                    """
                ).fetchall()
                account_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(accounts)").fetchall()
                }
                schedule_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(workflow_schedules)").fetchall()
                }

            self.assertIn("definition_version", workflow_columns)
            self.assertIn("schema_version", step_columns)
            self.assertIn("watcher_id", log_columns)
            self.assertIn("display_name_normalized", account_columns)
            self.assertIn("username_normalized", account_columns)
            self.assertIn("login_id_normalized", account_columns)
            self.assertIn("schedule_group_id", schedule_columns)
            self.assertIn("priority", schedule_columns)
            self.assertTrue(telemetry_tables)
            self.assertEqual(len(watcher_tables), 11)
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)


if __name__ == "__main__":
    unittest.main()
