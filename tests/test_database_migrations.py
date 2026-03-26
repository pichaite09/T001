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

            self.assertEqual(db.current_schema_version(), 4)

            with db.connection() as connection:
                workflow_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(workflows)").fetchall()
                }
                step_columns = {
                    row["name"]
                    for row in connection.execute("PRAGMA table_info(steps)").fetchall()
                }
                telemetry_tables = connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='step_telemetry'"
                ).fetchall()

            self.assertIn("definition_version", workflow_columns)
            self.assertIn("schema_version", step_columns)
            self.assertTrue(telemetry_tables)
        finally:
            if os.path.exists(db_path):
                os.remove(db_path)


if __name__ == "__main__":
    unittest.main()
