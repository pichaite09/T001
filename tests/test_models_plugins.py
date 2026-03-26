from __future__ import annotations

import unittest

from automation_studio.models import STEP_SCHEMA_VERSION, definition_for, migrate_step_parameters


class ModelPluginTests(unittest.TestCase):
    def test_plugin_definition_is_registered(self) -> None:
        definition = definition_for("plugin:echo_context")
        self.assertEqual(definition.label, "Plugin: Echo Context")
        self.assertTrue(definition.fields)

    def test_old_schema_parameter_aliases_are_migrated(self) -> None:
        migrated = migrate_step_parameters(
            "shell",
            {"run_if": "True", "store_as": "result"},
            schema_version=1,
        )
        self.assertEqual(migrated["run_if_expression"], "True")
        self.assertEqual(migrated["result_variable"], "result")
        self.assertEqual(STEP_SCHEMA_VERSION, 2)


if __name__ == "__main__":
    unittest.main()
