from __future__ import annotations

from typing import Any


PLUGIN_KEY = "echo_context"
PLUGIN_LABEL = "Plugin: Echo Context"
PLUGIN_DESCRIPTION = "Sample custom plugin step that renders a template-like message into telemetry and logs."
PLUGIN_TEMPLATE = {
    "message": "Hello ${vars.get('user', 'guest')}",
    "write_variable": "last_plugin_message",
}
PLUGIN_FIELDS = [
    {
        "key": "message",
        "label": "Message",
        "field_type": "textarea",
        "default": "Hello ${vars.get('user', 'guest')}",
        "required": True,
    },
    {
        "key": "write_variable",
        "label": "Write Variable",
        "field_type": "text",
        "default": "last_plugin_message",
    },
]
PLUGIN_PRESETS = [
    {
        "label": "Echo User",
        "description": "Store a greeting using the current workflow variable state.",
        "parameters": {
            "message": "Hello ${vars.get('user', 'guest')}",
            "write_variable": "last_plugin_message",
        },
    }
]


def run(device: Any, parameters: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    message = str(parameters.get("message", ""))
    variable_name = str(parameters.get("write_variable", "")).strip()
    if variable_name:
        context["vars"][variable_name] = message
    return {
        "plugin": PLUGIN_KEY,
        "message": message,
        "stored_variable": variable_name,
    }
