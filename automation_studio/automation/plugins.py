from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path
from types import ModuleType
from typing import Any


PLUGIN_PACKAGE = "automation_studio.custom_steps"
PLUGIN_PREFIX = "plugin:"


def _plugin_package_paths() -> list[str]:
    package_path = Path(__file__).resolve().parents[1] / "custom_steps"
    if not package_path.exists():
        return []
    return [str(package_path)]


def _iter_plugin_modules() -> list[ModuleType]:
    modules: list[ModuleType] = []
    for module_info in pkgutil.iter_modules(_plugin_package_paths()):
        if module_info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{PLUGIN_PACKAGE}.{module_info.name}")
        modules.append(module)
    return modules


def discover_plugin_metadata() -> list[dict[str, Any]]:
    metadata: list[dict[str, Any]] = []
    for module in _iter_plugin_modules():
        plugin_key = getattr(module, "PLUGIN_KEY", "").strip()
        if not plugin_key:
            continue
        metadata.append(
            {
                "key": plugin_key,
                "label": getattr(module, "PLUGIN_LABEL", plugin_key),
                "description": getattr(module, "PLUGIN_DESCRIPTION", "Custom plugin step"),
                "template": getattr(module, "PLUGIN_TEMPLATE", {}),
                "fields": getattr(module, "PLUGIN_FIELDS", []),
                "presets": getattr(module, "PLUGIN_PRESETS", []),
            }
        )
    return metadata


def plugin_handler_for_step_type(step_type: str):
    if not step_type.startswith(PLUGIN_PREFIX):
        return None
    plugin_key = step_type[len(PLUGIN_PREFIX) :]
    for module in _iter_plugin_modules():
        if getattr(module, "PLUGIN_KEY", "").strip() == plugin_key:
            return getattr(module, "run", None)
    return None
