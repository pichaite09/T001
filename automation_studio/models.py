from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from automation_studio.automation.plugins import discover_plugin_metadata


VARIABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
WORKFLOW_DEFINITION_VERSION = 2
STEP_SCHEMA_VERSION = 2
WATCHER_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class StepField:
    key: str
    label: str
    field_type: str = "text"
    default: Any = ""
    required: bool = False
    placeholder: str = ""
    help_text: str = ""
    options: tuple[tuple[Any, str], ...] = ()
    min_value: float | int | None = None
    max_value: float | int | None = None
    decimals: int = 2


@dataclass(frozen=True)
class StepPreset:
    label: str
    description: str
    parameters: dict[str, Any]


@dataclass(frozen=True)
class StepDefinition:
    key: str
    label: str
    description: str
    template: dict[str, Any]
    fields: tuple[StepField, ...] = field(default_factory=tuple)
    presets: tuple[StepPreset, ...] = field(default_factory=tuple)

    def template_json(self) -> str:
        return json.dumps(self.template, indent=2, ensure_ascii=False)


@dataclass(frozen=True)
class WatcherPreset:
    label: str
    description: str
    name: str
    condition_type: str
    condition: dict[str, Any]
    action_type: str
    action: dict[str, Any]
    policy: dict[str, Any]


EXECUTION_POLICY_FIELDS = (
    StepField(
        "step_timeout_seconds",
        "Step Timeout (sec)",
        field_type="float",
        default=0.0,
        min_value=0,
        decimals=2,
    ),
    StepField(
        "retry_count",
        "Retry Count",
        field_type="int",
        default=0,
        min_value=0,
    ),
    StepField(
        "retry_delay_seconds",
        "Retry Delay (sec)",
        field_type="float",
        default=1.0,
        min_value=0,
        decimals=2,
    ),
    StepField(
        "continue_on_error",
        "Continue On Error",
        field_type="bool",
        default=False,
    ),
    StepField(
        "on_failure",
        "On Failure",
        field_type="combo",
        default="stop",
        options=(
            ("stop", "Stop Workflow"),
            ("skip", "Skip Step"),
            ("take_screenshot", "Take Screenshot"),
        ),
    ),
    StepField(
        "capture_hierarchy_on_failure",
        "Capture Hierarchy On Failure",
        field_type="bool",
        default=False,
    ),
)


FLOW_CONTROL_FIELDS = (
    StepField(
        "run_if_expression",
        "Run If Expression",
        field_type="textarea",
        default="",
        placeholder="vars.get('otp') and vars.get('otp') != ''",
    ),
    StepField(
        "repeat_times",
        "Repeat Times",
        field_type="int",
        default=1,
        min_value=1,
    ),
    StepField(
        "repeat_delay_seconds",
        "Repeat Delay (sec)",
        field_type="float",
        default=0.0,
        min_value=0,
        decimals=2,
    ),
    StepField(
        "result_variable",
        "Store Result As",
        field_type="text",
        default="",
        placeholder="last_click_result",
    ),
)


def default_execution_policy() -> dict[str, Any]:
    return {field.key: field.default for field in EXECUTION_POLICY_FIELDS}


def default_flow_control() -> dict[str, Any]:
    return {field.key: field.default for field in FLOW_CONTROL_FIELDS}


WATCHER_SCOPE_OPTIONS = (
    ("global", "Global"),
    ("workflow", "Workflow"),
    ("device", "Device"),
)

WATCHER_CONDITION_TEMPLATES: dict[str, dict[str, Any]] = {
    "selector_exists": {"text": "Allow", "timeout": 0.0},
    "selector_gone": {"resource_id": "com.example:id/loading", "timeout": 0.0},
    "text_exists": {"text": "Allow", "timeout": 0.0},
    "text_contains": {"resource_id": "com.example:id/message", "expected_text": "Success", "timeout": 0.0},
    "app_in_foreground": {"package": "com.example.app"},
    "package_changed": {},
    "elapsed_time": {"seconds": 30},
    "variable_changed": {"variable_name": "otp"},
    "expression": {"expression": "vars.get('need_popup_guard') == True"},
}

WATCHER_ACTION_TEMPLATES: dict[str, dict[str, Any]] = {
    "run_step": {
        "step_type": "click",
        "parameters": {"text": "Allow", "timeout": 1.0},
    },
    "action_chain": {
        "actions": [
            {"action_type": "take_screenshot", "action": {"filename_prefix": "popup_before_close"}},
            {"action_type": "press_back", "action": {}},
        ]
    },
    "press_back": {},
    "take_screenshot": {"filename_prefix": "watcher_event"},
    "dump_hierarchy": {"filename_prefix": "watcher_view"},
    "stop_workflow": {"reason": "Watcher requested workflow stop"},
    "set_variable": {"variable_name": "watcher_triggered", "value": "1"},
}

WATCHER_CONDITION_FIELDS: dict[str, tuple[StepField, ...]] = {
    "selector_exists": (
        StepField("text", "Text", placeholder="Allow"),
        StepField("resource_id", "Resource ID", placeholder="com.example:id/allow"),
        StepField("xpath", "XPath", placeholder='//*[@text="Allow"]'),
        StepField("description", "Description", placeholder="Allow"),
        StepField("class_name", "Class Name", placeholder="android.widget.Button"),
        StepField("timeout", "Timeout", field_type="float", default=0.0, min_value=0, decimals=1),
    ),
    "selector_gone": (
        StepField("text", "Text", placeholder="Loading"),
        StepField("resource_id", "Resource ID", placeholder="com.example:id/loading"),
        StepField("xpath", "XPath", placeholder='//*[@text="Loading"]'),
        StepField("description", "Description", placeholder="Loading"),
        StepField("class_name", "Class Name", placeholder="android.widget.ProgressBar"),
        StepField("timeout", "Timeout", field_type="float", default=0.0, min_value=0, decimals=1),
    ),
    "text_exists": (
        StepField("text", "Text", required=True, placeholder="Allow"),
        StepField("timeout", "Timeout", field_type="float", default=0.0, min_value=0, decimals=1),
    ),
    "text_contains": (
        StepField("text", "Selector Text", placeholder="Status"),
        StepField("resource_id", "Resource ID", placeholder="com.example:id/status"),
        StepField("xpath", "XPath", placeholder='//*[@resource-id="com.example:id/status"]'),
        StepField("description", "Description", placeholder="Status"),
        StepField("class_name", "Class Name", placeholder="android.widget.TextView"),
        StepField("expected_text", "Expected Text", required=True, placeholder="Success"),
        StepField("timeout", "Timeout", field_type="float", default=0.0, min_value=0, decimals=1),
    ),
    "app_in_foreground": (
        StepField("package", "Package Name", required=True, placeholder="com.example.app"),
    ),
    "package_changed": (),
    "elapsed_time": (
        StepField("seconds", "Elapsed Seconds", field_type="float", default=30.0, min_value=0, decimals=1),
    ),
    "variable_changed": (
        StepField("variable_name", "Variable Name", required=True, placeholder="otp"),
    ),
    "expression": (
        StepField(
            "expression",
            "Expression",
            field_type="textarea",
            default="vars.get('need_popup_guard') == True",
            required=True,
            placeholder="vars.get('need_popup_guard') == True",
        ),
    ),
}

WATCHER_ACTION_FIELDS: dict[str, tuple[StepField, ...]] = {
    "run_step": (),
    "action_chain": (
        StepField(
            "actions",
            "Actions JSON",
            field_type="textarea",
            default='[\n  {"action_type": "take_screenshot", "action": {"filename_prefix": "popup_before_close"}},\n  {"action_type": "press_back", "action": {}}\n]',
            required=True,
            placeholder='[{"action_type": "press_back", "action": {}}]',
        ),
    ),
    "press_back": (),
    "take_screenshot": (
        StepField("filename_prefix", "Filename Prefix", default="watcher_event", required=True, placeholder="watcher_event"),
    ),
    "dump_hierarchy": (
        StepField("filename_prefix", "Filename Prefix", default="watcher_view", required=True, placeholder="watcher_view"),
    ),
    "stop_workflow": (
        StepField("reason", "Reason", default="Watcher requested workflow stop", required=True, placeholder="Popup loop detected"),
    ),
    "set_variable": (
        StepField("variable_name", "Variable Name", required=True, placeholder="watcher_triggered"),
        StepField("value", "Value", default="1", placeholder="1"),
    ),
}

WATCHER_STAGE_OPTIONS = (
    ("before_step", "Before Step"),
    ("after_step", "After Step"),
    ("during_wait", "During Wait"),
)


def default_watcher_policy() -> dict[str, Any]:
    return {
        "cooldown_seconds": 3.0,
        "debounce_count": 1,
        "max_triggers_per_run": 0,
        "stop_after_match": False,
        "match_mode": "first_match",
        "active_stages": ["before_step", "after_step", "during_wait"],
    }


def watcher_condition_template(condition_type: str) -> dict[str, Any]:
    return dict(WATCHER_CONDITION_TEMPLATES.get(condition_type, {}))


def watcher_action_template(action_type: str) -> dict[str, Any]:
    return dict(WATCHER_ACTION_TEMPLATES.get(action_type, {}))


def watcher_presets() -> tuple[WatcherPreset, ...]:
    return (
        WatcherPreset(
            label="Auto Allow Popup",
            description="When an Allow button appears, click it once and continue the workflow.",
            name="Auto Allow Popup",
            condition_type="selector_exists",
            condition={"text": "Allow", "timeout": 0.0},
            action_type="run_step",
            action={"step_type": "click", "parameters": {"text": "Allow", "timeout": 1.0}},
            policy={"cooldown_seconds": 2.0, "max_triggers_per_run": 3, "stop_after_match": False, "active_stages": ["before_step", "after_step", "during_wait"]},
        ),
        WatcherPreset(
            label="Auto Back Tip Panel",
            description="If a tip panel appears, press Back to dismiss it.",
            name="Auto Back Tip Panel",
            condition_type="selector_exists",
            condition={"resource_id": "product_tip_panel", "timeout": 0.0},
            action_type="press_back",
            action={},
            policy={"cooldown_seconds": 2.0, "max_triggers_per_run": 5, "stop_after_match": False, "active_stages": ["before_step", "after_step", "during_wait"]},
        ),
        WatcherPreset(
            label="Screenshot On Popup",
            description="Capture evidence whenever a popup selector appears.",
            name="Screenshot On Popup",
            condition_type="selector_exists",
            condition={"text": "Allow", "timeout": 0.0},
            action_type="take_screenshot",
            action={"filename_prefix": "popup_detected"},
            policy={"cooldown_seconds": 5.0, "debounce_count": 1, "max_triggers_per_run": 10, "stop_after_match": False, "match_mode": "first_match", "active_stages": ["before_step", "after_step", "during_wait"]},
        ),
        WatcherPreset(
            label="Screenshot Then Back",
            description="Capture the current popup first, then dismiss it with Back.",
            name="Screenshot Then Back",
            condition_type="selector_exists",
            condition={"text": "Allow", "timeout": 0.0},
            action_type="action_chain",
            action={
                "actions": [
                    {"action_type": "take_screenshot", "action": {"filename_prefix": "popup_before_back"}},
                    {"action_type": "press_back", "action": {}},
                ]
            },
            policy={"cooldown_seconds": 3.0, "debounce_count": 1, "max_triggers_per_run": 5, "stop_after_match": False, "match_mode": "continue", "active_stages": ["before_step", "after_step", "during_wait"]},
        ),
        WatcherPreset(
            label="Stop After 10 Minutes",
            description="Stop the current workflow after ten minutes of runtime.",
            name="Stop After 10 Minutes",
            condition_type="elapsed_time",
            condition={"seconds": 600},
            action_type="stop_workflow",
            action={"reason": "Workflow exceeded 10 minutes"},
            policy={"cooldown_seconds": 0.0, "debounce_count": 1, "max_triggers_per_run": 1, "stop_after_match": True, "match_mode": "first_match", "active_stages": ["before_step", "after_step", "during_wait"]},
        ),
    )


def _selector_fields(timeout_default: int = 10) -> tuple[StepField, ...]:
    return (
        StepField("text", "Text", placeholder="Login"),
        StepField("resource_id", "Resource ID", placeholder="com.example:id/login"),
        StepField("xpath", "XPath", placeholder='//*[@text="Login"]'),
        StepField("description", "Description", placeholder="Open navigation"),
        StepField("class_name", "Class Name", placeholder="android.widget.Button"),
        StepField("timeout", "Timeout", field_type="float", default=float(timeout_default), min_value=0, decimals=1),
    )


def _direction_fields(default_repeat: int = 1) -> tuple[StepField, ...]:
    return (
        StepField(
            "direction",
            "Direction",
            field_type="combo",
            default="up",
            options=(("up", "Up"), ("down", "Down"), ("left", "Left"), ("right", "Right"), ("", "Custom")),
        ),
        StepField("scale", "Scale", field_type="float", default=0.6, min_value=0.05, max_value=1.0, decimals=2),
        StepField("anchor_x", "Anchor X", field_type="float", default=0.5, min_value=0, max_value=1, decimals=2),
        StepField("anchor_y", "Anchor Y", field_type="float", default=0.5, min_value=0, max_value=1, decimals=2),
        StepField("margin_ratio", "Margin Ratio", field_type="float", default=0.1, min_value=0, max_value=0.45, decimals=2),
        StepField("x1_ratio", "Start X Ratio", field_type="float", default=0.5, min_value=0, max_value=1, decimals=2),
        StepField("y1_ratio", "Start Y Ratio", field_type="float", default=0.8, min_value=0, max_value=1, decimals=2),
        StepField("x2_ratio", "End X Ratio", field_type="float", default=0.5, min_value=0, max_value=1, decimals=2),
        StepField("y2_ratio", "End Y Ratio", field_type="float", default=0.2, min_value=0, max_value=1, decimals=2),
        StepField("x1", "Start X", field_type="int", default=540, min_value=0),
        StepField("y1", "Start Y", field_type="int", default=1600, min_value=0),
        StepField("x2", "End X", field_type="int", default=540, min_value=0),
        StepField("y2", "End Y", field_type="int", default=400, min_value=0),
        StepField("duration", "Duration", field_type="float", default=0.2, min_value=0, decimals=2),
        StepField("repeat", "Repeat", field_type="int", default=default_repeat, min_value=1),
        StepField("pause_seconds", "Pause Seconds", field_type="float", default=0.0, min_value=0, decimals=2),
    )


def _scroll_to_selector_fields() -> tuple[StepField, ...]:
    return (
        *_selector_fields(timeout_default=0),
        StepField(
            "direction",
            "Direction",
            field_type="combo",
            default="up",
            options=(("up", "Up"), ("down", "Down"), ("left", "Left"), ("right", "Right"), ("", "Custom")),
        ),
        StepField("max_swipes", "Max Swipes", field_type="int", default=8, min_value=1),
        StepField("scale", "Scale", field_type="float", default=0.75, min_value=0.05, max_value=1.0, decimals=2),
        StepField("anchor_x", "Anchor X", field_type="float", default=0.5, min_value=0, max_value=1, decimals=2),
        StepField("anchor_y", "Anchor Y", field_type="float", default=0.55, min_value=0, max_value=1, decimals=2),
        StepField("margin_ratio", "Margin Ratio", field_type="float", default=0.1, min_value=0, max_value=0.45, decimals=2),
        StepField("x1_ratio", "Start X Ratio", field_type="float", default=0.5, min_value=0, max_value=1, decimals=2),
        StepField("y1_ratio", "Start Y Ratio", field_type="float", default=0.8, min_value=0, max_value=1, decimals=2),
        StepField("x2_ratio", "End X Ratio", field_type="float", default=0.5, min_value=0, max_value=1, decimals=2),
        StepField("y2_ratio", "End Y Ratio", field_type="float", default=0.2, min_value=0, max_value=1, decimals=2),
        StepField("x1", "Start X", field_type="int", default=540, min_value=0),
        StepField("y1", "Start Y", field_type="int", default=1600, min_value=0),
        StepField("x2", "End X", field_type="int", default=540, min_value=0),
        StepField("y2", "End Y", field_type="int", default=400, min_value=0),
        StepField("duration", "Duration", field_type="float", default=0.18, min_value=0, decimals=2),
        StepField("pause_seconds", "Pause Seconds", field_type="float", default=0.25, min_value=0, decimals=2),
    )


STEP_DEFINITIONS = [
    StepDefinition(
        key="launch_app",
        label="Launch App",
        description="Start an Android app by package name.",
        template={"package": "com.example.app"},
        fields=(StepField("package", "Package Name", required=True, placeholder="com.example.app"),),
    ),
    StepDefinition(
        key="stop_app",
        label="Stop App",
        description="Stop an Android app by package name.",
        template={"package": "com.example.app"},
        fields=(StepField("package", "Package Name", required=True, placeholder="com.example.app"),),
    ),
    StepDefinition(
        key="tap",
        label="Tap Coordinates",
        description="Tap an absolute screen coordinate.",
        template={"x": 540, "y": 1200},
        fields=(
            StepField("x", "X", field_type="int", default=540, required=True, min_value=0),
            StepField("y", "Y", field_type="int", default=1200, required=True, min_value=0),
        ),
        presets=(StepPreset("Tap Center", "Tap the middle of the screen.", {"x": 540, "y": 1200}),),
    ),
    StepDefinition(
        key="click",
        label="Click Selector",
        description="Click a UI element by selector or use fallback coordinates.",
        template={"text": "Login", "timeout": 10},
        fields=(
            *_selector_fields(),
            StepField("x", "Fallback X", field_type="int", default=0, min_value=0),
            StepField("y", "Fallback Y", field_type="int", default=0, min_value=0),
        ),
        presets=(
            StepPreset("By Text", "Click by text selector.", {"text": "Login", "timeout": 10}),
            StepPreset("By Resource ID", "Click by resource id.", {"resource_id": "com.example:id/login", "timeout": 10}),
        ),
    ),
    StepDefinition(
        key="long_click",
        label="Long Click",
        description="Long press an element or absolute coordinates.",
        template={"text": "Hold Me", "timeout": 10, "duration": 0.8},
        fields=(
            *_selector_fields(),
            StepField("x", "Fallback X", field_type="int", default=0, min_value=0),
            StepField("y", "Fallback Y", field_type="int", default=0, min_value=0),
            StepField("duration", "Duration", field_type="float", default=0.8, min_value=0, decimals=2),
        ),
        presets=(StepPreset("Long Click By Text", "Long click an element found by text.", {"text": "Hold Me", "timeout": 10, "duration": 0.8}),),
    ),
    StepDefinition(
        key="double_click",
        label="Double Click",
        description="Double tap an element or fallback coordinates.",
        template={"text": "Open", "timeout": 10, "interval_seconds": 0.15},
        fields=(
            *_selector_fields(),
            StepField("x", "Fallback X", field_type="int", default=0, min_value=0),
            StepField("y", "Fallback Y", field_type="int", default=0, min_value=0),
            StepField("interval_seconds", "Interval (sec)", field_type="float", default=0.15, min_value=0, decimals=2),
        ),
    ),
    StepDefinition(
        key="set_text",
        label="Set Text",
        description="Fill text into a selected field or the focused field.",
        template={"resource_id": "com.example:id/input", "text": "demo", "clear_first": True},
        fields=(
            *_selector_fields(),
            StepField("text", "Input Text", required=True, placeholder="demo"),
            StepField("clear_first", "Clear First", field_type="bool", default=True),
        ),
        presets=(
            StepPreset(
                "Field By Resource ID",
                "Fill a field by resource id.",
                {"resource_id": "com.example:id/input", "text": "demo", "clear_first": True},
            ),
            StepPreset("Focused Field", "Fill the currently focused field.", {"text": "demo", "clear_first": True}),
        ),
    ),
    StepDefinition(
        key="wait",
        label="Wait",
        description="Pause the workflow for a number of seconds.",
        template={"seconds": 2},
        fields=(StepField("seconds", "Seconds", field_type="float", default=2.0, required=True, min_value=0, decimals=2),),
        presets=(
            StepPreset("Short Wait", "Pause for 1 second.", {"seconds": 1}),
            StepPreset("Medium Wait", "Pause for 3 seconds.", {"seconds": 3}),
        ),
    ),
    StepDefinition(
        key="wait_for_text",
        label="Wait For Text",
        description="Wait until a selector appears.",
        template={"text": "Success", "timeout": 15},
        fields=_selector_fields(timeout_default=15),
        presets=(StepPreset("Wait Success Text", "Wait for a success message.", {"text": "Success", "timeout": 15}),),
    ),
    StepDefinition(
        key="wait_for_element",
        label="Wait For Element",
        description="Wait for an element to appear or disappear.",
        template={"resource_id": "com.example:id/result", "timeout": 15, "desired_state": "exists"},
        fields=(
            *_selector_fields(timeout_default=15),
            StepField(
                "desired_state",
                "Desired State",
                field_type="combo",
                default="exists",
                options=(("exists", "Exists"), ("gone", "Gone")),
            ),
            StepField("poll_interval_seconds", "Poll Interval", field_type="float", default=0.5, min_value=0.05, decimals=2),
        ),
    ),
    StepDefinition(
        key="swipe",
        label="Swipe",
        description="Swipe using direction, ratios, or absolute coordinates.",
        template={"direction": "up", "scale": 0.6, "anchor_x": 0.5, "anchor_y": 0.5, "duration": 0.2, "repeat": 1},
        fields=_direction_fields(default_repeat=1),
        presets=(
            StepPreset("Swipe Up", "Standard upward swipe.", {"direction": "up", "scale": 0.6, "anchor_x": 0.5, "anchor_y": 0.5, "duration": 0.2, "repeat": 1}),
            StepPreset("Swipe Down", "Standard downward swipe.", {"direction": "down", "scale": 0.6, "anchor_x": 0.5, "anchor_y": 0.5, "duration": 0.2, "repeat": 1}),
            StepPreset("Custom Ratios", "Swipe using screen ratios.", {"direction": "", "x1_ratio": 0.5, "y1_ratio": 0.8, "x2_ratio": 0.5, "y2_ratio": 0.2, "duration": 0.2, "repeat": 1}),
        ),
    ),
    StepDefinition(
        key="scroll",
        label="Scroll",
        description="Scroll by repeatedly swiping in a direction.",
        template={"direction": "up", "scale": 0.75, "anchor_x": 0.5, "anchor_y": 0.5, "duration": 0.18, "repeat": 3, "pause_seconds": 0.25},
        fields=_direction_fields(default_repeat=3),
        presets=(
            StepPreset("Scroll Down Feed", "Scroll a feed upward to reveal more content.", {"direction": "up", "scale": 0.75, "anchor_x": 0.5, "anchor_y": 0.55, "duration": 0.18, "repeat": 3, "pause_seconds": 0.25}),
            StepPreset("Scroll Up", "Reverse scroll direction.", {"direction": "down", "scale": 0.75, "anchor_x": 0.5, "anchor_y": 0.45, "duration": 0.18, "repeat": 3, "pause_seconds": 0.25}),
        ),
    ),
    StepDefinition(
        key="scroll_to_selector",
        label="Scroll To Selector",
        description="Swipe until the target selector appears or max swipes is reached.",
        template={
            "resource_id": "com.example:id/target",
            "timeout": 0.5,
            "direction": "up",
            "max_swipes": 8,
            "scale": 0.75,
            "anchor_x": 0.5,
            "anchor_y": 0.55,
            "duration": 0.18,
            "pause_seconds": 0.25,
        },
        fields=_scroll_to_selector_fields(),
        presets=(
            StepPreset(
                "Find By Resource ID",
                "Scroll a feed until a resource id appears.",
                {
                    "resource_id": "com.example:id/target",
                    "timeout": 0.5,
                    "direction": "up",
                    "max_swipes": 8,
                    "scale": 0.75,
                    "anchor_x": 0.5,
                    "anchor_y": 0.55,
                    "duration": 0.18,
                    "pause_seconds": 0.25,
                },
            ),
            StepPreset(
                "Find By Text",
                "Scroll until matching text becomes visible.",
                {
                    "text": "Buy Now",
                    "timeout": 0.5,
                    "direction": "up",
                    "max_swipes": 6,
                    "scale": 0.7,
                    "anchor_x": 0.5,
                    "anchor_y": 0.55,
                    "duration": 0.18,
                    "pause_seconds": 0.2,
                },
            ),
        ),
    ),
    StepDefinition(
        key="switch_account",
        label="Switch Account",
        description="Run the configured switch workflow for a platform and target account on the current device.",
        template={
            "platform_key": "shopee",
            "account_name": "main-shop",
            "launch_package_first": True,
        },
        fields=(
            StepField("platform_key", "Platform Key", required=True, placeholder="shopee"),
            StepField("account_name", "Account Name", placeholder="main-shop"),
            StepField("account_id", "Account ID", field_type="int", default=0, min_value=0),
            StepField("launch_package_first", "Launch Package First", field_type="bool", default=True),
        ),
        presets=(
            StepPreset(
                "Switch By Name",
                "Resolve the account by platform key and display name.",
                {"platform_key": "shopee", "account_name": "main-shop", "launch_package_first": True},
            ),
        ),
    ),
    StepDefinition(
        key="press_key",
        label="Press Key",
        description="Press common Android keys such as home, back, or enter.",
        template={"key": "back"},
        fields=(
            StepField(
                "key",
                "Key",
                field_type="combo",
                default="back",
                required=True,
                options=(("back", "Back"), ("home", "Home"), ("enter", "Enter"), ("recent", "Recent"), ("menu", "Menu")),
            ),
        ),
        presets=(
            StepPreset("Back", "Press the Back key.", {"key": "back"}),
            StepPreset("Home", "Press the Home key.", {"key": "home"}),
        ),
    ),
    StepDefinition(
        key="input_keycode",
        label="Input Keycode",
        description="Send a raw Android keycode through adb shell input keyevent.",
        template={"keycode": 66, "long_press": False},
        fields=(
            StepField("keycode", "Keycode", field_type="int", default=66, required=True, min_value=0),
            StepField("long_press", "Long Press", field_type="bool", default=False),
        ),
        presets=(
            StepPreset("Enter", "Send KEYCODE_ENTER.", {"keycode": 66, "long_press": False}),
            StepPreset("Back", "Send KEYCODE_BACK.", {"keycode": 4, "long_press": False}),
        ),
    ),
    StepDefinition(
        key="shell",
        label="ADB Shell",
        description="Run a shell command on the Android device.",
        template={"command": "input keyevent 3"},
        fields=(StepField("command", "Shell Command", field_type="textarea", required=True, default="input keyevent 3"),),
        presets=(StepPreset("Go Home", "Return to the home screen via shell.", {"command": "input keyevent 3"}),),
    ),
    StepDefinition(
        key="screenshot",
        label="Screenshot",
        description="Save a screenshot artifact to disk.",
        template={"directory": "artifacts/screenshots", "filename": "screen.png"},
        fields=(
            StepField("directory", "Directory", default="artifacts/screenshots"),
            StepField("filename", "Filename", default="screen.png"),
        ),
    ),
    StepDefinition(
        key="dump_hierarchy",
        label="Dump Hierarchy",
        description="Save the current UI hierarchy as XML.",
        template={"directory": "artifacts/hierarchy", "filename": "view.xml"},
        fields=(
            StepField("directory", "Directory", default="artifacts/hierarchy"),
            StepField("filename", "Filename", default="view.xml"),
        ),
    ),
    StepDefinition(
        key="assert_exists",
        label="Assert Exists",
        description="Fail if the expected element does not exist.",
        template={"resource_id": "com.example:id/result", "timeout": 10},
        fields=_selector_fields(),
        presets=(StepPreset("Assert By Resource ID", "Assert an element by resource id.", {"resource_id": "com.example:id/result", "timeout": 10}),),
    ),
    StepDefinition(
        key="assert_text",
        label="Assert Text",
        description="Fail if the selected element text does not match.",
        template={"resource_id": "com.example:id/result", "expected_text": "Success", "match_mode": "contains", "timeout": 10},
        fields=(
            *_selector_fields(),
            StepField("expected_text", "Expected Text", required=True, placeholder="Success"),
            StepField(
                "match_mode",
                "Match Mode",
                field_type="combo",
                default="contains",
                options=(("exact", "Exact"), ("contains", "Contains"), ("starts_with", "Starts With"), ("ends_with", "Ends With")),
            ),
        ),
    ),
    StepDefinition(
        key="assert_state",
        label="Assert State",
        description="Fail if the selected element state does not match the expected true/false value.",
        template={"resource_id": "com.example:id/like_button", "state_name": "selected", "expected": True, "timeout": 5},
        fields=(
            *_selector_fields(),
            StepField(
                "state_name",
                "State Name",
                field_type="combo",
                default="selected",
                options=(
                    ("selected", "Selected"),
                    ("checked", "Checked"),
                    ("enabled", "Enabled"),
                    ("focused", "Focused"),
                    ("clickable", "Clickable"),
                    ("scrollable", "Scrollable"),
                    ("long_clickable", "Long Clickable"),
                ),
            ),
            StepField("expected", "Expected", field_type="bool", default=True),
        ),
    ),
    StepDefinition(
        key="branch_on_state",
        label="Branch On State",
        description="Check an element state and jump to different step positions when it is true or false.",
        template={
            "resource_id": "com.example:id/like_button",
            "state_name": "selected",
            "target_position_on_true": 10,
            "target_position_on_false": 20,
            "timeout": 5,
        },
        fields=(
            *_selector_fields(),
            StepField(
                "state_name",
                "State Name",
                field_type="combo",
                default="selected",
                options=(
                    ("selected", "Selected"),
                    ("checked", "Checked"),
                    ("enabled", "Enabled"),
                    ("focused", "Focused"),
                    ("clickable", "Clickable"),
                    ("scrollable", "Scrollable"),
                    ("long_clickable", "Long Clickable"),
                ),
            ),
            StepField("target_position_on_true", "Target Position On True", field_type="int", default=10, required=True, min_value=1),
            StepField("target_position_on_false", "Target Position On False", field_type="int", default=20, required=True, min_value=1),
        ),
    ),
    StepDefinition(
        key="set_variable",
        label="Set Variable",
        description="Store a literal value, JSON object, template, or expression result in workflow context.",
        template={"variable_name": "otp", "value_mode": "literal", "value": "123456"},
        fields=(
            StepField("variable_name", "Variable Name", required=True, placeholder="otp"),
            StepField(
                "value_mode",
                "Value Mode",
                field_type="combo",
                default="literal",
                options=(("literal", "Literal Text"), ("template", "Template String"), ("expression", "Expression"), ("json", "JSON")),
            ),
            StepField("value", "Value", field_type="textarea", default="123456", placeholder="${vars.get('server_otp')}"),
        ),
        presets=(
            StepPreset("Static Value", "Store a fixed text value.", {"variable_name": "otp", "value_mode": "literal", "value": "123456"}),
            StepPreset("Expression Result", "Compute a value from current vars.", {"variable_name": "next_index", "value_mode": "expression", "value": "int(vars.get('loop_index', 0)) + 1"}),
        ),
    ),
    StepDefinition(
        key="extract_text",
        label="Extract Text",
        description="Read text or an info field from a selector and store it in a workflow variable.",
        template={"resource_id": "com.example:id/otp", "variable_name": "otp", "source": "text", "timeout": 10},
        fields=(
            *_selector_fields(),
            StepField("variable_name", "Variable Name", required=True, placeholder="otp"),
            StepField(
                "source",
                "Source",
                field_type="combo",
                default="text",
                options=(("text", "Visible Text"), ("content_desc", "Content Description"), ("resource_id", "Resource ID"), ("class_name", "Class Name"), ("info_json", "Whole Info JSON")),
            ),
        ),
    ),
    StepDefinition(
        key="chance_gate",
        label="Chance Gate",
        description="Randomly decide whether to continue, skip the next steps, or jump to another position.",
        template={"probability_percent": 30, "skip_count_on_fail": 1},
        fields=(
            StepField("probability_percent", "Probability (%)", field_type="float", default=30.0, required=True, min_value=0, max_value=100, decimals=2),
            StepField("skip_count_on_fail", "Skip Count On Fail", field_type="int", default=1, min_value=0),
            StepField("target_position_on_pass", "Target Position On Pass", field_type="int", default=0, min_value=0),
            StepField("target_position_on_fail", "Target Position On Fail", field_type="int", default=0, min_value=0),
        ),
        presets=(
            StepPreset("30% Like", "30 percent chance to run the next step, otherwise skip it.", {"probability_percent": 30, "skip_count_on_fail": 1}),
            StepPreset("50/50 Split", "Half chance to continue and half chance to skip the next step.", {"probability_percent": 50, "skip_count_on_fail": 1}),
        ),
    ),
    StepDefinition(
        key="random_wait",
        label="Random Wait",
        description="Sleep for a random duration between min and max seconds.",
        template={"min_seconds": 3, "max_seconds": 8},
        fields=(
            StepField("min_seconds", "Min Seconds", field_type="float", default=3.0, required=True, min_value=0, decimals=2),
            StepField("max_seconds", "Max Seconds", field_type="float", default=8.0, required=True, min_value=0, decimals=2),
        ),
        presets=(
            StepPreset("Watch Short Clip", "Wait between 5 and 12 seconds.", {"min_seconds": 5, "max_seconds": 12}),
            StepPreset("Human Pause", "Wait between 1.5 and 3 seconds.", {"min_seconds": 1.5, "max_seconds": 3}),
        ),
    ),
    StepDefinition(
        key="loop_until_elapsed",
        label="Loop Until Elapsed",
        description="Jump back to a target position until the specified duration has passed.",
        template={"duration_minutes": 10, "target_position": 1},
        fields=(
            StepField("duration_minutes", "Duration (minutes)", field_type="float", default=10.0, required=True, min_value=0.01, decimals=2),
            StepField("target_position", "Target Position", field_type="int", default=1, required=True, min_value=1),
        ),
        presets=(
            StepPreset("Loop 10 Minutes", "Repeat the block for 10 minutes.", {"duration_minutes": 10, "target_position": 1}),
            StepPreset("Loop 30 Minutes", "Repeat the block for 30 minutes.", {"duration_minutes": 30, "target_position": 1}),
        ),
    ),
    StepDefinition(
        key="conditional_jump",
        label="Conditional Jump",
        description="Jump to another step position when an expression evaluates to true.",
        template={"expression": "int(vars.get('loop_index', 0)) < 3", "target_position": 2},
        fields=(
            StepField("expression", "Expression", field_type="textarea", required=True, default="int(vars.get('loop_index', 0)) < 3"),
            StepField("target_position", "Target Position", field_type="int", required=True, default=2, min_value=1),
        ),
        presets=(StepPreset("Loop Back", "Jump back while loop_index is still below the limit.", {"expression": "int(vars.get('loop_index', 0)) < 3", "target_position": 2}),),
    ),
]


def _plugin_field_from_metadata(field_data: dict[str, Any]) -> StepField:
    return StepField(
        key=str(field_data["key"]),
        label=str(field_data.get("label") or field_data["key"]),
        field_type=str(field_data.get("field_type") or "text"),
        default=field_data.get("default", ""),
        required=bool(field_data.get("required", False)),
        placeholder=str(field_data.get("placeholder", "")),
        help_text=str(field_data.get("help_text", "")),
        options=tuple((option[0], option[1]) for option in field_data.get("options", [])),
        min_value=field_data.get("min_value"),
        max_value=field_data.get("max_value"),
        decimals=int(field_data.get("decimals", 2)),
    )


def _plugin_preset_from_metadata(preset_data: dict[str, Any]) -> StepPreset:
    return StepPreset(
        label=str(preset_data.get("label") or "Preset"),
        description=str(preset_data.get("description") or ""),
        parameters=dict(preset_data.get("parameters") or {}),
    )


def _plugin_step_definitions() -> list[StepDefinition]:
    definitions: list[StepDefinition] = []
    for plugin in discover_plugin_metadata():
        definitions.append(
            StepDefinition(
                key=f"plugin:{plugin['key']}",
                label=str(plugin.get("label") or plugin["key"]),
                description=str(plugin.get("description") or "Custom plugin step"),
                template=dict(plugin.get("template") or {}),
                fields=tuple(
                    _plugin_field_from_metadata(field_data)
                    for field_data in plugin.get("fields", [])
                ),
                presets=tuple(
                    _plugin_preset_from_metadata(preset_data)
                    for preset_data in plugin.get("presets", [])
                ),
            )
        )
    return definitions


STEP_DEFINITIONS.extend(_plugin_step_definitions())
STEP_DEFINITION_MAP = {definition.key: definition for definition in STEP_DEFINITIONS}


def definition_for(step_type: str) -> StepDefinition:
    if step_type not in STEP_DEFINITION_MAP:
        raise ValueError(f"Unsupported step type: {step_type}")
    return STEP_DEFINITION_MAP[step_type]


def preset_map_for(step_type: str) -> dict[str, StepPreset]:
    definition = definition_for(step_type)
    return {preset.label: preset for preset in definition.presets}


def _selector_present(parameters: dict[str, Any]) -> bool:
    return any(str(parameters.get(key, "")).strip() for key in ("text", "resource_id", "xpath", "description", "class_name"))


def _safe_float(value: Any, label: str, errors: list[str]) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        errors.append(f"{label} must be a number")
        return None


def _safe_int(value: Any, label: str, errors: list[str]) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        errors.append(f"{label} must be an integer")
        return None


def _has_coordinate_pair(parameters: dict[str, Any], x_key: str = "x", y_key: str = "y") -> bool:
    return parameters.get(x_key) not in (None, "") and parameters.get(y_key) not in (None, "")


def _valid_variable_name(value: Any) -> bool:
    return bool(VARIABLE_NAME_PATTERN.match(str(value or "").strip()))


def migrate_step_parameters(
    step_type: str,
    parameters: dict[str, Any],
    schema_version: int | None = None,
) -> dict[str, Any]:
    migrated = dict(parameters or {})
    version = int(schema_version or 1)

    if version < 2:
        if "run_if" in migrated and "run_if_expression" not in migrated:
            migrated["run_if_expression"] = migrated.pop("run_if")
        if "store_as" in migrated and "result_variable" not in migrated:
            migrated["result_variable"] = migrated.pop("store_as")

    return migrated


def validate_step_parameters(step_type: str, parameters: dict[str, Any]) -> list[str]:
    parameters = migrate_step_parameters(step_type, parameters, STEP_SCHEMA_VERSION)
    definition = definition_for(step_type)
    errors: list[str] = []

    for field_definition in definition.fields:
        value = parameters.get(field_definition.key)
        if field_definition.required:
            if field_definition.field_type in {"text", "textarea", "combo"} and not str(value or "").strip():
                errors.append(f"{field_definition.label} is required")
            if field_definition.field_type in {"int", "float"} and value is None:
                errors.append(f"{field_definition.label} is required")

    def require_selector(step_label: str) -> None:
        if not _selector_present(parameters):
            errors.append(f"{step_label} requires at least one selector field")

    if step_type in {"launch_app", "stop_app"} and not str(parameters.get("package", "")).strip():
        errors.append("Package Name is required")

    if step_type == "tap" and not _has_coordinate_pair(parameters):
        errors.append("Tap requires both x and y")

    if step_type in {"click", "long_click", "double_click"}:
        if not _selector_present(parameters) and not _has_coordinate_pair(parameters):
            errors.append(f"{definition.label} requires selector fields or fallback x/y")

    if step_type == "long_click":
        duration = _safe_float(parameters.get("duration", 0.8), "Duration", errors)
        if duration is not None and duration < 0:
            errors.append("Duration must be greater than or equal to 0")

    if step_type == "double_click":
        interval_seconds = _safe_float(parameters.get("interval_seconds", 0.15), "Interval", errors)
        if interval_seconds is not None and interval_seconds < 0:
            errors.append("Interval must be greater than or equal to 0")

    if step_type == "set_text" and not str(parameters.get("text", "")).strip():
        errors.append("Input Text is required")

    if step_type == "wait":
        seconds = _safe_float(parameters.get("seconds", 0), "Seconds", errors)
        if seconds is not None and seconds < 0:
            errors.append("Seconds must be greater than or equal to 0")

    if step_type == "random_wait":
        min_seconds = _safe_float(parameters.get("min_seconds", 0), "Min Seconds", errors)
        max_seconds = _safe_float(parameters.get("max_seconds", 0), "Max Seconds", errors)
        if min_seconds is not None and min_seconds < 0:
            errors.append("Min Seconds must be greater than or equal to 0")
        if max_seconds is not None and max_seconds < 0:
            errors.append("Max Seconds must be greater than or equal to 0")
        if min_seconds is not None and max_seconds is not None and min_seconds > max_seconds:
            errors.append("Min Seconds must be less than or equal to Max Seconds")

    if step_type in {"wait_for_text", "assert_exists", "wait_for_element", "assert_text", "assert_state", "branch_on_state", "extract_text", "scroll_to_selector"}:
        require_selector(definition.label)

    if step_type == "wait_for_element":
        desired_state = str(parameters.get("desired_state", "exists") or "exists").strip()
        poll_interval = _safe_float(parameters.get("poll_interval_seconds", 0.5), "Poll Interval", errors)
        if desired_state not in {"exists", "gone"}:
            errors.append("Desired State must be either exists or gone")
        if poll_interval is not None and poll_interval <= 0:
            errors.append("Poll Interval must be greater than 0")

    if step_type in {"swipe", "scroll"}:
        direction = str(parameters.get("direction", "")).strip().lower()
        if direction and direction not in {"up", "down", "left", "right"}:
            errors.append("Direction must be one of up, down, left, right or empty for custom")
        if not direction:
            absolute_ready = all(key in parameters for key in ("x1", "y1", "x2", "y2"))
            ratio_ready = all(key in parameters for key in ("x1_ratio", "y1_ratio", "x2_ratio", "y2_ratio"))
            if not absolute_ready and not ratio_ready:
                errors.append(f"{definition.label} custom mode requires x1/y1/x2/y2 or x1_ratio/y1_ratio/x2_ratio/y2_ratio")

    if step_type == "scroll_to_selector":
        direction = str(parameters.get("direction", "")).strip().lower()
        if direction and direction not in {"up", "down", "left", "right"}:
            errors.append("Direction must be one of up, down, left, right or empty for custom")
        if not direction:
            absolute_ready = all(key in parameters for key in ("x1", "y1", "x2", "y2"))
            ratio_ready = all(key in parameters for key in ("x1_ratio", "y1_ratio", "x2_ratio", "y2_ratio"))
            if not absolute_ready and not ratio_ready:
                errors.append(f"{definition.label} custom mode requires x1/y1/x2/y2 or x1_ratio/y1_ratio/x2_ratio/y2_ratio")
        timeout = _safe_float(parameters.get("timeout", 0) or 0, "Timeout", errors)
        max_swipes = _safe_int(parameters.get("max_swipes", 0) or 0, "Max Swipes", errors)
        duration = _safe_float(parameters.get("duration", 0) or 0, "Duration", errors)
        pause_seconds = _safe_float(parameters.get("pause_seconds", 0) or 0, "Pause Seconds", errors)
        if timeout is not None and timeout < 0:
            errors.append("Timeout must be greater than or equal to 0")
        if max_swipes is not None and max_swipes < 1:
            errors.append("Max Swipes must be greater than or equal to 1")
        if duration is not None and duration < 0:
            errors.append("Duration must be greater than or equal to 0")
        if pause_seconds is not None and pause_seconds < 0:
            errors.append("Pause Seconds must be greater than or equal to 0")

    if step_type == "switch_account":
        if not str(parameters.get("platform_key", "")).strip():
            errors.append("Platform Key is required")
        account_id = _safe_int(parameters.get("account_id", 0) or 0, "Account ID", errors)
        account_name = str(parameters.get("account_name", "") or "").strip()
        if account_id is not None and account_id < 0:
            errors.append("Account ID must be greater than or equal to 0")
        if not account_name and not int(parameters.get("account_id", 0) or 0):
            errors.append("Switch Account requires account_name or account_id")

    if step_type == "press_key" and not str(parameters.get("key", "")).strip():
        errors.append("Key is required")

    if step_type == "input_keycode":
        keycode = _safe_int(parameters.get("keycode"), "Keycode", errors)
        if keycode is not None and keycode < 0:
            errors.append("Keycode must be greater than or equal to 0")

    if step_type == "shell" and not str(parameters.get("command", "")).strip():
        errors.append("Shell Command is required")

    if step_type == "assert_text":
        if not str(parameters.get("expected_text", "")).strip():
            errors.append("Expected Text is required")
        match_mode = str(parameters.get("match_mode", "contains") or "contains").strip()
        if match_mode not in {"exact", "contains", "starts_with", "ends_with"}:
            errors.append("Match Mode must be exact, contains, starts_with, or ends_with")

    if step_type == "assert_state":
        state_name = str(parameters.get("state_name", "selected") or "selected").strip()
        if state_name not in {"selected", "checked", "enabled", "focused", "clickable", "scrollable", "long_clickable"}:
            errors.append("State Name must be selected, checked, enabled, focused, clickable, scrollable, or long_clickable")
        if not isinstance(parameters.get("expected", True), bool):
            errors.append("Expected must be true or false")

    if step_type == "branch_on_state":
        state_name = str(parameters.get("state_name", "selected") or "selected").strip()
        target_position_on_true = _safe_int(parameters.get("target_position_on_true"), "Target Position On True", errors)
        target_position_on_false = _safe_int(parameters.get("target_position_on_false"), "Target Position On False", errors)
        if state_name not in {"selected", "checked", "enabled", "focused", "clickable", "scrollable", "long_clickable"}:
            errors.append("State Name must be selected, checked, enabled, focused, clickable, scrollable, or long_clickable")
        if target_position_on_true is not None and target_position_on_true < 1:
            errors.append("Target Position On True must be greater than or equal to 1")
        if target_position_on_false is not None and target_position_on_false < 1:
            errors.append("Target Position On False must be greater than or equal to 1")

    if step_type == "set_variable":
        variable_name = str(parameters.get("variable_name", "")).strip()
        if not _valid_variable_name(variable_name):
            errors.append("Variable Name must start with a letter or underscore and contain only letters, numbers, or underscores")
        value_mode = str(parameters.get("value_mode", "literal") or "literal").strip()
        if value_mode not in {"literal", "template", "expression", "json"}:
            errors.append("Value Mode must be literal, template, expression, or json")
        if value_mode == "json":
            try:
                json.loads(str(parameters.get("value", "") or "null"))
            except json.JSONDecodeError:
                errors.append("Value must be valid JSON when Value Mode is json")

    if step_type == "extract_text":
        variable_name = str(parameters.get("variable_name", "")).strip()
        if not _valid_variable_name(variable_name):
            errors.append("Variable Name must start with a letter or underscore and contain only letters, numbers, or underscores")
        source = str(parameters.get("source", "text") or "text").strip()
        if source not in {"text", "content_desc", "resource_id", "class_name", "info_json"}:
            errors.append("Source must be text, content_desc, resource_id, class_name, or info_json")

    if step_type == "conditional_jump":
        if not str(parameters.get("expression", "")).strip():
            errors.append("Expression is required")
        target_position = _safe_int(parameters.get("target_position"), "Target Position", errors)
        if target_position is not None and target_position < 1:
            errors.append("Target Position must be greater than or equal to 1")

    if step_type == "chance_gate":
        probability_percent = _safe_float(parameters.get("probability_percent", 0), "Probability (%)", errors)
        skip_count_on_fail = _safe_int(parameters.get("skip_count_on_fail", 0), "Skip Count On Fail", errors)
        target_position_on_pass = _safe_int(parameters.get("target_position_on_pass", 0) or 0, "Target Position On Pass", errors)
        target_position_on_fail = _safe_int(parameters.get("target_position_on_fail", 0) or 0, "Target Position On Fail", errors)
        if probability_percent is not None and not 0 <= probability_percent <= 100:
            errors.append("Probability (%) must be between 0 and 100")
        if skip_count_on_fail is not None and skip_count_on_fail < 0:
            errors.append("Skip Count On Fail must be greater than or equal to 0")
        if target_position_on_pass is not None and target_position_on_pass < 0:
            errors.append("Target Position On Pass must be greater than or equal to 0")
        if target_position_on_fail is not None and target_position_on_fail < 0:
            errors.append("Target Position On Fail must be greater than or equal to 0")

    if step_type == "loop_until_elapsed":
        duration_minutes = _safe_float(parameters.get("duration_minutes", 0), "Duration (minutes)", errors)
        target_position = _safe_int(parameters.get("target_position"), "Target Position", errors)
        if duration_minutes is not None and duration_minutes <= 0:
            errors.append("Duration (minutes) must be greater than 0")
        if target_position is not None and target_position < 1:
            errors.append("Target Position must be greater than or equal to 1")

    if step_type.startswith("plugin:"):
        for field_definition in definition.fields:
            if field_definition.required and field_definition.key not in parameters:
                errors.append(f"{field_definition.label} is required")

    timeout_seconds = _safe_float(parameters.get("step_timeout_seconds", 0) or 0, "Step Timeout", errors)
    retry_count = _safe_int(parameters.get("retry_count", 0) or 0, "Retry Count", errors)
    retry_delay_seconds = _safe_float(parameters.get("retry_delay_seconds", 0) or 0, "Retry Delay", errors)
    on_failure = str(parameters.get("on_failure", "stop") or "stop").strip()

    if timeout_seconds is not None and timeout_seconds < 0:
        errors.append("Step Timeout must be greater than or equal to 0")
    if retry_count is not None and retry_count < 0:
        errors.append("Retry Count must be greater than or equal to 0")
    if retry_delay_seconds is not None and retry_delay_seconds < 0:
        errors.append("Retry Delay must be greater than or equal to 0")
    if on_failure not in {"stop", "skip", "take_screenshot"}:
        errors.append("On Failure must be one of stop, skip, take_screenshot")

    repeat_times = _safe_int(parameters.get("repeat_times", 1) or 1, "Repeat Times", errors)
    repeat_delay_seconds = _safe_float(parameters.get("repeat_delay_seconds", 0) or 0, "Repeat Delay", errors)
    result_variable = str(parameters.get("result_variable", "") or "").strip()

    if repeat_times is not None and repeat_times < 1:
        errors.append("Repeat Times must be greater than or equal to 1")
    if repeat_delay_seconds is not None and repeat_delay_seconds < 0:
        errors.append("Repeat Delay must be greater than or equal to 0")
    if result_variable and not _valid_variable_name(result_variable):
        errors.append("Store Result As must start with a letter or underscore and contain only letters, numbers, or underscores")

    return errors


def validate_workflow_structure(steps: list[dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    positions = {int(step["position"]) for step in steps}

    for step in steps:
        if not step.get("is_enabled", True):
            continue
        try:
            parameters = step["parameters"] if isinstance(step["parameters"], dict) else json.loads(step["parameters"] or "{}")
            parameters = migrate_step_parameters(
                step["step_type"],
                parameters,
                int(step.get("schema_version", 1) or 1),
            )
        except json.JSONDecodeError as exc:
            errors.append(f"Step {step['position']} '{step['name']}': invalid JSON ({exc})")
            continue

        if step["step_type"] == "conditional_jump":
            target_position = int(parameters.get("target_position", 0) or 0)
            if target_position not in positions:
                errors.append(
                    f"Step {step['position']} '{step['name']}': target position {target_position} does not exist"
                )

        if step["step_type"] == "loop_until_elapsed":
            target_position = int(parameters.get("target_position", 0) or 0)
            if target_position not in positions:
                errors.append(
                    f"Step {step['position']} '{step['name']}': target position {target_position} does not exist"
                )

        if step["step_type"] == "chance_gate":
            for key, label in (
                ("target_position_on_pass", "target position on pass"),
                ("target_position_on_fail", "target position on fail"),
            ):
                target_position = int(parameters.get(key, 0) or 0)
                if target_position and target_position not in positions:
                    errors.append(
                        f"Step {step['position']} '{step['name']}': {label} {target_position} does not exist"
                    )

        if step["step_type"] == "branch_on_state":
            for key, label in (
                ("target_position_on_true", "target position on true"),
                ("target_position_on_false", "target position on false"),
            ):
                target_position = int(parameters.get(key, 0) or 0)
                if target_position not in positions:
                    errors.append(
                        f"Step {step['position']} '{step['name']}': {label} {target_position} does not exist"
                    )

    return errors


def validate_watcher_config(
    name: str,
    scope_type: str,
    scope_id: int | None,
    condition_type: str,
    condition: dict[str, Any],
    action_type: str,
    action: dict[str, Any],
    policy: dict[str, Any],
) -> list[str]:
    errors: list[str] = []

    if not str(name or "").strip():
        errors.append("Watcher name is required")

    if scope_type not in {option[0] for option in WATCHER_SCOPE_OPTIONS}:
        errors.append("Scope Type must be global, workflow, or device")
    elif scope_type == "global":
        if scope_id not in (None, 0):
            errors.append("Global watcher must not have a scope target")
    elif not scope_id or int(scope_id) < 1:
        errors.append("Workflow/Device watcher must have a valid target id")

    if condition_type not in WATCHER_CONDITION_TEMPLATES:
        errors.append(f"Unsupported watcher condition: {condition_type}")
    if action_type not in WATCHER_ACTION_TEMPLATES:
        errors.append(f"Unsupported watcher action: {action_type}")

    if condition_type in {"selector_exists", "selector_gone", "text_exists", "text_contains"}:
        if not _selector_present(condition):
            errors.append("Selector Exists condition requires text, resource_id, xpath, description, or class_name")
        timeout = _safe_float(condition.get("timeout", 0) or 0, "Condition Timeout", errors)
        if timeout is not None and timeout < 0:
            errors.append("Condition Timeout must be greater than or equal to 0")

    if condition_type == "text_contains" and not str(condition.get("expected_text", "")).strip():
        errors.append("Text Contains condition requires expected_text")

    if condition_type == "app_in_foreground" and not str(condition.get("package", "")).strip():
        errors.append("App In Foreground condition requires a package")

    if condition_type == "package_changed" and condition:
        if "package" in condition and not str(condition.get("package", "")).strip():
            errors.append("Package Changed condition package must not be empty")

    if condition_type == "elapsed_time":
        seconds = _safe_float(condition.get("seconds", 0), "Elapsed Time Seconds", errors)
        if seconds is not None and seconds < 0:
            errors.append("Elapsed Time Seconds must be greater than or equal to 0")

    if condition_type == "variable_changed":
        variable_name = str(condition.get("variable_name", "")).strip()
        if not _valid_variable_name(variable_name):
            errors.append("Variable Changed condition requires a valid variable_name")

    if condition_type == "expression" and not str(condition.get("expression", "")).strip():
        errors.append("Expression condition requires an expression")

    if action_type == "run_step":
        step_type = str(action.get("step_type", "")).strip()
        parameters = action.get("parameters", {})
        if not step_type:
            errors.append("Run Step action requires step_type")
        elif not isinstance(parameters, dict):
            errors.append("Run Step action parameters must be an object")
        elif step_type in STEP_DEFINITION_MAP:
            errors.extend(validate_step_parameters(step_type, parameters))
        else:
            errors.append(f"Unsupported action step type: {step_type}")

    if action_type == "action_chain":
        actions = action.get("actions", [])
        if isinstance(actions, str):
            try:
                actions = json.loads(actions)
            except json.JSONDecodeError:
                errors.append("Action Chain actions must be valid JSON")
                actions = []
        if not isinstance(actions, list) or not actions:
            errors.append("Action Chain requires a non-empty actions list")
        else:
            for index, item in enumerate(actions, start=1):
                if not isinstance(item, dict):
                    errors.append(f"Action Chain entry #{index} must be an object")
                    continue
                nested_type = str(item.get("action_type", "")).strip()
                nested_action = item.get("action", {})
                if nested_type not in WATCHER_ACTION_TEMPLATES or nested_type == "action_chain":
                    errors.append(f"Action Chain entry #{index} has unsupported action_type: {nested_type}")
                    continue
                if not isinstance(nested_action, dict):
                    errors.append(f"Action Chain entry #{index} action must be an object")
                    continue
                nested_errors = validate_watcher_config(
                    name=name,
                    scope_type=scope_type,
                    scope_id=scope_id,
                    condition_type="expression",
                    condition={"expression": "True"},
                    action_type=nested_type,
                    action=nested_action,
                    policy={"cooldown_seconds": 0, "debounce_count": 1, "max_triggers_per_run": 0, "stop_after_match": False, "match_mode": "first_match", "active_stages": ["before_step"]},
                )
                errors.extend(error for error in nested_errors if not error.startswith("Expression condition"))

    if action_type == "set_variable":
        variable_name = str(action.get("variable_name", "")).strip()
        if not _valid_variable_name(variable_name):
            errors.append("Set Variable action requires a valid variable_name")

    if action_type == "take_screenshot":
        prefix = str(action.get("filename_prefix", "") or "").strip()
        if not prefix:
            errors.append("Take Screenshot action requires filename_prefix")

    if action_type == "dump_hierarchy":
        prefix = str(action.get("filename_prefix", "") or "").strip()
        if not prefix:
            errors.append("Dump Hierarchy action requires filename_prefix")

    cooldown_seconds = _safe_float(policy.get("cooldown_seconds", 0) or 0, "Cooldown Seconds", errors)
    debounce_count = _safe_int(policy.get("debounce_count", 1) or 1, "Debounce Count", errors)
    max_triggers_per_run = _safe_int(policy.get("max_triggers_per_run", 0) or 0, "Max Triggers Per Run", errors)
    match_mode = str(policy.get("match_mode", "first_match") or "first_match").strip()
    active_stages = policy.get("active_stages", [])

    if cooldown_seconds is not None and cooldown_seconds < 0:
        errors.append("Cooldown Seconds must be greater than or equal to 0")
    if debounce_count is not None and debounce_count < 1:
        errors.append("Debounce Count must be greater than or equal to 1")
    if max_triggers_per_run is not None and max_triggers_per_run < 0:
        errors.append("Max Triggers Per Run must be greater than or equal to 0")
    if match_mode not in {"first_match", "continue"}:
        errors.append("Match Mode must be first_match or continue")
    if not isinstance(active_stages, list) or not active_stages:
        errors.append("Active Stages must be a non-empty list")
    else:
        valid_stages = {"before_step", "after_step", "during_wait"}
        for stage in active_stages:
            if str(stage) not in valid_stages:
                errors.append(f"Unsupported active stage: {stage}")

    return errors
