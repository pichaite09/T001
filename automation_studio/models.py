from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StepDefinition:
    key: str
    label: str
    description: str
    template: dict[str, Any]

    def template_json(self) -> str:
        return json.dumps(self.template, indent=2, ensure_ascii=False)


STEP_DEFINITIONS = [
    StepDefinition("launch_app", "Launch App", "เปิดแอปตาม package name", {"package": "com.example.app"}),
    StepDefinition("stop_app", "Stop App", "ปิดแอปตาม package name", {"package": "com.example.app"}),
    StepDefinition("tap", "Tap Coordinates", "แตะพิกัดบนหน้าจอด้วย x/y", {"x": 540, "y": 1200}),
    StepDefinition(
        "click",
        "Click Selector",
        "คลิก element ด้วย text, resource_id, xpath หรือ description",
        {"text": "Login", "timeout": 10},
    ),
    StepDefinition(
        "set_text",
        "Set Text",
        "กรอกข้อความลงใน element ที่ระบุ หรือช่องที่ focus อยู่",
        {"resource_id": "com.example:id/input", "text": "demo", "clear_first": True},
    ),
    StepDefinition("wait", "Wait", "หน่วงเวลาเป็นวินาที", {"seconds": 2}),
    StepDefinition(
        "wait_for_text",
        "Wait For Text",
        "รอจนกว่า element หรือข้อความจะปรากฏ",
        {"text": "Success", "timeout": 15},
    ),
    StepDefinition(
        "swipe",
        "Swipe",
        "ลากหน้าจอแบบกำหนดพิกัดตรง, แบบอิงสัดส่วนจอ, หรือแบบกำหนดทิศทาง",
        {
            "direction": "up",
            "scale": 0.6,
            "anchor_x": 0.5,
            "anchor_y": 0.5,
            "duration": 0.2,
            "repeat": 1,
        },
    ),
    StepDefinition("press_key", "Press Key", "กดปุ่ม Android เช่น home, back, enter", {"key": "back"}),
    StepDefinition("shell", "ADB Shell", "รัน shell command บนอุปกรณ์", {"command": "input keyevent 3"}),
    StepDefinition(
        "screenshot",
        "Screenshot",
        "บันทึกภาพหน้าจอเป็นไฟล์",
        {"directory": "artifacts/screenshots", "filename": "screen.png"},
    ),
    StepDefinition(
        "dump_hierarchy",
        "Dump Hierarchy",
        "บันทึก XML hierarchy ของหน้าจอปัจจุบัน",
        {"directory": "artifacts/hierarchy", "filename": "view.xml"},
    ),
    StepDefinition(
        "assert_exists",
        "Assert Exists",
        "ตรวจว่า element มีอยู่จริง ถ้าไม่พบให้ workflow fail",
        {"resource_id": "com.example:id/result", "timeout": 10},
    ),
]

STEP_DEFINITION_MAP = {definition.key: definition for definition in STEP_DEFINITIONS}
