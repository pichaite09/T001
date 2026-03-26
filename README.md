# Android Automation Studio

เดสก์ท็อปแอปสำหรับจัดการ Android automation ด้วย Python, `PySide6`, `uiautomator2` และ `sqlite`

## Features

- หน้า `Devices` สำหรับจัดการอุปกรณ์ Android และทดสอบการเชื่อมต่อ
- หน้า `Workflow` สำหรับสร้าง workflow, เพิ่ม steps หลายประเภท และสั่งรันกับอุปกรณ์ที่เลือก
- หน้า `Log` สำหรับตรวจสอบ execution log และผลลัพธ์ย้อนหลัง
- จัดเก็บข้อมูลด้วย `sqlite` ในไฟล์ `automation_studio.db`
- Phase 1 ของระบบ `step` พร้อมแล้ว:
  - Step Registry กลางสำหรับ template, preset และ validation
  - Step Editor แบบ form-based พร้อม JSON preview
  - Duplicate step, Enable/Disable step, Move Up/Move Down และ drag reorder
  - preset สำหรับ step หลักเช่น `click`, `set_text`, `wait`, `swipe`, `press_key`
- Phase 2 ของระบบ execution พร้อมแล้ว:
  - ตั้ง `step timeout`, `retry`, `retry delay` และ `continue on error` ได้ต่อ step
  - รองรับ `on_failure`: `stop`, `skip`, `take_screenshot`
  - pre-run validation ก่อนเริ่ม workflow
  - log แยกระดับ `workflow` และ `step` พร้อม `duration_ms`, `attempt`, `run_id`
  - บันทึก artifact ของ `screenshot`, `dump_hierarchy`, `shell output` และ failure capture
- รองรับ workflow steps หลักสำหรับ automation:
  - `launch_app`
  - `stop_app`
  - `tap`
  - `click`
  - `set_text`
  - `wait`
  - `wait_for_text`
  - `swipe`
  - `press_key`
  - `shell`
  - `screenshot`
  - `dump_hierarchy`
  - `assert_exists`

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

## Notes

- ต้องเปิด `adb` และเชื่อมอุปกรณ์ Android ไว้ก่อนใช้งาน
- บาง step ต้องกำหนด selector ใน `parameters` เช่น `text`, `resource_id`, `xpath` หรือ `description`
- แอปจะสร้างฐานข้อมูล `automation_studio.db` ให้อัตโนมัติเมื่อเปิดครั้งแรก
- `swipe` รองรับทั้งพิกัดตรง (`x1/y1/x2/y2`), แบบสัดส่วนจอ (`x1_ratio/...`) และแบบกำหนดทิศทาง (`direction`)
- การบันทึก step จะ validate พารามิเตอร์ตามชนิด step ก่อนเสมอ
- artifact จากการรัน workflow จะถูกเก็บไว้ใน `artifacts/runs/...`

## Execution Policy Example

```json
{
  "text": "Login",
  "timeout": 10,
  "step_timeout_seconds": 15,
  "retry_count": 2,
  "retry_delay_seconds": 1.5,
  "continue_on_error": false,
  "on_failure": "take_screenshot",
  "capture_hierarchy_on_failure": true
}
```

## Example Swipe Parameters

`swipe` แบบทิศทาง

```json
{
  "direction": "up",
  "scale": 0.6,
  "anchor_x": 0.5,
  "anchor_y": 0.5,
  "duration": 0.2,
  "repeat": 2,
  "pause_seconds": 0.4
}
```

`swipe` แบบสัดส่วนหน้าจอ

```json
{
  "x1_ratio": 0.5,
  "y1_ratio": 0.8,
  "x2_ratio": 0.5,
  "y2_ratio": 0.2,
  "duration": 0.2
}
```

## Phase 3 Highlights

- Added new steps: `long_click`, `double_click`, `scroll`, `wait_for_element`, `assert_text`, `input_keycode`, `set_variable`, `extract_text`, `conditional_jump`
- Added flow-control fields on every step: `run_if_expression`, `repeat_times`, `repeat_delay_seconds`, `result_variable`
- Added workflow context variables with `${...}` template interpolation
- Added conditional branching and loop support through `conditional_jump`
- Added workflow JSON export/import directly from the `Workflow` page

## Phase 3 Example

```json
{
  "variable_name": "otp",
  "value_mode": "template",
  "value": "${vars.get('latest_otp')}"
}
```

```json
{
  "expression": "int(vars.get('loop_index', 0)) < 3",
  "target_position": 2
}
```

## Phase 4 Highlights

- Added real database migrations with schema version tracking in `schema_migrations`
- Added `definition_version` for workflows and `schema_version` for steps
- Added step telemetry aggregation for success, failure, continued failure, skipped counts, and failure rate
- Added custom step plugin discovery from `automation_studio/custom_steps`
- Added sample plugin step `plugin:echo_context`
- Added `unittest` smoke coverage for migrations, schema migration helpers, plugins, telemetry, and import/export cleanup

## Custom Step Plugins

- Drop Python plugin files into `automation_studio/custom_steps`
- Expose `PLUGIN_KEY`, `PLUGIN_LABEL`, `PLUGIN_DESCRIPTION`, `PLUGIN_TEMPLATE`, `PLUGIN_FIELDS`, and a `run(device, parameters, context)` function
- Plugin steps are registered automatically as `plugin:<PLUGIN_KEY>`

## Test

```bash
python -m unittest discover -s tests -v
```

## New Timing And Chance Steps

- `chance_gate`: random chance to continue, skip the next steps, or jump elsewhere
- `loop_until_elapsed`: repeat a block until the configured number of minutes has passed
- `random_wait`: wait for a random duration between `min_seconds` and `max_seconds`

```json
{
  "probability_percent": 30,
  "skip_count_on_fail": 1
}
```

```json
{
  "duration_minutes": 10,
  "target_position": 1
}
```

```json
{
  "min_seconds": 5,
  "max_seconds": 12
}
```
