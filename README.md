# Android Automation Studio

เดสก์ท็อปแอปสำหรับจัดการ Android automation ด้วย Python, `PySide6`, `uiautomator2` และ `sqlite`

## Features

- หน้า `Devices` สำหรับจัดการอุปกรณ์ Android และทดสอบการเชื่อมต่อ
- หน้า `Workflow` สำหรับสร้าง workflow, เพิ่ม steps หลายประเภท และสั่งรันกับอุปกรณ์ที่เลือก
- หน้า `Log` สำหรับตรวจสอบ execution log และผลลัพธ์ย้อนหลัง
- จัดเก็บข้อมูลด้วย `sqlite` ในไฟล์ `automation_studio.db`
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
