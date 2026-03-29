# User Guide

## Overview

Android Automation Studio is a desktop application for building and running Android automation workflows with:

- `PySide6` for the UI
- `uiautomator2` for device control
- `sqlite` for local storage

The application stores its data in `automation_studio.db` in the project root.

## Requirements

- Windows with Python 3.11+ recommended
- `adb` installed and available in `PATH`
- Android devices connected by USB or ADB over Wi-Fi
- Python packages from `requirements.txt`

Install dependencies:

```powershell
pip install -r requirements.txt
```

## Start The Main App

From the project root:

```powershell
python main.py
```

The main window opens with a default size of `1600x900`.

## Main Pages

### Devices

Use `Devices` to:

- Add or edit Android devices
- Test connection
- Refresh device runtime info
- Capture screenshot
- Dump UI hierarchy
- Open the `Screen Wall`

Typical flow:

1. Click `New`
2. Enter device name and serial or ADB address
3. Save
4. Click `Test Connection`
5. Confirm `Status`, `Manufacturer`, `Model`, `Android`, and `Current App`

### Accounts

Use `Accounts` to manage:

- Platforms per device
- Accounts under each platform
- Current account
- Account aliases
- Switch workflow per platform

Aliases are useful when the app displays different account names in different screens. The system supports:

- `display_name`
- `username`
- `login_id`
- `aliases`

### Workflow

Use `Workflow` to build step-based automations.

Typical workflow actions include:

- `launch_app`
- `click`
- `tap`
- `set_text`
- `wait`
- `wait_for_text`
- `wait_for_element`
- `swipe`
- `scroll_to_selector`
- `press_key`
- `shell`
- `screenshot`
- `dump_hierarchy`
- `set_variable`
- `extract_text`
- `conditional_jump`
- `branch_on_exists`
- `branch_on_state`
- `switch_account`
- `run_for_each_account`
- `prepare_upload_context`
- `download_video_asset`
- `push_file_to_device`
- `delete_local_file`

Useful concepts:

- `Execution Policy`
  Controls timeout, retry, continue-on-error, and failure capture.
- `Flow Control`
  Controls conditional execution, repeat, delays, and result variables.
- `Template Values`
  Use `${...}` in many fields to resolve runtime values.

Example:

```json
{
  "text": "${upload.get('title')}"
}
```

### Schedules

Use `Schedules` to run workflows automatically.

The app supports schedule types such as:

- `once`
- `interval`
- `daily`
- `weekly`

You can also configure:

- active windows
- overlap policy
- retry on failure
- pause and resume
- groups and priority

### Uploads

Use `Uploads` to create upload jobs that carry content data into a workflow.

Supported upload fields:

- `device`
- `platform`
- `account`
- `workflow`
- `code_product`
- `link_product`
- `title`
- `description`
- `tags`
- `video_url`
- `cover_url`
- `local_video_path`
- `metadata_json`

Typical upload flow:

1. Create an upload job
2. Choose device, platform, account, and workflow
3. Fill content fields
4. Click `Save Upload`
5. Click `Run Now`

Inside the workflow you can use:

- `${upload.get('title')}`
- `${upload.get('description')}`
- `${upload.get('code_product')}`
- `${upload.get('video_url')}`
- `${upload.get('local_video_path')}`
- `${upload.get('device_video_path')}`

Useful upload steps:

1. `prepare_upload_context`
2. `download_video_asset`
3. `push_file_to_device`
4. app-specific upload steps
5. `delete_local_file`

### Watchers

Use `Watchers` to handle unexpected popups or runtime events.

Watchers can run:

- `before_step`
- `after_step`
- `during_wait`

Use them carefully. If `during_wait` is enabled on a watcher that triggers often, workflow timing can become longer than expected.

### Log

Use `Log` to inspect:

- workflow runs
- step results
- watcher activity
- schedules
- device maintenance events

It also includes:

- quick filters
- run sessions
- event detail panel
- telemetry
- analytics

### Runtime

Use `Runtime` to monitor active work.

It shows:

- running workflows
- queued/running upload jobs
- queued/running schedules

You can also:

- stop workflows
- stop uploads
- cancel queued uploads
- stop schedules
- cancel queued schedules

The runtime page now uses shared runtime data, so it can also see work started by:

- the main UI
- `Screen Wall`
- the external Upload API

## Screen Wall

`Screen Wall` opens in a separate window and separate process.

Open it from the left sidebar button: `Screen Wall`.

Features:

- live screenshot polling for all devices
- workflow run across selected devices
- stop workflow from the wall
- brightness controls
- `Home`, `Back`, `Recent Apps`
- double-click a screen to open `scrcpy`
- polling presets
- `scrcpy` presets and controls

The wall has separate settings for:

- `Screen Wall Polling`
- `scrcpy Realtime`

Those settings are remembered when reopened.

## Running Workflows

### Run A Single Workflow Manually

1. Open `Workflow`
2. Select a workflow
3. Select a device
4. Click `Run`

### Run A Single Step

1. Open `Workflow`
2. Select a step
3. Click `Run Step`

### Run By Schedule

1. Open `Schedules`
2. Create or edit a schedule
3. Enable the schedule

### Run Across Multiple Devices

1. Open `Screen Wall`
2. Select devices
3. Choose a workflow
4. Select mode:
   - `Sequential`
   - `Parallel x2`
   - `Parallel x4`
   - `Parallel All`
5. Click `Run Selected` or `Run All`

## Working With Uploads

### Create A New Upload Job

1. Open `Uploads`
2. Click `New Upload`
3. Select:
   - device
   - platform
   - account
   - workflow
4. Fill content fields
5. Click `Save Upload`

### Auto Run Draft Jobs

The page includes `Auto Run Draft Jobs`.

If enabled:

- the page checks for jobs with status `draft`
- jobs are picked automatically
- they run one at a time

You can turn it on or off and choose the check interval.

### Tags Format

Hashtags are preserved as typed.

Example input:

```text
#tag1 #tag2 #tag3
```

To convert the list to a plain space-separated string inside a workflow, use:

```text
${' '.join(upload.get('tags', []))}
```

## API Usage

The project includes an Upload API server.

Run it locally:

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 127.0.0.1 --port 8000
```

Allow LAN access:

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 0.0.0.0 --port 8000
```

With API key:

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 127.0.0.1 --port 8000 --api-key your-secret-key
```

Health check:

```powershell
curl http://127.0.0.1:8000/api/health
```

Important endpoints:

- `GET /api/health`
- `GET /api/devices`
- `GET /api/workflows`
- `GET /api/uploads`
- `POST /api/uploads`
- `PUT /api/uploads/{id}`
- `POST /api/uploads/{id}/run`
- `POST /api/uploads/run-batch`
- `POST /api/uploads/export`
- `POST /api/uploads/import`
- `GET /api/upload-templates`
- `POST /api/upload-templates`

## Common Problems

### Device Connects But Automation Fails

Check:

- `adb devices`
- device authorization dialog
- app package name
- selector accuracy

### Workflow Timing Feels Longer Than Expected

Check:

- `wait` and `random_wait` steps
- `wait_for_text` timeout
- active watchers with `during_wait`
- retries and retry delay

### Upload Video Does Not Reach The Phone

Recommended flow:

1. `download_video_asset`
2. `push_file_to_device`
3. choose the file in the app

Example destination:

```json
{
  "local_path": "${upload.get('local_video_path')}",
  "device_path": "/sdcard/Movies/upload_video.mp4",
  "create_parent": true
}
```

### Stop Does Not Work Immediately

Most stops are graceful. The current step may finish first, then execution stops.

This is especially relevant for:

- long waits
- nested workflows
- screen wall batch runs
- upload jobs started from API

## Recommended Daily Workflow

1. Add devices
2. Configure platforms and accounts
3. Build and test a workflow on one device
4. Add watchers for popups
5. Create schedules or upload jobs
6. Monitor from `Runtime`
7. Use `Screen Wall` for multi-device operation
8. Use `Log` for diagnostics
