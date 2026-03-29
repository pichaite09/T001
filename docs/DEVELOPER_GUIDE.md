# Developer Guide

## Purpose

This guide explains how the codebase is organized, how the main runtime flows work, and how to extend the system safely.

## Stack

- Python
- `PySide6`
- `uiautomator2`
- `sqlite`

Core entry points:

- Main UI: [main.py](/d:/PRO/T001/main.py)
- Main window: [main_window.py](/d:/PRO/T001/automation_studio/ui/main_window.py)
- Upload API: [api_server.py](/d:/PRO/T001/automation_studio/api_server.py)
- Screen Wall process: [viewer_process.py](/d:/PRO/T001/automation_studio/viewer_process.py)

## High-Level Architecture

The project follows a layered pattern:

1. `DatabaseManager`
   - schema creation and migrations
2. `Repositories`
   - SQL read/write operations
3. `Services`
   - business logic and orchestration
4. `UI Pages`
   - page-level interaction and background threads
5. `WorkflowExecutor`
   - low-level step execution against devices

Important files:

- Database: [database.py](/d:/PRO/T001/automation_studio/database.py)
- Repositories: [repositories.py](/d:/PRO/T001/automation_studio/repositories.py)
- Services: [services.py](/d:/PRO/T001/automation_studio/services.py)
- Engine: [engine.py](/d:/PRO/T001/automation_studio/automation/engine.py)
- Models and step schema: [models.py](/d:/PRO/T001/automation_studio/models.py)

## Main Modules

### UI Pages

- [devices_page.py](/d:/PRO/T001/automation_studio/ui/pages/devices_page.py)
- [accounts_page.py](/d:/PRO/T001/automation_studio/ui/pages/accounts_page.py)
- [uploads_page.py](/d:/PRO/T001/automation_studio/ui/pages/uploads_page.py)
- [workflow_page.py](/d:/PRO/T001/automation_studio/ui/pages/workflow_page.py)
- [schedules_page.py](/d:/PRO/T001/automation_studio/ui/pages/schedules_page.py)
- [runtime_page.py](/d:/PRO/T001/automation_studio/ui/pages/runtime_page.py)
- [watchers_page.py](/d:/PRO/T001/automation_studio/ui/pages/watchers_page.py)
- [log_page.py](/d:/PRO/T001/automation_studio/ui/pages/log_page.py)

### Automation Runtime

- [engine.py](/d:/PRO/T001/automation_studio/automation/engine.py)
- [services.py](/d:/PRO/T001/automation_studio/services.py)

### Multi-Device Monitoring

- [screen_viewer_window.py](/d:/PRO/T001/automation_studio/ui/screen_viewer_window.py)
- [viewer_process.py](/d:/PRO/T001/automation_studio/viewer_process.py)

### Upload API

- [api_server.py](/d:/PRO/T001/automation_studio/api_server.py)

## Database

Current schema version: `18`

Migration history lives in [database.py](/d:/PRO/T001/automation_studio/database.py).

Important tables:

- `devices`
- `workflows`
- `steps`
- `logs`
- `step_telemetry`
- `watchers`
- `watcher_telemetry`
- `device_platforms`
- `accounts`
- `account_aliases`
- `workflow_schedules`
- `schedule_groups`
- `schedule_runs`
- `upload_jobs`
- `upload_templates`
- `runtime_locks`
- `runtime_tasks`

### runtime_locks

Used to prevent conflicting execution on:

- upload jobs
- devices

This is the execution lock layer.

### runtime_tasks

Used for shared runtime visibility and control across:

- main UI
- API server
- Screen Wall process

This is the monitoring and control layer.

## Service Layer

### DeviceService

Responsibilities:

- persist devices
- test connection
- collect runtime device info
- screenshot and hierarchy maintenance actions

### AccountService

Responsibilities:

- device platforms
- accounts
- aliases
- current account
- account runtime context resolution

### WorkflowService

Responsibilities:

- validate workflows
- execute workflows or selected steps
- manage active executors
- expose runtime task state
- handle stop requests
- support nested workflow execution for:
  - `switch_account`
  - `run_for_each_account`
  - upload-related context preparation

### UploadService

Responsibilities:

- CRUD upload jobs
- CRUD upload templates
- execute upload jobs
- inject `upload` context into workflows
- persist local/device video paths
- manage upload queue status

### SchedulerService

Responsibilities:

- compute next runs
- execute scheduled workflows
- record history
- support policy and group behavior

## Workflow Execution Model

`WorkflowService.execute_workflow()` resolves:

- workflow
- device
- account/platform context
- optional upload context

Then it creates `WorkflowExecutor`.

`WorkflowExecutor`:

- resolves templates
- validates selectors
- executes steps
- applies retries and failure policy
- polls watchers
- supports flow control
- can stop gracefully

### Shared Stop Flow

Stop requests can come from:

- Runtime page
- Screen Wall
- API

The flow is:

1. caller requests stop
2. request is written to `runtime_tasks`
3. running executor polls external stop state
4. executor sets `_stop_requested`
5. execution exits gracefully

Nested workflows now inherit the parent stop signal, so stopping a main workflow also stops:

- switch workflow
- target workflow inside `run_for_each_account`
- other nested workflow runs triggered inside service helpers

## Upload Runtime Model

Upload jobs use both:

- `upload_jobs.status`
- `runtime_tasks`

Typical states:

- `draft`
- `queued`
- `running`
- `success`
- `failed`
- `cancelled`

Runtime page reads shared runtime information instead of relying only on page-local threads.

## Screen Wall Runtime Model

`Screen Wall` runs in a separate process.

Because of that:

- in-memory executor state in the main UI is not enough
- runtime information must be shared through the database

The screen wall builds its own `WorkflowService` in [viewer_process.py](/d:/PRO/T001/automation_studio/viewer_process.py) and now uses the shared runtime repository so the main app can see its active work.

## Upload API Runtime Model

The API server runs its own worker thread and its own service graph.

Because it is a separate process:

- upload queue visibility is shared through database status
- runtime task visibility is shared through `runtime_tasks`
- stop and cancel requests must go through DB-backed control state

## Extending The System

### Add A New Workflow Step

1. Add schema/template support in [models.py](/d:/PRO/T001/automation_studio/models.py)
2. Add runtime handler in [engine.py](/d:/PRO/T001/automation_studio/automation/engine.py)
3. Add tests in:
   - [test_engine_steps.py](/d:/PRO/T001/tests/test_engine_steps.py)
   - optionally [test_services_phase4.py](/d:/PRO/T001/tests/test_services_phase4.py)
4. Ensure step editor renders fields correctly

### Add A New UI Page

1. Create page under `automation_studio/ui/pages`
2. Inject required services from [main_window.py](/d:/PRO/T001/automation_studio/ui/main_window.py)
3. Add page to `QStackedWidget`
4. Add navigation entry
5. Add tests if the page has runtime logic or background execution

### Add A New API Endpoint

1. Update [api_server.py](/d:/PRO/T001/automation_studio/api_server.py)
2. Reuse service-layer logic instead of duplicating business rules
3. Add HTTP tests in [test_upload_api.py](/d:/PRO/T001/tests/test_upload_api.py)

### Add A New Migration

1. Add a migration method in [database.py](/d:/PRO/T001/automation_studio/database.py)
2. Append it to `_migrations()`
3. Update migration tests in [test_database_migrations.py](/d:/PRO/T001/tests/test_database_migrations.py)
4. Keep migrations additive and idempotent

## Testing

Run the full suite:

```powershell
python -m unittest discover -s tests -v
```

Useful focused runs:

```powershell
python -m unittest tests.test_engine_steps -v
python -m unittest tests.test_services_phase4 -v
python -m unittest tests.test_uploads -v
python -m unittest tests.test_upload_api -v
python -m unittest tests.test_screen_viewer -v
```

Compile sanity:

```powershell
python -m py_compile automation_studio\services.py automation_studio\automation\engine.py automation_studio\api_server.py
```

## Running In Development

### Main UI

```powershell
python main.py
```

### Upload API

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 127.0.0.1 --port 8000
```

### Screen Wall Process Directly

```powershell
python -m automation_studio.viewer_process --db-path automation_studio.db --refresh-ms 1000
```

## Concurrency Rules

Current design principles:

- database is the shared source of truth across processes
- `runtime_locks` prevent conflicting execution
- `runtime_tasks` expose visibility and control
- stop is graceful, not destructive
- queued work can be cancelled

If you add a new execution path, make sure it does both:

1. acquire or respect execution locks
2. register a shared runtime task if users must monitor or stop it

## Practical Recommendations

- Keep business rules in services, not in UI pages
- Keep SQL in repositories
- Add migrations instead of editing live schema assumptions
- Reuse shared runtime infrastructure for any new background runner
- Prefer additive changes to runtime state instead of creating another in-memory-only queue

## Documentation Map

- User manual: [USER_GUIDE.md](/d:/PRO/T001/docs/USER_GUIDE.md)
- Developer guide: [DEVELOPER_GUIDE.md](/d:/PRO/T001/docs/DEVELOPER_GUIDE.md)
