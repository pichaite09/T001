# API Guide

## Purpose

This guide documents the `Upload API` implemented in [api_server.py](/d:/PRO/T001/automation_studio/api_server.py).

The API is focused on:

- reference data lookup
- upload job CRUD
- upload execution queue
- upload template CRUD
- upload import/export

Base URL examples in this guide use:

```text
http://127.0.0.1:8000
```

## Run The API

From the project root:

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 127.0.0.1 --port 8000
```

Expose on LAN:

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 0.0.0.0 --port 8000
```

Protect with API key:

```powershell
python -m automation_studio.api_server --db-path automation_studio.db --host 127.0.0.1 --port 8000 --api-key your-secret-key
```

## Authentication

If the server is started with `--api-key`, every request must include:

```http
X-API-Key: your-secret-key
```

Example:

```powershell
curl.exe -H "X-API-Key: your-secret-key" http://127.0.0.1:8000/api/health
```

If the key is missing or incorrect, the server returns:

```json
{
  "success": false,
  "message": "Unauthorized"
}
```

with HTTP `401`.

## Content Type

For `POST`, `PUT`, and `DELETE` requests that send JSON bodies, use:

```http
Content-Type: application/json; charset=utf-8
```

Examples in this guide use `curl.exe` because Windows PowerShell aliases `curl` to `Invoke-WebRequest`.

## Response Shape

Most responses follow one of these patterns.

Single item:

```json
{
  "success": true,
  "item": {}
}
```

List:

```json
{
  "success": true,
  "items": []
}
```

Queued execution:

```json
{
  "success": true,
  "queued": true,
  "queued_ids": [1]
}
```

Error:

```json
{
  "success": false,
  "message": "Error message"
}
```

## Common HTTP Status Codes

- `200` request completed successfully
- `201` resource created
- `202` request accepted and queued
- `400` invalid body or invalid arguments
- `401` unauthorized
- `404` resource not found
- `405` method not allowed
- `409` upload job is already queued or running
- `500` internal server error

## Data Model Overview

The API mainly exposes these objects.

### Device

Example:

```json
{
  "id": 1,
  "name": "Phone",
  "serial": "SERIAL1",
  "last_status": "connected",
  "last_seen": "2026-03-29 10:00:00"
}
```

### Workflow

Example:

```json
{
  "id": 13,
  "name": "shopee-upload",
  "description": "",
  "is_active": true
}
```

### Device Platform

Example:

```json
{
  "id": 2,
  "device_id": 1,
  "platform_key": "shopee",
  "platform_name": "shopee",
  "package_name": "com.shopee.th",
  "current_account_id": 10,
  "is_enabled": true
}
```

### Account

Example:

```json
{
  "id": 10,
  "device_platform_id": 2,
  "display_name": "shop1",
  "username": "shop1",
  "login_id": "shop1",
  "is_enabled": true
}
```

### Upload Job

Example:

```json
{
  "id": 1,
  "device_id": 1,
  "device_platform_id": 2,
  "account_id": 10,
  "workflow_id": 13,
  "code_product": "SKU-001",
  "link_product": "https://example.com/product/1",
  "title": "โพสต์ใหม่",
  "description": "รายละเอียดโพสต์",
  "tags": ["#iblanc", "#sale"],
  "video_url": "https://cdn.example.com/video.mp4",
  "cover_url": "https://cdn.example.com/cover.jpg",
  "local_video_path": "",
  "metadata": {
    "campaign": "launch"
  },
  "status": "draft",
  "last_error": "",
  "result": {}
}
```

### Upload Template

Example:

```json
{
  "id": 1,
  "name": "Shopee Base",
  "description": "Default values",
  "device_id": 1,
  "device_platform_id": 2,
  "account_id": 10,
  "workflow_id": 13,
  "code_product": "",
  "link_product": "",
  "title": "Template Title",
  "description_template": "Template Description",
  "tags": ["#sale", "#iblanc"],
  "video_url": "https://cdn.example.com/base.mp4",
  "cover_url": "",
  "local_video_path": "",
  "metadata": {
    "source": "template"
  }
}
```

## Endpoint Reference

### 1. Health

#### `GET /api/health`

Purpose:
- confirm that the server is running

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/health
```

Response:

```json
{
  "success": true,
  "status": "ok"
}
```

### 2. Devices

#### `GET /api/devices`

Purpose:
- list all devices

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/devices
```

Response:

```json
{
  "success": true,
  "items": [
    {
      "id": 1,
      "name": "Phone",
      "serial": "SERIAL1",
      "last_status": "connected",
      "last_seen": "2026-03-29 10:00:00"
    }
  ]
}
```

### 3. Workflows

#### `GET /api/workflows`

Purpose:
- list workflows that can be assigned to upload jobs

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/workflows
```

Response:

```json
{
  "success": true,
  "items": [
    {
      "id": 13,
      "name": "shopee-upload",
      "description": "",
      "is_active": true
    }
  ]
}
```

### 4. Platforms By Device

#### `GET /api/devices/{device_id}/platforms`

Purpose:
- list platforms configured on a device

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/devices/1/platforms
```

Response:

```json
{
  "success": true,
  "items": [
    {
      "id": 2,
      "device_id": 1,
      "platform_key": "shopee",
      "platform_name": "shopee",
      "package_name": "com.shopee.th",
      "current_account_id": 10,
      "is_enabled": true
    }
  ]
}
```

### 5. Accounts By Device Platform

#### `GET /api/device-platforms/{platform_id}/accounts`

Purpose:
- list accounts inside a device platform

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/device-platforms/2/accounts
```

Response:

```json
{
  "success": true,
  "items": [
    {
      "id": 10,
      "device_platform_id": 2,
      "display_name": "shop1",
      "username": "shop1",
      "login_id": "shop1",
      "is_enabled": true
    }
  ]
}
```

### 6. Upload Jobs

#### `GET /api/uploads`

Purpose:
- list upload jobs

Supported query parameters:
- `status`
- `device_id`
- `workflow_id`
- `account_id`
- `device_platform_id`

Examples:

All jobs:

```powershell
curl.exe http://127.0.0.1:8000/api/uploads
```

Filter by device and status:

```powershell
curl.exe "http://127.0.0.1:8000/api/uploads?device_id=1&status=draft"
```

Filter by workflow:

```powershell
curl.exe "http://127.0.0.1:8000/api/uploads?workflow_id=13"
```

Response:

```json
{
  "success": true,
  "items": [
    {
      "id": 1,
      "device_id": 1,
      "workflow_id": 13,
      "title": "โพสต์ใหม่",
      "tags": ["#iblanc", "#sale"],
      "metadata": {
        "campaign": "launch"
      },
      "status": "draft",
      "result": {}
    }
  ]
}
```

#### `POST /api/uploads`

Purpose:
- create a new upload job

Body fields:
- `device_id` required
- `workflow_id` required
- `device_platform_id` optional
- `account_id` optional
- `code_product` optional
- `link_product` optional
- `title` optional but usually needed by your workflow
- `description` optional
- `tags` optional, array of strings recommended
- `video_url` optional
- `cover_url` optional
- `local_video_path` optional
- `metadata` optional, object recommended

Example request:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads ^
  -H "Content-Type: application/json" ^
  -d "{\"device_id\":1,\"device_platform_id\":2,\"account_id\":10,\"workflow_id\":13,\"code_product\":\"SKU-001\",\"link_product\":\"https://example.com/product/1\",\"title\":\"โพสต์ใหม่\",\"description\":\"รายละเอียดโพสต์\",\"tags\":[\"#iblanc\",\"#sale\"],\"video_url\":\"https://cdn.example.com/video.mp4\",\"cover_url\":\"https://cdn.example.com/cover.jpg\",\"local_video_path\":\"\",\"metadata\":{\"campaign\":\"launch\"}}"
```

Response:

```json
{
  "success": true,
  "item": {
    "id": 1,
    "device_id": 1,
    "device_platform_id": 2,
    "account_id": 10,
    "workflow_id": 13,
    "code_product": "SKU-001",
    "link_product": "https://example.com/product/1",
    "title": "โพสต์ใหม่",
    "description": "รายละเอียดโพสต์",
    "tags": ["#iblanc", "#sale"],
    "video_url": "https://cdn.example.com/video.mp4",
    "cover_url": "https://cdn.example.com/cover.jpg",
    "local_video_path": "",
    "metadata": {
      "campaign": "launch"
    },
    "status": "draft"
  }
}
```

#### `GET /api/uploads/{upload_job_id}`

Purpose:
- get a single upload job

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/uploads/1
```

Response:

```json
{
  "success": true,
  "item": {
    "id": 1,
    "title": "โพสต์ใหม่",
    "status": "draft",
    "tags": ["#iblanc", "#sale"],
    "metadata": {
      "campaign": "launch"
    },
    "result": {}
  }
}
```

#### `PUT /api/uploads/{upload_job_id}`

Purpose:
- update an existing upload job

You can send only the fields you want to change.

Example:

```powershell
curl.exe -X PUT http://127.0.0.1:8000/api/uploads/1 ^
  -H "Content-Type: application/json" ^
  -d "{\"title\":\"โพสต์ใหม่แก้ไขแล้ว\",\"tags\":[\"#updated\",\"#iblanc\"]}"
```

Response:

```json
{
  "success": true,
  "item": {
    "id": 1,
    "title": "โพสต์ใหม่แก้ไขแล้ว",
    "tags": ["#updated", "#iblanc"]
  }
}
```

#### `DELETE /api/uploads/{upload_job_id}`

Purpose:
- delete an upload job

Example:

```powershell
curl.exe -X DELETE http://127.0.0.1:8000/api/uploads/1
```

Response:

```json
{
  "success": true,
  "deleted_id": 1
}
```

#### `POST /api/uploads/{upload_job_id}/run`

Purpose:
- queue one upload job for execution

Important:
- this endpoint returns `202`
- execution is asynchronous
- poll the job afterwards to watch the status

Example:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads/1/run ^
  -H "Content-Type: application/json" ^
  -d "{}"
```

Response:

```json
{
  "success": true,
  "queued": true,
  "queued_ids": [1],
  "item": {
    "id": 1,
    "status": "queued"
  }
}
```

Possible conflict response if already queued or running:

```json
{
  "success": false,
  "message": "Upload job is already queued or running"
}
```

with HTTP `409`.

#### `POST /api/uploads/{upload_job_id}/retry`

Purpose:
- queue an already existing upload job again

Behavior:
- same queue behavior as `/run`

Example:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads/1/retry ^
  -H "Content-Type: application/json" ^
  -d "{}"
```

#### `GET /api/uploads/{upload_job_id}/result`

Purpose:
- get compact status/result payload for an upload job

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/uploads/1/result
```

Response:

```json
{
  "success": true,
  "item": {
    "id": 1,
    "status": "success",
    "last_error": "",
    "result": {
      "success": true
    }
  }
}
```

### 7. Upload Summary

#### `GET /api/uploads/summary`

Purpose:
- return upload dashboard numbers

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/uploads/summary
```

Response example:

```json
{
  "success": true,
  "item": {
    "total_jobs": 12,
    "draft_count": 3,
    "running_count": 1,
    "success_count": 7,
    "failed_count": 1,
    "template_count": 2
  }
}
```

### 8. Create Upload Job From Template

#### `POST /api/uploads/from-template`

Purpose:
- create a new upload job by starting from a template and overriding selected fields

Required body field:
- `template_id`

Optional override fields:
- `device_id`
- `device_platform_id`
- `account_id`
- `workflow_id`
- `code_product`
- `link_product`
- `title`
- `description`
- `tags`
- `video_url`
- `cover_url`
- `local_video_path`
- `metadata`

Example:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads/from-template ^
  -H "Content-Type: application/json" ^
  -d "{\"template_id\":1,\"code_product\":\"SKU-OVERRIDE\",\"title\":\"ชื่อที่ override\"}"
```

Response:

```json
{
  "success": true,
  "item": {
    "id": 5,
    "code_product": "SKU-OVERRIDE",
    "title": "ชื่อที่ override"
  }
}
```

### 9. Batch Execution

#### `POST /api/uploads/run-batch`

Purpose:
- queue multiple upload jobs

Body fields:
- `upload_job_ids` required, non-empty list
- `continue_on_error` optional, default `true`

Example:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads/run-batch ^
  -H "Content-Type: application/json" ^
  -d "{\"upload_job_ids\":[1,2,3],\"continue_on_error\":true}"
```

Response:

```json
{
  "success": true,
  "queued": true,
  "queued_ids": [1, 2, 3],
  "continue_on_error": true,
  "items": [
    {"id": 1, "status": "queued"},
    {"id": 2, "status": "queued"},
    {"id": 3, "status": "queued"}
  ]
}
```

Behavior:
- if `continue_on_error` is `false`, remaining queued jobs in that batch are moved back to `draft` if an earlier job fails

### 10. Export Upload Jobs

#### `POST /api/uploads/export`

Purpose:
- export upload jobs as a JSON payload that can later be imported

Body:
- `upload_job_ids` optional list

If omitted:
- all upload jobs are exported

Example:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads/export ^
  -H "Content-Type: application/json" ^
  -d "{\"upload_job_ids\":[1]}"
```

Response example:

```json
{
  "success": true,
  "item": {
    "jobs": [
      {
        "device_id": 1,
        "workflow_id": 13,
        "code_product": "SKU-1",
        "title": "Upload 1",
        "description": "Batch",
        "tags": ["#tag1"],
        "video_url": "https://cdn.example.com/1.mp4"
      }
    ],
    "templates": []
  }
}
```

### 11. Import Upload Jobs

#### `POST /api/uploads/import`

Purpose:
- import upload jobs and templates from an export payload

Body:
- the JSON object previously returned in `item` by `/api/uploads/export`

Example:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads/import ^
  -H "Content-Type: application/json" ^
  -d "{\"jobs\":[{\"device_id\":1,\"workflow_id\":13,\"code_product\":\"SKU-1\",\"title\":\"Upload 1\",\"description\":\"Batch\",\"tags\":[\"#tag1\"],\"video_url\":\"https://cdn.example.com/1.mp4\"}],\"templates\":[]}"
```

Response:

```json
{
  "success": true,
  "created_ids": [7],
  "items": [
    {
      "id": 7,
      "title": "Upload 1"
    }
  ]
}
```

### 12. Upload Templates

#### `GET /api/upload-templates`

Purpose:
- list upload templates

Supported query parameters:
- `device_id`
- `workflow_id`
- `active_only`

Examples:

All templates:

```powershell
curl.exe http://127.0.0.1:8000/api/upload-templates
```

Only active templates for one device:

```powershell
curl.exe "http://127.0.0.1:8000/api/upload-templates?device_id=1&active_only=true"
```

Response:

```json
{
  "success": true,
  "items": [
    {
      "id": 1,
      "name": "Shopee Base",
      "tags": ["#sale", "#iblanc"],
      "metadata": {
        "source": "template"
      }
    }
  ]
}
```

#### `POST /api/upload-templates`

Purpose:
- create a template

Body fields:
- `name` required in practice
- `description`
- `device_id`
- `device_platform_id`
- `account_id`
- `workflow_id`
- `code_product`
- `link_product`
- `title`
- `description_template`
- `tags`
- `video_url`
- `cover_url`
- `local_video_path`
- `metadata`

Example:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/upload-templates ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"Shopee Base\",\"description\":\"Default values\",\"device_id\":1,\"workflow_id\":13,\"title\":\"Template Title\",\"description_template\":\"Template Description\",\"tags\":[\"#sale\",\"#iblanc\"],\"video_url\":\"https://cdn.example.com/base.mp4\",\"metadata\":{\"source\":\"template\"}}"
```

Response:

```json
{
  "success": true,
  "item": {
    "id": 1,
    "name": "Shopee Base",
    "description_template": "Template Description",
    "tags": ["#sale", "#iblanc"]
  }
}
```

#### `GET /api/upload-templates/{template_id}`

Purpose:
- fetch a single template

Example:

```powershell
curl.exe http://127.0.0.1:8000/api/upload-templates/1
```

#### `PUT /api/upload-templates/{template_id}`

Purpose:
- update a template

Example:

```powershell
curl.exe -X PUT http://127.0.0.1:8000/api/upload-templates/1 ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"Shopee Base Updated\",\"tags\":[\"#updated\"]}"
```

Response:

```json
{
  "success": true,
  "item": {
    "id": 1,
    "name": "Shopee Base Updated",
    "tags": ["#updated"]
  }
}
```

#### `DELETE /api/upload-templates/{template_id}`

Purpose:
- delete a template

Example:

```powershell
curl.exe -X DELETE http://127.0.0.1:8000/api/upload-templates/1
```

Response:

```json
{
  "success": true,
  "deleted_id": 1
}
```

## Practical Flows

### Flow 1: Create And Run A Single Upload Job

1. `GET /api/devices`
2. `GET /api/workflows`
3. If needed:
   - `GET /api/devices/{device_id}/platforms`
   - `GET /api/device-platforms/{platform_id}/accounts`
4. `POST /api/uploads`
5. `POST /api/uploads/{id}/run`
6. Poll `GET /api/uploads/{id}` until `status` becomes `success` or `failed`
7. Optionally read `GET /api/uploads/{id}/result`

### Flow 2: Create From Template

1. `POST /api/upload-templates`
2. `POST /api/uploads/from-template`
3. `POST /api/uploads/{id}/run`
4. Poll for status

### Flow 3: Batch Upload

1. Create multiple upload jobs
2. `POST /api/uploads/run-batch`
3. Poll each job with `GET /api/uploads/{id}`

### Flow 4: Export And Import

1. `POST /api/uploads/export`
2. Store the returned `item`
3. `POST /api/uploads/import`

## Notes About Queueing

The API now runs upload execution through an internal worker queue.

That means:

- `/run` returns `202` and queues the job
- `/retry` returns `202` and queues the job
- `/run-batch` returns `202` and queues the batch
- duplicate queueing is rejected with `409`

Good polling pattern:

1. call `/run`
2. if response is `202`, store the job ID
3. poll `/api/uploads/{id}` every 1 to 3 seconds
4. stop polling when `status` is `success` or `failed`

## Common Errors

### `400 Invalid JSON body`

Cause:
- malformed JSON

Fix:
- verify commas, quotes, and braces

### `404 Upload job not found`

Cause:
- wrong upload job ID

Fix:
- list jobs first with `GET /api/uploads`

### `404 Upload template not found`

Cause:
- wrong template ID

Fix:
- list templates first with `GET /api/upload-templates`

### `409 Upload job is already queued or running`

Cause:
- the same job was submitted twice

Fix:
- poll existing job status instead of queueing again

### `401 Unauthorized`

Cause:
- missing or incorrect `X-API-Key`

Fix:
- include the right header

## Testing The API Quickly

Health:

```powershell
curl.exe http://127.0.0.1:8000/api/health
```

Create:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads ^
  -H "Content-Type: application/json" ^
  -d "{\"device_id\":1,\"workflow_id\":13,\"title\":\"Upload From API\",\"description\":\"Created by API\",\"tags\":[\"#api\",\"#upload\"],\"video_url\":\"https://cdn.example.com/api.mp4\"}"
```

Run:

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/uploads/1/run ^
  -H "Content-Type: application/json" ^
  -d "{}"
```

Read job:

```powershell
curl.exe http://127.0.0.1:8000/api/uploads/1
```

Read result:

```powershell
curl.exe http://127.0.0.1:8000/api/uploads/1/result
```

## Source References

- API entry point: [api_server.py](/d:/PRO/T001/automation_studio/api_server.py)
- Upload service: [services.py](/d:/PRO/T001/automation_studio/services.py)
- Upload repository: [repositories.py](/d:/PRO/T001/automation_studio/repositories.py)
- Upload API tests: [test_upload_api.py](/d:/PRO/T001/tests/test_upload_api.py)
