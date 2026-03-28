from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from automation_studio.api_server import create_api_application, create_http_server
from automation_studio.database import DatabaseManager
from automation_studio.repositories import DeviceRepository
from automation_studio.services import DeviceService


class FakeUploadDevice:
    def __init__(self) -> None:
        self.actions: list[tuple] = []
        self.info = {"manufacturer": "Google", "model": "Pixel 7", "version": "14"}

    def shell(self, command: str):
        self.actions.append(("shell", command))
        return ("ok", 0)

    def window_size(self):
        return (1080, 2400)

    def app_current(self):
        return {"package": "com.example.app", "activity": "MainActivity"}

    def screen_on(self):
        return True


class FakeUploadDeviceService(DeviceService):
    def __init__(self, device_repository: DeviceRepository, device: FakeUploadDevice) -> None:
        super().__init__(device_repository)
        self.fake_device = device

    def connect_device(self, serial: str):
        return self.fake_device


class UploadApiTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = DatabaseManager(self.db_path)
        self.db.init_schema()
        device_repository = DeviceRepository(self.db)
        self.fake_device = FakeUploadDevice()
        self.device_service = FakeUploadDeviceService(device_repository, self.fake_device)
        self.app = create_api_application(self.db_path, device_service=self.device_service)
        self.server = create_http_server(self.app, host="127.0.0.1", port=0)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()

        self.device_id = self.app.device_service.save_device(None, "Phone", "SERIAL1", "")
        self.workflow_id = self.app.workflow_service.save_workflow(None, "Upload Workflow", "", True)
        self.app.workflow_service.save_step(
            None,
            self.workflow_id,
            1,
            "Echo Upload Title",
            "shell",
            json.dumps({"command": "echo ${upload.get('title')}"}, ensure_ascii=False),
            True,
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _request(self, method: str, path: str, payload: dict | None = None, headers: dict[str, str] | None = None):
        body = None
        request_headers = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            request_headers.update(headers)
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(f"{self.base_url}{path}", data=body, headers=request_headers, method=method)
        with urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))

    def _wait_for_upload_status(self, upload_job_id: int, expected_status: str, *, timeout: float = 5.0) -> dict:
        deadline = time.time() + timeout
        last_item: dict = {}
        while time.time() < deadline:
            status, payload = self._request("GET", f"/api/uploads/{upload_job_id}")
            self.assertEqual(status, 200)
            last_item = payload["item"]
            if str(last_item.get("status") or "") == expected_status:
                return last_item
            time.sleep(0.1)
        self.fail(f"Upload job #{upload_job_id} did not reach status '{expected_status}'. Last item: {last_item!r}")

    def test_create_list_get_and_run_upload_job_via_api(self) -> None:
        create_payload = {
            "device_id": self.device_id,
            "workflow_id": self.workflow_id,
            "code_product": "SKU-API",
            "link_product": "https://example.com/api",
            "title": "Upload From API",
            "description": "Created by API",
            "tags": ["#api", "#upload"],
            "video_url": "https://cdn.example.com/api.mp4",
            "metadata": {"source": "api"},
        }

        status, created = self._request("POST", "/api/uploads", create_payload)
        self.assertEqual(status, 201)
        upload_job_id = created["item"]["id"]
        self.assertEqual(created["item"]["tags"], ["#api", "#upload"])
        self.assertEqual(created["item"]["metadata"], {"source": "api"})

        status, jobs = self._request("GET", "/api/uploads")
        self.assertEqual(status, 200)
        self.assertEqual(len(jobs["items"]), 1)
        self.assertEqual(jobs["items"][0]["id"], upload_job_id)

        status, job = self._request("GET", f"/api/uploads/{upload_job_id}")
        self.assertEqual(status, 200)
        self.assertEqual(job["item"]["title"], "Upload From API")

        status, run_result = self._request("POST", f"/api/uploads/{upload_job_id}/run", {})
        self.assertEqual(status, 202)
        self.assertTrue(run_result["queued"])
        finished_item = self._wait_for_upload_status(upload_job_id, "success")
        self.assertEqual(self.fake_device.actions, [("shell", "echo Upload From API")])
        self.assertEqual(finished_item["status"], "success")

    def test_reference_endpoints_return_devices_and_workflows(self) -> None:
        status, devices = self._request("GET", "/api/devices")
        self.assertEqual(status, 200)
        self.assertEqual(devices["items"][0]["id"], self.device_id)

        status, workflows = self._request("GET", "/api/workflows")
        self.assertEqual(status, 200)
        self.assertEqual(workflows["items"][0]["id"], self.workflow_id)

    def test_template_and_summary_endpoints(self) -> None:
        template_payload = {
            "name": "TikTok Upload Base",
            "description": "Default upload values",
            "device_id": self.device_id,
            "workflow_id": self.workflow_id,
            "title": "Template Title",
            "description_template": "Template Description",
            "tags": ["#template", "#upload"],
            "video_url": "https://cdn.example.com/template.mp4",
            "metadata": {"template": True},
        }

        status, created = self._request("POST", "/api/upload-templates", template_payload)
        self.assertEqual(status, 201)
        template_id = created["item"]["id"]
        self.assertEqual(created["item"]["tags"], ["#template", "#upload"])

        status, templates = self._request("GET", "/api/upload-templates")
        self.assertEqual(status, 200)
        self.assertEqual(len(templates["items"]), 1)
        self.assertEqual(templates["items"][0]["id"], template_id)

        status, template = self._request("GET", f"/api/upload-templates/{template_id}")
        self.assertEqual(status, 200)
        self.assertEqual(template["item"]["name"], "TikTok Upload Base")

        status, updated = self._request(
            "PUT",
            f"/api/upload-templates/{template_id}",
            {
                "name": "TikTok Upload Updated",
                "tags": ["#updated"],
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(updated["item"]["name"], "TikTok Upload Updated")
        self.assertEqual(updated["item"]["tags"], ["#updated"])

        status, summary = self._request("GET", "/api/uploads/summary")
        self.assertEqual(status, 200)
        self.assertEqual(summary["item"]["template_count"], 1)
        self.assertEqual(summary["item"]["total_jobs"], 0)

        status, deleted = self._request("DELETE", f"/api/upload-templates/{template_id}")
        self.assertEqual(status, 200)
        self.assertEqual(deleted["deleted_id"], template_id)

    def test_batch_export_and_import_endpoints(self) -> None:
        payloads = [
            {
                "device_id": self.device_id,
                "workflow_id": self.workflow_id,
                "code_product": f"SKU-{index}",
                "link_product": f"https://example.com/{index}",
                "title": f"Upload {index}",
                "description": "Batch",
                "tags": [f"#tag{index}"],
                "video_url": f"https://cdn.example.com/{index}.mp4",
            }
            for index in (1, 2)
        ]
        upload_job_ids: list[int] = []
        for payload in payloads:
            status, created = self._request("POST", "/api/uploads", payload)
            self.assertEqual(status, 201)
            upload_job_ids.append(int(created["item"]["id"]))

        status, batch = self._request(
            "POST",
            "/api/uploads/run-batch",
            {"upload_job_ids": upload_job_ids, "continue_on_error": True},
        )
        self.assertEqual(status, 202)
        self.assertTrue(batch["queued"])
        self.assertEqual(len(batch["items"]), 2)
        for upload_job_id in upload_job_ids:
            self._wait_for_upload_status(upload_job_id, "success")

        status, exported = self._request("POST", "/api/uploads/export", {"upload_job_ids": [upload_job_ids[0]]})
        self.assertEqual(status, 200)
        self.assertEqual(len(exported["item"]["jobs"]), 1)
        exported_job = exported["item"]["jobs"][0]
        self.assertEqual(exported_job["code_product"], "SKU-1")

        status, imported = self._request("POST", "/api/uploads/import", exported["item"])
        self.assertEqual(status, 201)
        self.assertEqual(len(imported["created_ids"]), 1)
        self.assertEqual(imported["items"][0]["code_product"], "SKU-1")

    def test_can_create_job_from_template_and_filter_uploads(self) -> None:
        status, created_template = self._request(
            "POST",
            "/api/upload-templates",
            {
                "name": "Base Template",
                "device_id": self.device_id,
                "workflow_id": self.workflow_id,
                "title": "Template Upload",
                "description_template": "Template Description",
                "tags": ["#base"],
                "video_url": "https://cdn.example.com/base.mp4",
                "metadata": {"source": "template"},
            },
        )
        self.assertEqual(status, 201)
        template_id = int(created_template["item"]["id"])

        status, created_job = self._request(
            "POST",
            "/api/uploads/from-template",
            {
                "template_id": template_id,
                "code_product": "SKU-TEMPLATE",
                "title": "Template Override",
            },
        )
        self.assertEqual(status, 201)
        upload_job_id = int(created_job["item"]["id"])
        self.assertEqual(created_job["item"]["title"], "Template Override")
        self.assertEqual(created_job["item"]["metadata"], {"source": "template"})

        status, filtered = self._request("GET", f"/api/uploads?device_id={self.device_id}&status=draft")
        self.assertEqual(status, 200)
        self.assertEqual(len(filtered["items"]), 1)
        self.assertEqual(filtered["items"][0]["id"], upload_job_id)

        status, filtered_templates = self._request("GET", f"/api/upload-templates?device_id={self.device_id}&active_only=true")
        self.assertEqual(status, 200)
        self.assertEqual(len(filtered_templates["items"]), 1)
        self.assertEqual(filtered_templates["items"][0]["id"], template_id)

    def test_retry_result_and_delete_upload_job_endpoints(self) -> None:
        status, created = self._request(
            "POST",
            "/api/uploads",
            {
                "device_id": self.device_id,
                "workflow_id": self.workflow_id,
                "code_product": "SKU-RETRY",
                "link_product": "https://example.com/retry",
                "title": "Retry Upload",
                "description": "Retry me",
                "tags": ["#retry"],
                "video_url": "https://cdn.example.com/retry.mp4",
            },
        )
        self.assertEqual(status, 201)
        upload_job_id = int(created["item"]["id"])

        status, first_run = self._request("POST", f"/api/uploads/{upload_job_id}/run", {})
        self.assertEqual(status, 202)
        self.assertTrue(first_run["queued"])
        self._wait_for_upload_status(upload_job_id, "success")

        status, retry_run = self._request("POST", f"/api/uploads/{upload_job_id}/retry", {})
        self.assertEqual(status, 202)
        self.assertTrue(retry_run["queued"])
        self._wait_for_upload_status(upload_job_id, "success")
        self.assertEqual(self.fake_device.actions, [("shell", "echo Retry Upload"), ("shell", "echo Retry Upload")])

        status, result_payload = self._request("GET", f"/api/uploads/{upload_job_id}/result")
        self.assertEqual(status, 200)
        self.assertEqual(result_payload["item"]["status"], "success")
        self.assertTrue(result_payload["item"]["result"]["success"])

        status, deleted = self._request("DELETE", f"/api/uploads/{upload_job_id}")
        self.assertEqual(status, 200)
        self.assertEqual(deleted["deleted_id"], upload_job_id)

        with self.assertRaises(HTTPError) as ctx:
            self._request("GET", f"/api/uploads/{upload_job_id}")
        self.assertEqual(ctx.exception.code, 404)

    def test_api_key_can_protect_endpoints(self) -> None:
        protected_app = create_api_application(self.db_path, device_service=self.device_service, api_key="secret-key")
        protected_server = create_http_server(protected_app, host="127.0.0.1", port=0)
        thread = threading.Thread(target=protected_server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{protected_server.server_port}"
        try:
            request = Request(f"{base_url}/api/health", method="GET")
            with self.assertRaises(HTTPError) as ctx:
                urlopen(request, timeout=10)
            self.assertEqual(ctx.exception.code, 401)

            request = Request(f"{base_url}/api/health", headers={"X-API-Key": "secret-key"}, method="GET")
            with urlopen(request, timeout=10) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["success"])
        finally:
            protected_server.shutdown()
            protected_server.server_close()

    def test_second_run_request_is_rejected_while_job_is_queued_or_running(self) -> None:
        status, created = self._request(
            "POST",
            "/api/uploads",
            {
                "device_id": self.device_id,
                "workflow_id": self.workflow_id,
                "code_product": "SKU-QUEUE",
                "link_product": "https://example.com/queue",
                "title": "Queue Upload",
                "description": "Queue me",
                "tags": ["#queue"],
                "video_url": "https://cdn.example.com/queue.mp4",
            },
        )
        self.assertEqual(status, 201)
        upload_job_id = int(created["item"]["id"])

        status, queued = self._request("POST", f"/api/uploads/{upload_job_id}/run", {})
        self.assertEqual(status, 202)
        self.assertTrue(queued["queued"])

        with self.assertRaises(HTTPError) as ctx:
            self._request("POST", f"/api/uploads/{upload_job_id}/run", {})
        self.assertEqual(ctx.exception.code, 409)

        self._wait_for_upload_status(upload_job_id, "success")


if __name__ == "__main__":
    unittest.main()
