from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from PySide6 import QtCore, QtWidgets

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    AccountRepository,
    DeviceRepository,
    LogRepository,
    TelemetryRepository,
    UploadRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)
from automation_studio.services import (
    AccountService,
    DeviceService,
    LogService,
    TelemetryService,
    UploadService,
    WatcherService,
    WatcherTelemetryService,
    WorkflowService,
)
from automation_studio.ui.pages.uploads_page import UploadsPage


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


class UploadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        cls.app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def setUp(self) -> None:
        self.settings = QtCore.QSettings("AutomationStudio", "UploadsPage")
        self.settings.clear()
        self.settings.sync()
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = DatabaseManager(self.db_path)
        self.db.init_schema()
        self.workflow_repository = WorkflowRepository(self.db)
        self.device_repository = DeviceRepository(self.db)
        self.account_repository = AccountRepository(self.db)
        self.upload_repository = UploadRepository(self.db)
        self.log_repository = LogRepository(self.db)
        self.telemetry_repository = TelemetryRepository(self.db)
        self.watcher_repository = WatcherRepository(self.db)
        self.watcher_telemetry_repository = WatcherTelemetryRepository(self.db)
        self.log_service = LogService(self.log_repository)
        self.telemetry_service = TelemetryService(self.telemetry_repository)
        self.watcher_telemetry_service = WatcherTelemetryService(self.watcher_telemetry_repository)
        self.fake_device = FakeUploadDevice()
        self.device_service = FakeUploadDeviceService(self.device_repository, self.fake_device)
        self.watcher_service = WatcherService(
            self.watcher_repository,
            self.device_repository,
            self.device_service,
            self.log_service,
            self.watcher_telemetry_service,
        )
        self.account_service = AccountService(
            self.account_repository,
            self.device_repository,
            self.workflow_repository,
        )
        self.workflow_service = WorkflowService(
            self.workflow_repository,
            self.device_repository,
            self.device_service,
            self.log_service,
            self.telemetry_service,
            self.watcher_service,
            self.watcher_telemetry_service,
            self.account_service,
        )
        self.upload_service = UploadService(
            self.upload_repository,
            self.device_repository,
            self.workflow_repository,
            self.account_service,
            self.workflow_service,
        )
        self.workflow_service.bind_upload_service(self.upload_service)

    def tearDown(self) -> None:
        self.settings.clear()
        self.settings.sync()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _wait_for(self, predicate, *, timeout_ms: int = 3000) -> None:
        deadline = QtCore.QTime.currentTime().addMSecs(timeout_ms)
        while not predicate():
            if QtCore.QTime.currentTime() > deadline:
                self.fail("Timed out waiting for condition")
            self.app.processEvents()

    def test_execute_upload_job_injects_upload_context_into_workflow(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Upload Workflow", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Shell Upload",
            "shell",
            json.dumps({"command": "echo ${upload.get('title')} | ${upload.get('code_product')}"}, ensure_ascii=False),
            True,
        )
        device_platform_id = self.account_service.save_device_platform(
            None,
            device_id,
            "tiktok",
            "TikTok",
            "com.zhiliaoapp.musically",
            None,
            True,
        )
        account_id = self.account_service.save_account(
            None,
            device_platform_id,
            "creator-main",
            "creator_main",
            "creator_login",
            "",
            "{}",
            True,
        )
        upload_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=device_platform_id,
            account_id=account_id,
            workflow_id=workflow_id,
            code_product="SKU-123",
            link_product="https://example.com/product",
            title="Launch Promo",
            description="Promo description",
            tags_text="promo, launch",
            video_url="https://cdn.example.com/video.mp4",
        )

        result = self.upload_service.execute_upload_job(upload_job_id)

        self.assertTrue(result["success"])
        self.assertEqual(self.fake_device.actions, [("shell", "echo Launch Promo | SKU-123")])
        upload_job = self.upload_service.get_upload_job(upload_job_id)
        self.assertEqual(upload_job["status"], "success")
        self.assertEqual(json.loads(upload_job["tags_json"]), ["promo", "launch"])
        logs = self.log_service.list_logs(workflow_id=workflow_id, device_id=device_id, limit=20)
        start_log = next(log for log in logs if log["status"] == "workflow_started")
        metadata = json.loads(start_log["metadata"])
        self.assertEqual(metadata["upload_job_id"], upload_job_id)
        self.assertEqual(metadata["upload_code_product"], "SKU-123")
        self.assertTrue(any(log["status"] == "upload_success" for log in logs))

    def test_execute_upload_job_persists_downloaded_local_video_path(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        source_dir = Path(tempfile.mkdtemp())
        source_path = source_dir / "video_source.mp4"
        source_path.write_bytes(b"video")
        download_dir = source_dir / "downloads"

        workflow_id = self.workflow_service.save_workflow(None, "Upload Download Workflow", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Download",
            "download_video_asset",
            json.dumps(
                {
                    "video_url": str(source_path),
                    "directory": str(download_dir),
                    "filename": "saved.mp4",
                },
                ensure_ascii=False,
            ),
            True,
        )
        upload_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-DL",
            link_product="https://example.com/dl",
            title="Upload Download",
            description="",
            tags_text="",
            video_url=str(source_path),
        )

        result = self.upload_service.execute_upload_job(upload_job_id)

        self.assertTrue(result["success"])
        upload_job = self.upload_service.get_upload_job(upload_job_id)
        self.assertEqual(upload_job["local_video_path"], str(download_dir / "saved.mp4"))

    def test_prepare_upload_context_step_can_load_job_by_id(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Prepare Upload", "", True)
        upload_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-777",
            link_product="https://example.com/product-777",
            title="Clip 777",
            description="Desc 777",
            tags_text="t1, t2",
            video_url="https://cdn.example.com/777.mp4",
        )
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Prepare Upload",
            "prepare_upload_context",
            json.dumps({"upload_job_id": upload_job_id}, ensure_ascii=False),
            True,
        )
        self.workflow_service.save_step(
            None,
            workflow_id,
            2,
            "Use Upload Vars",
            "shell",
            json.dumps({"command": "echo ${upload.get('title')} | ${vars.get('upload_code_product')}"}, ensure_ascii=False),
            True,
        )

        result = self.workflow_service.execute_workflow(workflow_id, device_id)

        self.assertTrue(result["success"])
        self.assertEqual(self.fake_device.actions, [("shell", "echo Clip 777 | SKU-777")])

    def test_download_video_asset_step_stores_local_path_in_upload_context(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        source_dir = Path(tempfile.mkdtemp())
        source_path = source_dir / "video_source.mp4"
        source_path.write_bytes(b"video")
        target_dir = source_dir / "downloads"

        workflow_id = self.workflow_service.save_workflow(None, "Download Upload Asset", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Download Video",
            "download_video_asset",
            json.dumps(
                {
                    "video_url": str(source_path),
                    "directory": str(target_dir),
                    "filename": "copied.mp4",
                },
                ensure_ascii=False,
            ),
            True,
        )
        self.workflow_service.save_step(
            None,
            workflow_id,
            2,
            "Use Local Video Path",
            "shell",
            json.dumps({"command": "echo ${upload.get('local_video_path')}"}, ensure_ascii=False),
            True,
        )

        result = self.workflow_service.execute_workflow(
            workflow_id,
            device_id,
            extra_context={
                "upload": {"video_url": str(source_path)},
                "vars": {"upload_video_url": str(source_path)},
            },
        )

        copied_path = target_dir / "copied.mp4"
        self.assertTrue(result["success"])
        self.assertTrue(copied_path.exists())
        self.assertEqual(self.fake_device.actions[-1], ("shell", f"echo {copied_path}"))

    def test_push_file_to_device_step_stores_device_path_in_upload_context(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        source_dir = Path(tempfile.mkdtemp())
        source_path = source_dir / "video_source.mp4"
        source_path.write_bytes(b"video")

        workflow_id = self.workflow_service.save_workflow(None, "Push Upload Asset", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Push Video",
            "push_file_to_device",
            json.dumps(
                {
                    "local_path": str(source_path),
                    "device_path": "/sdcard/Movies/upload_video.mp4",
                    "create_parent": True,
                },
                ensure_ascii=False,
            ),
            True,
        )
        self.workflow_service.save_step(
            None,
            workflow_id,
            2,
            "Use Device Video Path",
            "shell",
            json.dumps({"command": "echo ${upload.get('device_video_path')}"}, ensure_ascii=False),
            True,
        )

        with patch("automation_studio.automation.engine.shutil.which", return_value="adb"), patch(
            "automation_studio.automation.engine.subprocess.run"
        ) as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout="", stderr=""),
                Mock(returncode=0, stdout="1 file pushed", stderr=""),
                Mock(returncode=0, stdout="/sdcard/Movies/upload_video.mp4", stderr=""),
                Mock(returncode=0, stdout="Broadcast completed", stderr=""),
            ]
            result = self.workflow_service.execute_workflow(workflow_id, device_id)

        self.assertTrue(result["success"])
        self.assertEqual(self.fake_device.actions[-1], ("shell", "echo /sdcard/Movies/upload_video.mp4"))
        self.assertEqual(mock_run.call_count, 4)
        self.assertEqual(
            mock_run.call_args_list[0].args[0],
            ["adb", "-s", "SERIAL1", "shell", "mkdir", "-p", "/sdcard/Movies"],
        )
        self.assertEqual(
            mock_run.call_args_list[1].args[0],
            ["adb", "-s", "SERIAL1", "push", str(source_path), "/sdcard/Movies/upload_video.mp4"],
        )
        self.assertEqual(
            mock_run.call_args_list[2].args[0],
            ["adb", "-s", "SERIAL1", "shell", "ls /sdcard/Movies/upload_video.mp4"],
        )

    def test_delete_local_file_step_removes_file_and_clears_context(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        source_dir = Path(tempfile.mkdtemp())
        source_path = source_dir / "video_source.mp4"
        source_path.write_bytes(b"video")

        workflow_id = self.workflow_service.save_workflow(None, "Delete Local Upload Asset", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Delete Video",
            "delete_local_file",
            json.dumps(
                {
                    "local_path": str(source_path),
                    "missing_ok": False,
                    "clear_upload_local_video_path": True,
                },
                ensure_ascii=False,
            ),
            True,
        )
        self.workflow_service.save_step(
            None,
            workflow_id,
            2,
            "Use Cleared Local Video Path",
            "shell",
            json.dumps({"command": "echo ${upload.get('local_video_path')}"}, ensure_ascii=False),
            True,
        )

        result = self.workflow_service.execute_workflow(
            workflow_id,
            device_id,
            extra_context={
                "upload": {"local_video_path": str(source_path)},
                "vars": {"upload_local_video_path": str(source_path)},
            },
        )

        self.assertTrue(result["success"])
        self.assertFalse(source_path.exists())
        self.assertEqual(self.fake_device.actions[-1], ("shell", "echo "))

    def test_download_video_asset_uses_headers_for_http_sources(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        target_dir = Path(tempfile.mkdtemp()) / "downloads"
        workflow_id = self.workflow_service.save_workflow(None, "Download With Headers", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Download Video",
            "download_video_asset",
            json.dumps(
                {
                    "video_url": "https://cdn.example.com/video.mp4",
                    "directory": str(target_dir),
                    "filename": "video.mp4",
                    "referer": "https://example.com/post/1",
                    "headers_json": json.dumps({"Authorization": "Bearer token-123"}),
                    "timeout_seconds": 30,
                },
                ensure_ascii=False,
            ),
            True,
        )

        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, size=-1):
                if getattr(self, "_done", False):
                    return b""
                self._done = True
                return b"video-binary"

        with patch("automation_studio.automation.engine.urlopen", return_value=FakeResponse()) as mock_urlopen:
            result = self.workflow_service.execute_workflow(workflow_id, device_id)

        self.assertTrue(result["success"])
        downloaded_path = target_dir / "video.mp4"
        self.assertTrue(downloaded_path.exists())
        request = mock_urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://cdn.example.com/video.mp4")
        self.assertEqual(request.get_header("Referer"), "https://example.com/post/1")
        self.assertEqual(request.get_header("Authorization"), "Bearer token-123")
        self.assertEqual(mock_urlopen.call_args.kwargs["timeout"], 30.0)

    def test_execute_upload_job_persists_cleared_local_video_path(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        source_dir = Path(tempfile.mkdtemp())
        source_path = source_dir / "video_source.mp4"
        source_path.write_bytes(b"video")

        workflow_id = self.workflow_service.save_workflow(None, "Upload Delete Workflow", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Delete Downloaded File",
            "delete_local_file",
            json.dumps(
                {
                    "local_path": "${upload.get('local_video_path')}",
                    "missing_ok": False,
                    "clear_upload_local_video_path": True,
                },
                ensure_ascii=False,
            ),
            True,
        )
        upload_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-CLR",
            link_product="https://example.com/clr",
            title="Upload Clear",
            description="",
            tags_text="",
            video_url="https://cdn.example.com/clr.mp4",
            local_video_path=str(source_path),
        )

        result = self.upload_service.execute_upload_job(upload_job_id)

        self.assertTrue(result["success"])
        upload_job = self.upload_service.get_upload_job(upload_job_id)
        self.assertEqual(upload_job["local_video_path"], "")
        self.assertFalse(source_path.exists())

    def test_uploads_page_builds_and_populates_workflows(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        self.workflow_service.save_workflow(None, "Upload Workflow", "", True)
        self.account_service.save_device_platform(
            None,
            device_id,
            "shopee",
            "Shopee",
            "com.shopee.th",
            None,
            True,
        )

        page = UploadsPage(
            self.upload_service,
            self.workflow_service,
            self.device_service,
            self.account_service,
        )

        self.assertGreaterEqual(page.device_combo.count(), 2)
        self.assertGreaterEqual(page.workflow_combo.count(), 2)
        page.close()

    def test_export_and_import_upload_jobs_roundtrip(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Upload Workflow", "", True)
        original_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-100",
            link_product="https://example.com/p100",
            title="Upload 100",
            description="Desc 100",
            tags_text="a, b",
            video_url="https://cdn.example.com/100.mp4",
        )

        exported = self.upload_service.export_upload_jobs([original_id])
        imported_ids = self.upload_service.import_upload_jobs(exported)

        self.assertEqual(len(imported_ids), 1)
        imported = self.upload_service.get_upload_job(imported_ids[0])
        self.assertEqual(imported["code_product"], "SKU-100")
        self.assertEqual(imported["title"], "Upload 100")
        self.assertEqual(json.loads(imported["tags_json"]), ["a", "b"])

    def test_save_upload_job_preserves_hashtag_tags(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Upload Workflow", "", True)

        upload_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-TAGS",
            link_product="https://example.com/tags",
            title="Upload Tags",
            description="",
            tags_text="#โทนเนอร์ใต้วงแขน #สาวผิวบอบบาง #ผิวคลีนมั่นใจ #ของมันต้องมี #IBLANC",
            video_url="https://cdn.example.com/tags.mp4",
        )

        upload_job = self.upload_service.get_upload_job(upload_job_id)

        self.assertEqual(
            json.loads(upload_job["tags_json"]),
            ["#โทนเนอร์ใต้วงแขน", "#สาวผิวบอบบาง", "#ผิวคลีนมั่นใจ", "#ของมันต้องมี", "#IBLANC"],
        )
        self.assertEqual(
            self.upload_service.tags_to_text(upload_job["tags_json"]),
            "#โทนเนอร์ใต้วงแขน #สาวผิวบอบบาง #ผิวคลีนมั่นใจ #ของมันต้องมี #IBLANC",
        )

    def test_execute_upload_jobs_batch_runs_all_requested_jobs(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Batch Upload Workflow", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Echo Upload Title",
            "shell",
            json.dumps({"command": "echo ${upload.get('title')}"}, ensure_ascii=False),
            True,
        )
        first_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-A",
            link_product="https://example.com/a",
            title="Upload A",
            description="",
            tags_text="",
            video_url="https://cdn.example.com/a.mp4",
        )
        second_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-B",
            link_product="https://example.com/b",
            title="Upload B",
            description="",
            tags_text="",
            video_url="https://cdn.example.com/b.mp4",
        )

        result = self.upload_service.execute_upload_jobs([first_job_id, second_job_id], continue_on_error=True)

        self.assertTrue(result["success"])
        self.assertEqual(result["success_count"], 2)
        self.assertEqual(
            self.fake_device.actions,
            [("shell", "echo Upload A"), ("shell", "echo Upload B")],
        )

    def test_save_upload_template_and_summary_support_phase4_fields(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Upload Workflow", "", True)
        template_id = self.upload_service.save_upload_template(
            None,
            name="Launch Template",
            description="Template for launch uploads",
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-T",
            link_product="https://example.com/template",
            title="Template Title",
            upload_description="Template Description",
            tags_text="alpha, beta",
            video_url="https://cdn.example.com/template.mp4",
            cover_url="https://cdn.example.com/template.jpg",
            local_video_path="D:/videos/template.mp4",
            metadata_text='{"campaign": "launch"}',
        )
        upload_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-900",
            link_product="https://example.com/900",
            title="Upload 900",
            description="Desc 900",
            tags_text="x, y",
            video_url="https://cdn.example.com/900.mp4",
            cover_url="https://cdn.example.com/900.jpg",
            local_video_path="D:/videos/900.mp4",
            metadata_text='{"batch": 9}',
        )
        self.upload_repository.mark_upload_finished(upload_job_id, status="success", result_json="{}")

        template = self.upload_service.get_upload_template(template_id)
        summary = self.upload_service.upload_summary()
        upload_job = self.upload_service.get_upload_job(upload_job_id)

        self.assertEqual(template["name"], "Launch Template")
        self.assertEqual(template["local_video_path"], "D:/videos/template.mp4")
        self.assertEqual(upload_job["cover_url"], "https://cdn.example.com/900.jpg")
        self.assertEqual(upload_job["local_video_path"], "D:/videos/900.mp4")
        self.assertEqual(summary["template_count"], 1)
        self.assertEqual(summary["success_count"], 1)
        self.assertEqual(summary["top_workflow"], "Upload Workflow")

    def test_uploads_page_applies_selected_template_to_form(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Upload Workflow", "", True)
        template_id = self.upload_service.save_upload_template(
            None,
            name="Page Template",
            description="",
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-PAGE",
            link_product="https://example.com/page",
            title="Page Title",
            upload_description="Page Description",
            tags_text="tag-a, tag-b",
            video_url="https://cdn.example.com/page.mp4",
            cover_url="https://cdn.example.com/page.jpg",
            local_video_path="D:/videos/page.mp4",
            metadata_text='{"source": "template"}',
        )

        page = UploadsPage(
            self.upload_service,
            self.workflow_service,
            self.device_service,
            self.account_service,
        )
        page.refresh_templates()
        page._set_combo_data(page.template_combo, template_id)
        page.apply_selected_template()

        self.assertEqual(page.title_input.text(), "Page Title")
        self.assertEqual(page.cover_url_input.text(), "https://cdn.example.com/page.jpg")
        self.assertEqual(page.local_video_path_input.text(), "D:/videos/page.mp4")
        self.assertIn('"source": "template"', page.metadata_input.toPlainText())
        page.close()

    def test_uploads_page_restores_auto_runner_settings(self) -> None:
        self.settings.setValue("auto_run_enabled", True)
        self.settings.setValue("auto_run_interval_seconds", 30)
        self.settings.sync()

        page = UploadsPage(
            self.upload_service,
            self.workflow_service,
            self.device_service,
            self.account_service,
        )

        self.assertTrue(page.auto_run_checkbox.isChecked())
        self.assertEqual(int(page.auto_run_interval_combo.currentData() or 0), 30)
        self.assertTrue(page._auto_run_timer.isActive())
        page.close()

    def test_uploads_page_auto_runner_executes_next_draft_job(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Auto Upload Workflow", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Echo Upload Title",
            "shell",
            json.dumps({"command": "echo ${upload.get('title')}"}, ensure_ascii=False),
            True,
        )
        upload_job_id = self.upload_service.save_upload_job(
            None,
            device_id=device_id,
            device_platform_id=None,
            account_id=None,
            workflow_id=workflow_id,
            code_product="SKU-AUTO",
            link_product="https://example.com/auto",
            title="Auto Upload",
            description="",
            tags_text="",
            video_url="https://cdn.example.com/auto.mp4",
        )

        page = UploadsPage(
            self.upload_service,
            self.workflow_service,
            self.device_service,
            self.account_service,
        )
        page.auto_run_interval_combo.setCurrentIndex(page.auto_run_interval_combo.findData(5))
        page.auto_run_checkbox.setChecked(True)
        page._check_auto_run_draft_jobs()
        self._wait_for(lambda: page._run_thread is None)

        upload_job = self.upload_service.get_upload_job(upload_job_id)
        self.assertEqual(upload_job["status"], "success")
        self.assertEqual(self.fake_device.actions, [("shell", "echo Auto Upload")])
        self.assertIn("completed", page.status_label.text().lower())
        page.close()


if __name__ == "__main__":
    unittest.main()
