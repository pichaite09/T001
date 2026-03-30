from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets

import automation_studio.ui.screen_viewer_window as screen_viewer_module
from automation_studio.ui.screen_viewer_window import (
    DeviceDetailViewerWindow,
    DeviceScreenTile,
    ScreenViewerWindow,
    WorkflowBatchRunner,
    _settings,
    find_scrcpy_executable,
)
from automation_studio.viewer_process import build_parser


class ScreenViewerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
        cls.app = QtWidgets.QApplication.instance() or QtWidgets.QApplication([])

    def setUp(self) -> None:
        settings = _settings()
        settings.remove("wall")
        settings.sync()

    def tearDown(self) -> None:
        settings = _settings()
        settings.remove("wall")
        settings.sync()

    def _workflow_service_stub(self, workflows: list[dict]) -> object:
        class _Service:
            def __init__(self, items: list[dict]) -> None:
                self._items = [dict(item) for item in items]
                self.stop_requests: list[tuple[list[int], str]] = []

            def list_workflows(self) -> list[dict]:
                return [dict(item) for item in self._items]

            def execute_workflow(self, workflow_id: int, device_id: int) -> dict:
                return {"success": True, "message": f"Workflow {workflow_id} on {device_id}"}

            def request_stop_for_devices(self, device_ids: list[int], reason: str = "Workflow stopped by user") -> int:
                normalized = [int(device_id) for device_id in device_ids]
                self.stop_requests.append((normalized, reason))
                return len(normalized)

        return _Service(workflows)

    def test_viewer_parser_accepts_db_path(self) -> None:
        args = build_parser().parse_args(["--db-path", "automation_studio.db", "--refresh-ms", "500"])
        self.assertEqual(args.db_path, "automation_studio.db")
        self.assertEqual(args.refresh_ms, 500)

    def test_screen_viewer_window_builds_without_autostart(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        self.assertEqual(window.view_preset_combo.count(), 4)
        self.assertEqual(window.refresh_rate_combo.count(), 6)
        self.assertEqual(window.scrcpy_preset_combo.count(), 4)
        self.assertEqual(window.scrcpy_max_size_combo.count(), 5)
        self.assertEqual(window.scrcpy_max_fps_combo.count(), 5)
        self.assertEqual(window.scrcpy_bit_rate_combo.count(), 6)
        self.assertEqual(window.resolution_combo.count(), 4)
        self.assertEqual(window.zoom_combo.count(), 7)
        self.assertEqual(window.workflow_mode_combo.count(), 4)
        self.assertEqual(window.pause_button.text(), "Pause")
        self.assertEqual(len(window._tiles), 2)
        self.assertEqual(window.selection_count_chip.text(), "0 selected")
        self.assertEqual(window.workflow_combo.count(), 1)
        self.assertFalse(window.stop_workflow_button.isEnabled())
        self.assertFalse(window.timer.isActive())
        window.close()

    def test_screen_viewer_populates_workflow_combo(self) -> None:
        workflows = [{"id": 7, "name": "Demo Workflow"}, {"id": 3, "name": "Alpha Workflow"}]
        service = self._workflow_service_stub(workflows)
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            workflows=workflows,
            workflow_service=service,
            refresh_interval_ms=500,
            autostart=False,
        )
        self.assertEqual(window.workflow_combo.count(), 3)
        self.assertEqual(window.workflow_combo.itemText(1), "Alpha Workflow")
        window.close()

    def test_tile_can_save_current_frame(self) -> None:
        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        pixmap = QtGui.QPixmap(40, 80)
        pixmap.fill(QtGui.QColor("#00aa88"))
        tile._last_pixmap = pixmap
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = tile.save_current_frame(Path(temp_dir))
            self.assertTrue(output_path.exists())
            self.assertEqual(output_path.suffix.lower(), ".png")
        tile.deleteLater()

    def test_tile_resolution_scale_can_be_updated(self) -> None:
        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        tile.set_resolution_scale(0.5)
        self.assertAlmostEqual(tile._resolution_scale, 0.5)
        tile.deleteLater()

    def test_tile_workflow_state_updates_label(self) -> None:
        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        tile.set_workflow_state("running", "Workflow is running")
        self.assertEqual(tile.workflow_state_label.text(), "Running")
        self.assertIn("Workflow is running", tile.workflow_state_label.toolTip())
        tile.set_workflow_state("stopped", "Workflow stopped")
        self.assertEqual(tile.workflow_state_label.text(), "Stopped")
        tile.deleteLater()

    def test_tile_size_label_uses_display_size_after_resolution_scaling(self) -> None:
        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        pixmap = QtGui.QPixmap(40, 80)
        pixmap.fill(QtGui.QColor("#3366aa"))
        image = pixmap.toImage()
        buffer = QtCore.QBuffer()
        buffer.open(QtCore.QIODevice.OpenModeFlag.WriteOnly)
        image.save(buffer, "PNG")
        tile.apply_frame_payload(
            {
                "ok": True,
                "data": bytes(buffer.data()),
                "source_size": QtCore.QSize(1080, 2340),
                "display_size": QtCore.QSize(540, 1170),
                "resolution_scale": 0.5,
            }
        )
        self.assertEqual(tile.size_label.text(), "540 x 1170")
        self.assertIn("1080 x 2340", tile.size_label.toolTip())
        tile.deleteLater()

    def test_tile_double_click_requests_realtime_viewer(self) -> None:
        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        captured: list[str] = []
        tile.realtime_requested.connect(lambda selected_tile: captured.append(selected_tile.serial))
        tile.image_label.double_clicked.emit()
        self.assertEqual(captured, ["SERIAL1"])
        tile.deleteLater()

    def test_tile_skips_capture_when_adb_reports_device_offline(self) -> None:
        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        tile._host_device_is_ready = lambda: (False, "offline")  # type: ignore[method-assign]
        tile._ensure_connected = lambda: self.fail("capture should not try to connect when adb says offline")  # type: ignore[method-assign]
        payload = tile.capture_frame_payload()
        self.assertFalse(payload["ok"])
        self.assertIn("offline", payload["error"])
        tile.deleteLater()

    def test_tile_uses_cooldown_after_failed_capture(self) -> None:
        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        calls = {"count": 0}

        def _offline_check():
            calls["count"] += 1
            return False, "offline"

        tile._host_device_is_ready = _offline_check  # type: ignore[method-assign]
        first = tile.capture_frame_payload()
        second = tile.capture_frame_payload()
        self.assertFalse(first["ok"])
        self.assertFalse(second["ok"])
        self.assertTrue(second.get("cooldown"))
        self.assertEqual(calls["count"], 1)
        tile.deleteLater()

    def test_open_detail_viewer_reuses_window(self) -> None:
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            refresh_interval_ms=500,
            autostart=False,
        )
        tile = window._tiles[0]
        pixmap = QtGui.QPixmap(40, 80)
        pixmap.fill(QtGui.QColor("#2255cc"))
        tile._last_pixmap = pixmap
        window._open_detail_viewer(tile)
        window._open_detail_viewer(tile)
        self.assertEqual(len(window._detail_windows), 1)
        self.assertIn("SERIAL1", window._detail_windows)
        window.close()

    def test_detail_viewer_builds_and_saves_frame(self) -> None:
        pixmap = QtGui.QPixmap(32, 64)
        pixmap.fill(QtGui.QColor("#cc5522"))
        viewer = DeviceDetailViewerWindow(device_name="Phone A", serial="SERIAL1", initial_pixmap=pixmap)
        self.assertEqual(viewer.zoom_combo.count(), 8)
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = viewer.save_current_frame(Path(temp_dir))
            self.assertTrue(output_path.exists())
        viewer.close()

    def test_detail_viewer_maps_points_to_device_coords(self) -> None:
        pixmap = QtGui.QPixmap(100, 200)
        pixmap.fill(QtGui.QColor("#44aaee"))
        viewer = DeviceDetailViewerWindow(device_name="Phone A", serial="SERIAL1", initial_pixmap=pixmap)
        viewer.fit_button.setChecked(False)
        viewer.image_label.resize(100, 200)
        viewer._render_pixmap()
        mapped = viewer._map_label_point_to_device_coords(QtCore.QPoint(50, 100))
        self.assertEqual(mapped, (50, 100))
        viewer.close()

    def test_detail_viewer_tap_mode_clicks_device(self) -> None:
        pixmap = QtGui.QPixmap(100, 200)
        pixmap.fill(QtGui.QColor("#778899"))
        viewer = DeviceDetailViewerWindow(device_name="Phone A", serial="SERIAL1", initial_pixmap=pixmap)
        viewer.fit_button.setChecked(False)
        viewer.image_label.resize(100, 200)
        viewer._render_pixmap()
        captured: list[tuple[int, int]] = []
        viewer._tap_device = lambda x, y: captured.append((x, y))  # type: ignore[method-assign]
        viewer.tap_mode_button.setChecked(True)
        viewer._handle_image_clicked(QtCore.QPoint(25, 75))
        self.assertEqual(captured, [(25, 75)])
        viewer.close()

    def test_find_scrcpy_executable_can_detect_local_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            fake_binary = Path(temp_dir) / "scrcpy.exe"
            fake_binary.write_text("fake")
            previous = os.environ.get("SCRCPY_PATH")
            os.environ["SCRCPY_PATH"] = str(fake_binary)
            try:
                resolved = find_scrcpy_executable([Path(temp_dir)])
                self.assertEqual(resolved, str(fake_binary))
            finally:
                if previous is None:
                    os.environ.pop("SCRCPY_PATH", None)
                else:
                    os.environ["SCRCPY_PATH"] = previous

    def test_detail_viewer_can_start_realtime_backend_when_program_is_available(self) -> None:
        viewer = DeviceDetailViewerWindow(device_name="Phone A", serial="SERIAL1", scrcpy_program="scrcpy")
        captured: list[tuple[str, list[str]]] = []
        viewer._start_scrcpy_process = lambda program, arguments: captured.append((program, arguments)) or True  # type: ignore[method-assign]
        self.assertTrue(viewer.ensure_realtime_backend())
        self.assertEqual(captured, [("scrcpy", ["-s", "SERIAL1"])])
        viewer.close()

    def test_screen_viewer_window_can_store_scrcpy_program(self) -> None:
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            refresh_interval_ms=500,
            autostart=False,
        )
        window._set_scrcpy_program("C:/tools/scrcpy.exe", persist=False)
        self.assertEqual(window._scrcpy_program, "C:/tools/scrcpy.exe")
        self.assertEqual(window.locate_scrcpy_button.text(), "Change scrcpy")
        window.close()

    def test_screen_viewer_can_apply_quality_preset(self) -> None:
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            refresh_interval_ms=1000,
            autostart=False,
        )
        window.view_preset_combo.setCurrentIndex(window.view_preset_combo.findData("quality"))
        self.assertEqual(window.refresh_rate_combo.currentData(), 500)
        self.assertEqual(window.resolution_combo.currentData(), 1.0)
        window.close()

    def test_screen_viewer_builds_scrcpy_arguments_with_max_size(self) -> None:
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            refresh_interval_ms=1000,
            autostart=False,
        )
        window.scrcpy_max_size_combo.setCurrentIndex(window.scrcpy_max_size_combo.findData(800))
        self.assertEqual(window._build_scrcpy_arguments("SERIAL1"), ["-s", "SERIAL1", "--max-size", "800"])
        window.close()

    def test_screen_viewer_builds_scrcpy_arguments_with_fps_and_bitrate(self) -> None:
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            refresh_interval_ms=1000,
            autostart=False,
        )
        window.scrcpy_max_size_combo.setCurrentIndex(window.scrcpy_max_size_combo.findData(1024))
        window.scrcpy_max_fps_combo.setCurrentIndex(window.scrcpy_max_fps_combo.findData(30))
        window.scrcpy_bit_rate_combo.setCurrentIndex(window.scrcpy_bit_rate_combo.findData("8M"))
        self.assertEqual(
            window._build_scrcpy_arguments("SERIAL1"),
            ["-s", "SERIAL1", "--max-size", "1024", "--max-fps", "30", "--video-bit-rate", "8M"],
        )
        window.close()

    def test_screen_viewer_can_apply_scrcpy_high_quality_preset(self) -> None:
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            refresh_interval_ms=1000,
            autostart=False,
        )
        window.scrcpy_preset_combo.setCurrentIndex(window.scrcpy_preset_combo.findData("high_quality"))
        self.assertEqual(window.scrcpy_max_size_combo.currentData(), 1280)
        self.assertEqual(window.scrcpy_max_fps_combo.currentData(), 60)
        self.assertEqual(window.scrcpy_bit_rate_combo.currentData(), "16M")
        window.close()

    def test_screen_viewer_restores_saved_wall_settings(self) -> None:
        settings = _settings()
        settings.setValue("wall/view_preset", "custom")
        settings.setValue("wall/refresh_interval_ms", 5000)
        settings.setValue("wall/resolution_scale", 0.5)
        settings.setValue("wall/zoom_factor", 1.25)
        settings.setValue("wall/scrcpy_preset", "balanced")
        settings.setValue("wall/scrcpy_max_size", 1024)
        settings.setValue("wall/scrcpy_max_fps", 30)
        settings.setValue("wall/scrcpy_bit_rate", "8M")
        settings.setValue("wall/workflow_parallelism", 4)
        settings.sync()
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            refresh_interval_ms=1000,
            autostart=False,
        )
        self.assertEqual(window.view_preset_combo.currentData(), "custom")
        self.assertEqual(window.refresh_rate_combo.currentData(), 5000)
        self.assertEqual(window.resolution_combo.currentData(), 0.5)
        self.assertEqual(window.zoom_combo.currentData(), 1.25)
        self.assertEqual(window.scrcpy_preset_combo.currentData(), "balanced")
        self.assertEqual(window.scrcpy_max_size_combo.currentData(), 1024)
        self.assertEqual(window.scrcpy_max_fps_combo.currentData(), 30)
        self.assertEqual(window.scrcpy_bit_rate_combo.currentData(), "8M")
        self.assertEqual(window.workflow_mode_combo.currentData(), 4)
        window.close()

    def test_run_selected_workflow_uses_only_selected_devices(self) -> None:
        workflows = [{"id": 11, "name": "Demo Workflow"}]
        service = self._workflow_service_stub(workflows)
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            workflows=workflows,
            workflow_service=service,
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[tuple[int, list[int], str]] = []
        window._start_workflow_runner = lambda workflow_id, device_records, label: captured.append(  # type: ignore[method-assign]
            (workflow_id, [int(item["id"]) for item in device_records], label)
        )
        window.workflow_combo.setCurrentIndex(window.workflow_combo.findData(11))
        window._tiles[0].set_selected(True)
        window.run_selected_workflow()
        self.assertEqual(captured, [(11, [1], "Status: running workflow on 1 selected devices (Sequential)")])
        window.close()

    def test_run_all_workflow_uses_all_devices(self) -> None:
        workflows = [{"id": 11, "name": "Demo Workflow"}]
        service = self._workflow_service_stub(workflows)
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            workflows=workflows,
            workflow_service=service,
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[tuple[int, list[int], str]] = []
        window._start_workflow_runner = lambda workflow_id, device_records, label: captured.append(  # type: ignore[method-assign]
            (workflow_id, [int(item["id"]) for item in device_records], label)
        )
        window.workflow_combo.setCurrentIndex(window.workflow_combo.findData(11))
        window.run_all_workflow()
        self.assertEqual(captured, [(11, [1, 2], "Status: running workflow on all 2 devices (Sequential)")])
        window.close()

    def test_parallel_all_mode_uses_selected_device_count(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
                {"id": 3, "name": "Phone C", "serial": "SERIAL3"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        window.workflow_mode_combo.setCurrentIndex(window.workflow_mode_combo.findData(-1))
        self.assertEqual(window._selected_workflow_parallelism(), 3)
        window.close()

    def test_start_workflow_runner_passes_parallelism(self) -> None:
        workflows = [{"id": 11, "name": "Demo Workflow"}]
        service = self._workflow_service_stub(workflows)
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            workflows=workflows,
            workflow_service=service,
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: dict[str, object] = {}

        class _Runner(QtCore.QObject):
            progress = QtCore.Signal(object)
            result_ready = QtCore.Signal(dict)
            finished = QtCore.Signal()

            def __init__(self, workflow_service, workflow_id, device_records, max_parallel=1) -> None:
                super().__init__()
                captured["workflow_id"] = workflow_id
                captured["device_ids"] = [int(item["id"]) for item in device_records]
                captured["max_parallel"] = max_parallel

            def start(self) -> None:
                captured["started"] = True

            def request_stop(self) -> None:
                captured["stopped"] = True

        original_runner = screen_viewer_module.WorkflowBatchRunner
        screen_viewer_module.WorkflowBatchRunner = _Runner
        try:
            window.workflow_combo.setCurrentIndex(window.workflow_combo.findData(11))
            window.workflow_mode_combo.setCurrentIndex(window.workflow_mode_combo.findData(4))
            window.run_all_workflow()
        finally:
            screen_viewer_module.WorkflowBatchRunner = original_runner
        self.assertEqual(captured["workflow_id"], 11)
        self.assertEqual(captured["device_ids"], [1, 2])
        self.assertEqual(captured["max_parallel"], 2)
        self.assertTrue(captured["started"])
        window.close()

    def test_workflow_progress_updates_tile_state(self) -> None:
        workflows = [{"id": 11, "name": "Demo Workflow"}]
        service = self._workflow_service_stub(workflows)
        window = ScreenViewerWindow(
            devices=[{"id": 1, "name": "Phone A", "serial": "SERIAL1"}],
            workflows=workflows,
            workflow_service=service,
            refresh_interval_ms=500,
            autostart=False,
        )
        tile = window._tiles[0]
        window._on_workflow_progress({"phase": "started", "current": 1, "total": 1, "device_id": 1, "device_name": "Phone A"})
        self.assertEqual(tile.workflow_state_label.text(), "Running")
        window._on_workflow_result(
            {
                "success_count": 1,
                "total": 1,
                "results": [
                    {
                        "device_id": 1,
                        "result": {"success": True, "message": "ok"},
                    }
                ],
            }
        )
        self.assertEqual(tile.workflow_state_label.text(), "Done")
        self.assertIn("ok", tile.workflow_state_label.toolTip())
        window.close()

    def test_stop_workflow_marks_queued_tiles_and_requests_runner_stop(self) -> None:
        workflows = [{"id": 11, "name": "Demo Workflow"}]
        service = self._workflow_service_stub(workflows)
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            workflows=workflows,
            workflow_service=service,
            refresh_interval_ms=500,
            autostart=False,
        )

        class _Runner:
            def __init__(self) -> None:
                self.stopped = False

            def request_stop(self) -> None:
                self.stopped = True

        runner = _Runner()
        window._workflow_runner = runner  # type: ignore[assignment]
        window._workflow_target_device_ids = {1, 2}
        window._workflow_running_device_ids = {1}
        window._tiles[0].set_workflow_state("running", "Phone A")
        window._tiles[1].set_workflow_state("queued", "Demo Workflow")
        window.stop_workflow_run()
        self.assertTrue(runner.stopped)
        self.assertEqual(service.stop_requests, [([1], "Stopped from Screen Wall")])
        self.assertEqual(window._tiles[0].workflow_state_label.text(), "Running")
        self.assertEqual(window._tiles[1].workflow_state_label.text(), "Stopped")
        self.assertIn("stopping workflow", window.status_label.text().lower())
        window.close()

    def test_workflow_batch_runner_reports_stopped_devices(self) -> None:
        service = self._workflow_service_stub([{"id": 11, "name": "Demo Workflow"}])
        runner = WorkflowBatchRunner(
            service,
            11,
            [
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
        )
        captured: list[dict] = []
        runner.result_ready.connect(captured.append)
        runner.request_stop()
        runner.run()
        self.assertEqual(len(captured), 1)
        self.assertTrue(captured[0]["stopped"])
        self.assertEqual(captured[0]["stopped_count"], 2)
        self.assertEqual(captured[0]["results"][0]["result"]["message"], "Stopped before execution")

    def test_screen_viewer_can_select_all_tiles(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        window._select_all_tiles()
        self.assertEqual(window.selection_count_chip.text(), "2 selected")
        self.assertTrue(all(tile.is_selected() for tile in window._tiles))
        window.close()

    def test_open_realtime_selected_viewers_starts_each_selected_tile(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[str] = []
        window._start_detached_scrcpy = lambda serial: captured.append(serial) or True  # type: ignore[method-assign]
        window._tiles[0].set_selected(True)
        window._tiles[1].set_selected(True)
        window.open_realtime_selected_viewers()
        self.assertEqual(captured, ["SERIAL1", "SERIAL2"])
        window.close()

    def test_set_min_brightness_selected_only_targets_selected_tiles(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[str] = []
        window._tiles[0].set_min_brightness = lambda: captured.append("SERIAL1") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].set_min_brightness = lambda: captured.append("SERIAL2") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[0].set_selected(True)
        window.set_min_brightness_selected_tiles()
        self.assertEqual(captured, ["SERIAL1"])
        self.assertIn("minimum", window.status_label.text().lower())
        window.close()

    def test_set_max_brightness_selected_only_targets_selected_tiles(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[str] = []
        window._tiles[0].set_max_brightness = lambda: captured.append("SERIAL1") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].set_max_brightness = lambda: captured.append("SERIAL2") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].set_selected(True)
        window.set_max_brightness_selected_tiles()
        self.assertEqual(captured, ["SERIAL2"])
        self.assertIn("maximum", window.status_label.text().lower())
        window.close()

    def test_set_quarter_brightness_selected_only_targets_selected_tiles(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[str] = []
        window._tiles[0].set_quarter_brightness = lambda: captured.append("SERIAL1") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].set_quarter_brightness = lambda: captured.append("SERIAL2") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[0].set_selected(True)
        window.set_quarter_brightness_selected_tiles()
        self.assertEqual(captured, ["SERIAL1"])
        self.assertIn("25%", window.status_label.text().lower())
        window.close()

    def test_tile_quarter_brightness_uses_dimmer_mapping(self) -> None:
        class _FakeDevice:
            def __init__(self) -> None:
                self.commands: list[str] = []

            def shell(self, command: str):
                self.commands.append(command)
                return ("ok", 0)

        tile = DeviceScreenTile({"id": 1, "name": "Phone A", "serial": "SERIAL1"})
        fake_device = _FakeDevice()
        tile._ensure_connected = lambda: fake_device  # type: ignore[method-assign]
        success, message = tile.set_quarter_brightness()
        self.assertTrue(success)
        self.assertIn("25%", message)
        self.assertIn("settings put system screen_brightness 16", fake_device.commands)
        self.assertIn("cmd display brightness 0.06", fake_device.commands)
        tile.deleteLater()

    def test_press_home_selected_only_targets_selected_tiles(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[str] = []
        window._tiles[0].press_home = lambda: captured.append("SERIAL1") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].press_home = lambda: captured.append("SERIAL2") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[0].set_selected(True)
        window.press_home_selected_tiles()
        self.assertEqual(captured, ["SERIAL1"])
        self.assertIn("home", window.status_label.text().lower())
        window.close()

    def test_press_back_selected_only_targets_selected_tiles(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[str] = []
        window._tiles[0].press_back = lambda: captured.append("SERIAL1") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].press_back = lambda: captured.append("SERIAL2") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].set_selected(True)
        window.press_back_selected_tiles()
        self.assertEqual(captured, ["SERIAL2"])
        self.assertIn("back", window.status_label.text().lower())
        window.close()

    def test_press_recent_apps_selected_only_targets_selected_tiles(self) -> None:
        window = ScreenViewerWindow(
            devices=[
                {"id": 1, "name": "Phone A", "serial": "SERIAL1"},
                {"id": 2, "name": "Phone B", "serial": "SERIAL2"},
            ],
            refresh_interval_ms=500,
            autostart=False,
        )
        captured: list[str] = []
        window._tiles[0].press_recent_apps = lambda: captured.append("SERIAL1") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[1].press_recent_apps = lambda: captured.append("SERIAL2") or (True, "ok")  # type: ignore[method-assign]
        window._tiles[0].set_selected(True)
        window.press_recent_apps_selected_tiles()
        self.assertEqual(captured, ["SERIAL1"])
        self.assertIn("recent apps", window.status_label.text().lower())
        window.close()


if __name__ == "__main__":
    unittest.main()
