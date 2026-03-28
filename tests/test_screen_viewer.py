from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets

from automation_studio.ui.screen_viewer_window import (
    DeviceDetailViewerWindow,
    DeviceScreenTile,
    ScreenViewerWindow,
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
        self.assertEqual(window.pause_button.text(), "Pause")
        self.assertEqual(len(window._tiles), 2)
        self.assertEqual(window.selection_count_chip.text(), "0 selected")
        self.assertFalse(window.timer.isActive())
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
        window.close()

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


if __name__ == "__main__":
    unittest.main()
