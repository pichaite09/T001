from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta

from automation_studio.database import DatabaseManager
from automation_studio.repositories import (
    AccountRepository,
    DeviceRepository,
    LogRepository,
    ScheduleGroupRepository,
    ScheduleRepository,
    ScheduleRunRepository,
    TelemetryRepository,
    WatcherRepository,
    WatcherTelemetryRepository,
    WorkflowRepository,
)
from automation_studio.services import (
    AccountService,
    DeviceService,
    LogService,
    SchedulerService,
    TelemetryService,
    WatcherService,
    WatcherTelemetryService,
    WorkflowService,
)


class FakeDevice:
    def __init__(self) -> None:
        self.actions: list[tuple] = []

    def click(self, x: int, y: int) -> None:
        self.actions.append(("click", x, y))

    def window_size(self):
        return (1080, 2400)


class FakeDeviceService(DeviceService):
    def __init__(self, device_repository: DeviceRepository, device: FakeDevice) -> None:
        super().__init__(device_repository)
        self.fake_device = device

    def connect_device(self, serial: str):
        return self.fake_device


class FlakyWorkflowService:
    def __init__(self, results: list[dict[str, object]]) -> None:
        self.results = list(results)
        self.calls = 0

    def execute_workflow(self, workflow_id: int, device_id: int, device_platform_id=None, account_id=None, use_current_account: bool = False):
        self.calls += 1
        if self.results:
            return dict(self.results.pop(0))
        return {"success": True, "message": "ok"}


class SchedulerServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db = DatabaseManager(self.db_path)
        self.db.init_schema()

        self.workflow_repository = WorkflowRepository(self.db)
        self.device_repository = DeviceRepository(self.db)
        self.account_repository = AccountRepository(self.db)
        self.schedule_group_repository = ScheduleGroupRepository(self.db)
        self.schedule_repository = ScheduleRepository(self.db)
        self.schedule_run_repository = ScheduleRunRepository(self.db)
        self.log_repository = LogRepository(self.db)
        self.telemetry_repository = TelemetryRepository(self.db)
        self.watcher_repository = WatcherRepository(self.db)
        self.watcher_telemetry_repository = WatcherTelemetryRepository(self.db)

        self.log_service = LogService(self.log_repository)
        self.telemetry_service = TelemetryService(self.telemetry_repository)
        self.watcher_telemetry_service = WatcherTelemetryService(self.watcher_telemetry_repository)
        self.fake_device = FakeDevice()
        self.device_service = FakeDeviceService(self.device_repository, self.fake_device)
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
        self.scheduler_service = SchedulerService(
            self.schedule_repository,
            self.schedule_run_repository,
            self.workflow_repository,
            self.device_repository,
            self.workflow_service,
            self.log_service,
            self.account_service,
            schedule_group_repository=self.schedule_group_repository,
        )

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_manual_schedule_run_records_history_without_advancing_next_run(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Scheduled Workflow", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Tap Task",
            "tap",
            json.dumps({"x": 15, "y": 25}),
            True,
        )
        schedule_id = self.scheduler_service.save_schedule(
            None,
            "Interval Schedule",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "interval",
            {"every_minutes": 30},
            True,
        )

        schedule_before = self.scheduler_service.get_schedule(schedule_id)
        result = self.scheduler_service.execute_schedule(schedule_id, trigger_source="manual", advance_schedule=False)
        schedule_after = self.scheduler_service.get_schedule(schedule_id)
        runs = self.scheduler_service.list_runs(schedule_id)

        self.assertTrue(result["success"])
        self.assertEqual(self.fake_device.actions, [("click", 15, 25)])
        self.assertEqual(schedule_before["next_run_at"], schedule_after["next_run_at"])
        self.assertEqual(schedule_after["last_status"], "success")
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["trigger_source"], "manual")
        self.assertEqual(runs[0]["status"], "success")

    def test_due_once_schedule_runs_and_disables_itself(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "One Shot Workflow", "", True)
        self.workflow_service.save_step(
            None,
            workflow_id,
            1,
            "Tap Task",
            "tap",
            json.dumps({"x": 50, "y": 60}),
            True,
        )
        past_time = (datetime.now().astimezone() - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
        schedule_id = self.scheduler_service.save_schedule(
            None,
            "Run Once",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "once",
            {"run_at": past_time},
            True,
        )

        due_ids = [int(schedule["id"]) for schedule in self.scheduler_service.list_due_schedules()]
        result = self.scheduler_service.execute_schedule(schedule_id, trigger_source="timer", advance_schedule=True)
        schedule_after = self.scheduler_service.get_schedule(schedule_id)

        self.assertIn(schedule_id, due_ids)
        self.assertTrue(result["success"])
        self.assertEqual(self.fake_device.actions, [("click", 50, 60)])
        self.assertEqual(int(schedule_after["is_enabled"]), 0)
        self.assertIsNone(schedule_after["next_run_at"])
        self.assertEqual(schedule_after["last_status"], "success")

    def test_weekly_schedule_saves_and_describes_selected_days(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Weekly Workflow", "", True)
        schedule_id = self.scheduler_service.save_schedule(
            None,
            "Weekly Run",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "weekly",
            {"time": "09:30", "weekdays": [0, 2, 4], "jitter_seconds": 15},
            True,
        )

        schedule = self.scheduler_service.get_schedule(schedule_id)

        self.assertEqual(schedule["schedule_config"]["weekdays"], [0, 2, 4])
        self.assertIn("Weekly Mon, Wed, Fri at 09:30", schedule["schedule_summary"])
        self.assertIn("+15s jitter", schedule["schedule_summary"])

    def test_startup_missed_run_policy_skip_records_non_run(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Interval Workflow", "", True)
        schedule_id = self.scheduler_service.save_schedule(
            None,
            "Skip Missed",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "interval",
            {"every_minutes": 30, "missed_run_policy": "skip"},
            True,
        )
        self.schedule_repository.update_schedule_state(
            schedule_id,
            next_run_at=(datetime.now().astimezone() - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S"),
            last_status="idle",
            is_enabled=True,
        )

        decision = self.scheduler_service.resolve_run_request(
            schedule_id,
            trigger_source="timer",
            advance_schedule=True,
            startup_recovery=True,
            is_running=False,
        )
        schedule_after = self.scheduler_service.get_schedule(schedule_id)
        runs = self.scheduler_service.list_runs(schedule_id)

        self.assertEqual(decision["action"], "skip")
        self.assertEqual(decision["reason"], "missed_skipped")
        self.assertEqual(schedule_after["last_status"], "missed_skipped")
        self.assertTrue(runs)
        self.assertEqual(runs[0]["status"], "missed_skipped")

    def test_overlap_policy_queue_returns_queue_action(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Queue Workflow", "", True)
        schedule_id = self.scheduler_service.save_schedule(
            None,
            "Queue If Busy",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "interval",
            {"every_minutes": 15, "overlap_policy": "queue_next"},
            True,
        )

        decision = self.scheduler_service.resolve_run_request(
            schedule_id,
            trigger_source="timer",
            advance_schedule=True,
            startup_recovery=False,
            is_running=True,
        )

        self.assertEqual(decision["action"], "queue")

    def test_retry_on_failure_attempts_workflow_again(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Retry Workflow", "", True)
        schedule_id = self.scheduler_service.save_schedule(
            None,
            "Retry Schedule",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "interval",
            {"every_minutes": 5, "retry_on_failure": 1, "retry_delay_seconds": 0},
            True,
        )
        flaky = FlakyWorkflowService(
            [
                {"success": False, "message": "first fail"},
                {"success": True, "message": "second ok"},
            ]
        )
        self.scheduler_service.workflow_service = flaky

        result = self.scheduler_service.execute_schedule(schedule_id, trigger_source="manual", advance_schedule=False)

        self.assertTrue(result["success"])
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(flaky.calls, 2)

    def test_due_schedules_sort_by_priority_when_run_time_matches(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Priority Workflow", "", True)
        past_time = (datetime.now().astimezone() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")

        low_priority_id = self.scheduler_service.save_schedule(
            None,
            "Low Priority",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "once",
            {"run_at": past_time},
            True,
            priority=200,
        )
        high_priority_id = self.scheduler_service.save_schedule(
            None,
            "High Priority",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "once",
            {"run_at": past_time},
            True,
            priority=10,
        )

        due_ids = [int(schedule["id"]) for schedule in self.scheduler_service.list_due_schedules()]

        self.assertEqual(due_ids[:2], [high_priority_id, low_priority_id])

    def test_dashboard_snapshot_reports_groups_running_and_queued(self) -> None:
        device_id = self.device_repository.upsert_device(None, "Phone", "SERIAL1", "")
        workflow_id = self.workflow_service.save_workflow(None, "Dashboard Workflow", "", True)
        group_id = self.scheduler_service.save_group(None, "Morning Batch", "Morning jobs", True)

        running_id = self.scheduler_service.save_schedule(
            None,
            "Running Schedule",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "interval",
            {"every_minutes": 15},
            True,
            schedule_group_id=group_id,
            priority=20,
        )
        queued_id = self.scheduler_service.save_schedule(
            None,
            "Queued Schedule",
            workflow_id,
            device_id,
            None,
            None,
            False,
            "interval",
            {"every_minutes": 30},
            True,
            schedule_group_id=group_id,
            priority=30,
        )

        snapshot = self.scheduler_service.dashboard_snapshot(
            running_schedule_ids={running_id},
            queued_schedule_ids={queued_id},
        )

        self.assertEqual(snapshot["counts"]["groups"], 1)
        self.assertEqual(snapshot["counts"]["running"], 1)
        self.assertEqual(snapshot["counts"]["queued"], 1)
        self.assertTrue(snapshot["groups"])
        self.assertEqual(snapshot["groups"][0]["running_count"], 1)
        self.assertEqual(snapshot["groups"][0]["queued_count"], 1)


if __name__ == "__main__":
    unittest.main()
