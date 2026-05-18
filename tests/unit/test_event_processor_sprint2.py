from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest

from processor.domain.models import AlertToSend, RawEvent
from processor.services.alert_evaluator import AlertEvaluator
from processor.services.event_processor import EventProcessor
from processor.services.semaphore_calculator import SemaphoreCalculator


class DummyConsumer:
    async def setup(self) -> None:
        return None

    async def read_events(self, count: int = 10):
        return []

    async def acknowledge(self, *msg_ids: str) -> None:
        return None


@dataclass
class FakeRepo:
    alerts_by_dedup: dict[str, dict[str, object]] = field(default_factory=dict)
    incidences: list[dict[str, object]] = field(default_factory=list)
    expectations: dict[str, dict[str, object]] = field(default_factory=dict)

    async def upsert_dag_run(self, event) -> None:
        return None

    async def insert_task_instance(self, event) -> None:
        return None

    async def insert_alert(self, alert: AlertToSend, suppressed: bool = False, suppression_reason: str | None = None) -> int:
        dedup_key = alert.dedup_key or ""
        self.alerts_by_dedup[dedup_key] = {
            "id": 1,
            "occurrence_count": 1,
            "alert": alert,
            "suppressed": suppressed,
            "suppression_reason": suppression_reason,
        }
        return 1

    async def upsert_alert_occurrence(self, alert: AlertToSend, suppressed: bool = False, suppression_reason: str | None = None):
        dedup_key = alert.dedup_key or ""
        if dedup_key in self.alerts_by_dedup and not suppressed:
            row = self.alerts_by_dedup[dedup_key]
            row["occurrence_count"] = int(row["occurrence_count"]) + 1
            return int(row["id"]), False

        alert_id = len(self.alerts_by_dedup) + 1
        self.alerts_by_dedup[dedup_key] = {
            "id": alert_id,
            "occurrence_count": 1,
            "alert": alert,
            "suppressed": suppressed,
            "suppression_reason": suppression_reason,
        }
        return alert_id, True

    async def get_avg_duration(self, dag_id: str, region: str):
        return 10.0

    async def get_dag_catalog_entry(self, dag_id: str, region: str):
        return {"criticality": "medium", "sla_seconds": 100, "expected_reports_count": 2}

    async def has_report_evidence(self, dag_id: str, run_id: str | None, grace_started_at):
        return False

    async def update_catalog_from_sync(self, event) -> None:
        return None

    async def mark_alert_notified(self, alert_id: int, channel: str) -> None:
        return None

    async def get_primary_report_link(self, dag_id: str, region: str):
        return {
            "id_report": 11,
            "id_file": 22,
            "report_priority_weight": 100,
            "source_priority_weight": 50,
            "frequency_priority_weight": 25,
        }

    async def get_root_cause_task_id(self, dag_id: str, region: str, run_id: str):
        return "extract"

    async def upsert_report_run_expectation(
        self,
        dag_id: str,
        region: str,
        run_id: str,
        expected_reports_count: int,
        generated_reports_count: int,
        evaluation_status: str,
        evaluated_at,
    ):
        key = f"{dag_id}:{region}:{run_id}"
        missing = max(expected_reports_count - generated_reports_count, 0)
        row = {
            "expected_reports_count": expected_reports_count,
            "generated_reports_count": generated_reports_count,
            "missing_reports_count": missing,
            "evaluation_status": evaluation_status,
            "evaluated_at": evaluated_at,
        }
        self.expectations[key] = row
        return row

    async def insert_report_incidence(
        self,
        region: str,
        dag_id: str,
        run_id: str | None,
        id_report: int,
        id_file: int | None,
        category: str,
        severity: str,
        priority_score: float,
        description: str,
    ):
        self.incidences.append(
            {
                "region": region,
                "dag_id": dag_id,
                "run_id": run_id,
                "id_report": id_report,
                "id_file": id_file,
                "category": category,
                "severity": severity,
                "priority_score": priority_score,
                "description": description,
            }
        )
        return len(self.incidences)

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_dedup_10_repeated_events_creates_one_alert_with_occurrence_count_10() -> None:
    repo = FakeRepo()
    queue: asyncio.Queue = asyncio.Queue()
    processor = EventProcessor(
        consumer=DummyConsumer(),
        repo=repo,
        calculator=SemaphoreCalculator(),
        evaluator=AlertEvaluator(),
        alert_queue=queue,
        alert_grace_seconds=120,
    )

    for idx in range(10):
        event = RawEvent(
            msg_id=f"msg-{idx}",
            event_type="task_state_change",
            dag_id="D_BO_0001",
            region="BO",
            timestamp=datetime.now(timezone.utc),
            run_id="run-1",
            run_state="failed",
            task_id="extract",
            task_state="failed",
            try_number=1,
            max_tries=3,
            reports_generated=2,
        )
        await processor._process_one(event)

    assert len(repo.alerts_by_dedup) == 1
    dedup_row = next(iter(repo.alerts_by_dedup.values()))
    assert dedup_row["occurrence_count"] == 10
    alert = dedup_row["alert"]
    assert alert.root_cause_task_id == "extract"
    assert alert.id_report == 11
    assert queue.qsize() == 1


@pytest.mark.asyncio
async def test_missing_reports_generates_report_expectation_and_incidence() -> None:
    repo = FakeRepo()
    queue: asyncio.Queue = asyncio.Queue()
    processor = EventProcessor(
        consumer=DummyConsumer(),
        repo=repo,
        calculator=SemaphoreCalculator(),
        evaluator=AlertEvaluator(),
        alert_queue=queue,
        alert_grace_seconds=120,
    )

    event = RawEvent(
        msg_id="msg-100",
        event_type="dag_run_state_change",
        dag_id="D_BO_0002",
        region="BO",
        timestamp=datetime.now(timezone.utc),
        run_id="run-2",
        run_state="success",
        duration=50.0,
        reports_generated=0,
    )

    await processor._process_one(event)

    expectation = repo.expectations.get("D_BO_0002:BO:run-2")
    assert expectation is not None
    assert expectation["missing_reports_count"] == 2

    assert len(repo.incidences) >= 1
    missing_incidence = next((x for x in repo.incidences if x["category"] == "report_not_generated"), None)
    assert missing_incidence is not None
    assert missing_incidence["id_report"] == 11
    assert missing_incidence["severity"] == "critical"


@pytest.mark.asyncio
async def test_upstream_failed_uses_derived_root_cause_task() -> None:
    repo = FakeRepo()
    queue: asyncio.Queue = asyncio.Queue()
    processor = EventProcessor(
        consumer=DummyConsumer(),
        repo=repo,
        calculator=SemaphoreCalculator(),
        evaluator=AlertEvaluator(),
        alert_queue=queue,
        alert_grace_seconds=120,
    )

    event = RawEvent(
        msg_id="msg-200",
        event_type="task_state_change",
        dag_id="D_BO_0003",
        region="BO",
        timestamp=datetime.now(timezone.utc),
        run_id="run-upstream",
        run_state="failed",
        task_id="transform",
        task_state="upstream_failed",
        upstream_task_id="extract",
        reports_generated=2,
    )

    await processor._process_one(event)

    dedup_row = next(iter(repo.alerts_by_dedup.values()))
    alert = dedup_row["alert"]
    assert alert.root_cause_task_id == "extract"
