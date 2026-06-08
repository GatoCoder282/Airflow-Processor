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
    """Fake del IMonitoringRepository (solo lo que usa el EventProcessor)."""

    criticality: str = "medium"
    alerts_by_dedup: dict[str, dict[str, object]] = field(default_factory=dict)
    auto_resolved: list[tuple[str, str, str]] = field(default_factory=list)

    # ── Persistencia (no-op) ──────────────────────────────────────────────────
    async def upsert_dag_run(self, event) -> None:
        return None

    async def insert_task_instance(self, event) -> None:
        return None

    async def ensure_run_exists(self, dag_id: str, run_id: str, region: str) -> None:
        return None

    async def update_run_active_task(self, dag_id, run_id, region, task_id, task_state) -> None:
        return None

    async def update_run_task_counts(self, dag_id: str, run_id: str, region: str) -> None:
        return None

    async def upsert_unknown_dag(self, dag_id: str, region: str) -> None:
        return None

    async def auto_resolve_dag_alerts(self, dag_id: str, region: str, reason: str) -> int:
        self.auto_resolved.append((dag_id, region, reason))
        return 0

    async def get_publication_window(self, dag_id: str, region: str):
        return None

    # ── Alertas ───────────────────────────────────────────────────────────────
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

    # ── Enriquecimiento ───────────────────────────────────────────────────────
    async def get_avg_duration(self, dag_id: str, region: str):
        return 10.0

    async def get_dag_catalog_entry(self, dag_id: str, region: str):
        return {"criticality": self.criticality, "sla_seconds": 100, "expected_reports_count": 2}

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

    async def close(self) -> None:
        return None


def _make_processor(repo: FakeRepo) -> tuple[EventProcessor, asyncio.Queue]:
    queue: asyncio.Queue = asyncio.Queue()
    processor = EventProcessor(
        consumer=DummyConsumer(),
        repo=repo,
        calculator=SemaphoreCalculator(),
        evaluator=AlertEvaluator(),
        alert_queue=queue,
    )
    return processor, queue


@pytest.mark.asyncio
async def test_dedup_10_repeated_events_creates_one_alert_with_occurrence_count_10() -> None:
    repo = FakeRepo()
    processor, queue = _make_processor(repo)

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
    # Sin ventana de gracia: la alerta se encoló de inmediato en el primer evento.
    assert queue.qsize() == 1


@pytest.mark.asyncio
async def test_critical_alert_is_dispatched_immediately_without_grace_window() -> None:
    repo = FakeRepo(criticality="high")  # high → TASK_FAILED crítico
    processor, queue = _make_processor(repo)

    event = RawEvent(
        msg_id="msg-crit",
        event_type="task_state_change",
        dag_id="D_BO_0009",
        region="BO",
        timestamp=datetime.now(timezone.utc),
        run_id="run-crit",
        run_state="failed",
        task_id="extract",
        task_state="failed",
    )
    await processor._process_one(event)

    assert len(repo.alerts_by_dedup) == 1
    dedup_row = next(iter(repo.alerts_by_dedup.values()))
    assert dedup_row["suppressed"] is False
    assert dedup_row["alert"].severity.value == "critical"
    # La crítica se encola al instante (antes esperaba 120s en _pending_critical).
    assert queue.qsize() == 1


@pytest.mark.asyncio
async def test_successful_run_triggers_auto_resolve() -> None:
    repo = FakeRepo()
    processor, _ = _make_processor(repo)

    event = RawEvent(
        msg_id="msg-ok",
        event_type="dag_run_state_change",
        dag_id="D_BO_0002",
        region="BO",
        timestamp=datetime.now(timezone.utc),
        run_id="run-ok",
        run_state="success",
        duration=50.0,
    )
    await processor._process_one(event)

    assert repo.auto_resolved == [("D_BO_0002", "BO", "next_run_succeeded:run-ok")]


@pytest.mark.asyncio
async def test_upstream_failed_derives_root_cause_task() -> None:
    repo = FakeRepo()
    processor, _ = _make_processor(repo)

    event = RawEvent(
        msg_id="msg-200",
        event_type="task_state_change",
        dag_id="D_BO_0003",
        region="BO",
        timestamp=datetime.now(timezone.utc),
        run_id="run-upstream",
        task_id="transform",
        task_state="upstream_failed",
        upstream_task_id="extract",
    )

    root = await processor._derive_root_cause_task_id(event)
    assert root == "extract"
