from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest

from processor.services.alert_evaluator import AlertEvaluator
from processor.services.event_parser import parse_airflow_event
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
class E2ERepo:
    alerts_created: int = 0
    auto_resolved: list[str] = field(default_factory=list)

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
        self.auto_resolved.append(reason)
        return 0

    async def get_publication_window(self, dag_id: str, region: str):
        return None

    async def insert_alert(self, alert, suppressed: bool = False, suppression_reason: str | None = None):
        self.alerts_created += 1
        return self.alerts_created

    async def upsert_alert_occurrence(self, alert, suppressed: bool = False, suppression_reason: str | None = None):
        self.alerts_created += 1
        return self.alerts_created, True

    async def get_avg_duration(self, dag_id: str, region: str):
        return 50.0

    async def get_dag_catalog_entry(self, dag_id: str, region: str):
        return {"criticality": "high", "sla_seconds": 120, "expected_reports_count": 2}

    async def update_catalog_from_sync(self, event) -> None:
        return None

    async def mark_alert_notified(self, alert_id: int, channel: str) -> None:
        return None

    async def get_primary_report_link(self, dag_id: str, region: str):
        return {
            "id_report": 1001,
            "id_file": 2001,
            "report_priority_weight": 100,
            "source_priority_weight": 50,
            "frequency_priority_weight": 25,
        }

    async def get_root_cause_task_id(self, dag_id: str, region: str, run_id: str):
        return "extract"

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_e2e_extractor_like_payloads_flow() -> None:
    repo = E2ERepo()
    queue: asyncio.Queue = asyncio.Queue()
    processor = EventProcessor(
        consumer=DummyConsumer(),
        repo=repo,
        calculator=SemaphoreCalculator(),
        evaluator=AlertEvaluator(),
        alert_queue=queue,
    )

    payloads = [
        {
            "event_type": "dag_run_state_change",
            "dag_id": "D_BO_5000",
            "region": "BO",
            "timestamp": "2026-04-09T10:00:00Z",
            "run_id": "run_5000",
            "run_state": "success",
            "duration": "40",
        },
        {
            "event_type": "task_state_change",
            "dag_id": "D_BO_5000",
            "region": "BO",
            "timestamp": "2026-04-09T10:05:00Z",
            "run_id": "run_5001",
            "run_state": "failed",
            "task_id": "transform",
            "task_state": "failed",
            "upstream_task_id": "extract",
            "duration": "180",
            "detail": "URL dead timeout",
        },
    ]

    for idx, payload in enumerate(payloads, start=1):
        event = parse_airflow_event(f"{idx}-0", payload)
        await processor._process_one(event)

    # El run exitoso dispara auto-resolución de alertas abiertas del DAG.
    assert any("run_5000" in reason for reason in repo.auto_resolved)
    # La task fallida crítica genera una alerta encolada de inmediato (sin ventana de gracia).
    assert repo.alerts_created >= 1
    assert queue.qsize() >= 1
