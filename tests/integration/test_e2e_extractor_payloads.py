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
    expectations: list[dict[str, object]] = field(default_factory=list)
    incidences: list[dict[str, object]] = field(default_factory=list)
    alerts_created: int = 0

    async def upsert_dag_run(self, event) -> None:
        return None

    async def insert_task_instance(self, event) -> None:
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

    async def has_report_evidence(self, dag_id: str, run_id: str | None, grace_started_at):
        return False

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
        row = {
            "dag_id": dag_id,
            "region": region,
            "run_id": run_id,
            "expected_reports_count": expected_reports_count,
            "generated_reports_count": generated_reports_count,
            "missing_reports_count": max(expected_reports_count - generated_reports_count, 0),
            "evaluation_status": evaluation_status,
        }
        self.expectations.append(row)
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
async def test_e2e_extractor_like_payloads_flow() -> None:
    repo = E2ERepo()
    queue: asyncio.Queue = asyncio.Queue()
    processor = EventProcessor(
        consumer=DummyConsumer(),
        repo=repo,
        calculator=SemaphoreCalculator(),
        evaluator=AlertEvaluator(),
        alert_queue=queue,
        alert_grace_seconds=0,
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
            "reports_generated": "0",
        },
        {
            "event_type": "task_state_change",
            "dag_id": "D_BO_5000",
            "region": "BO",
            "timestamp": "2026-04-09T10:01:00Z",
            "run_id": "run_5000",
            "run_state": "failed",
            "task_id": "transform",
            "task_state": "upstream_failed",
            "upstream_task_id": "extract",
            "duration": "180",
            "reports_generated": "0",
            "detail": "URL dead timeout",
        },
    ]

    for idx, payload in enumerate(payloads, start=1):
        event = parse_airflow_event(f"{idx}-0", payload)
        await processor._process_one(event)

    await processor._process_pending_critical_alerts()

    assert len(repo.expectations) >= 1
    assert any(row["missing_reports_count"] > 0 for row in repo.expectations)
    assert any(inc["category"] in {"report_not_generated", "download_delay", "url_dead", "structure_change"} for inc in repo.incidences)
    assert queue.qsize() >= 1
