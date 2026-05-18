from __future__ import annotations

from dataclasses import dataclass

import pytest

from processor.adapters.postgres_monitoring_repo import PostgresMonitoringRepository
from processor.domain.enums import AlertSeverity, AlertType, IncidentCategory
from processor.domain.models import AlertToSend


@dataclass
class FakeConnection:
    last_query: str | None = None
    last_params: tuple[object, ...] | None = None

    async def execute(self, query: str, *params: object) -> str:
        self.last_query = " ".join(query.split())
        self.last_params = params
        return "OK"

    async def fetchrow(self, query: str, *params: object):
        self.last_query = " ".join(query.split())
        self.last_params = params
        return [101]

    async def __aenter__(self) -> "FakeConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class FakePool:
    def __init__(self) -> None:
        self.connection = FakeConnection()

    def acquire(self) -> FakeConnection:
        return self.connection


def _build_alert() -> AlertToSend:
    return AlertToSend(
        dag_id="D_BO_0001",
        region="BO",
        run_id="run_1",
        task_id="task_1",
        alert_type=AlertType.TASK_FAILED,
        severity=AlertSeverity.WARNING,
        incident_category=IncidentCategory.DOWNLOAD_DELAY,
        title="Alert title",
        message="Alert message",
        exception_snippet="Traceback",
        event_type_source="task_state_change",
        channels=["slack"],
    )


@pytest.mark.asyncio
async def test_insert_alert_persists_v21_fields() -> None:
    pool = FakePool()
    repo = PostgresMonitoringRepository(pool)  # type: ignore[arg-type]

    alert_id = await repo.insert_alert(_build_alert(), suppressed=True, suppression_reason="maintenance")

    assert alert_id == 101
    assert pool.connection.last_query is not None
    assert "event_type_source" in pool.connection.last_query
    assert "suppressed" in pool.connection.last_query
    assert "suppression_reason" in pool.connection.last_query


@pytest.mark.asyncio
async def test_mark_alert_notified_updates_new_and_legacy_channel_fields() -> None:
    pool = FakePool()
    repo = PostgresMonitoringRepository(pool)  # type: ignore[arg-type]

    await repo.mark_alert_notified(1, "slack")

    assert pool.connection.last_query is not None
    assert "notified_channel = $2" in pool.connection.last_query
    assert "channel = $2" in pool.connection.last_query
    assert "updated_at = NOW()" in pool.connection.last_query
