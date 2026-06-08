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


class _FakeTxn:
    async def __aenter__(self) -> "_FakeTxn":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class DedupConnection:
    """Conexión fake que distingue SELECT (dedup) de INSERT y soporta transaction()."""

    def __init__(self, existing_row: dict | None) -> None:
        self.existing_row = existing_row
        self.select_for_update = False
        self.update_executed = False
        self.insert_executed = False

    def transaction(self) -> _FakeTxn:
        return _FakeTxn()

    async def fetchrow(self, query: str, *params: object):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT id, notified"):
            assert "FOR UPDATE" in normalized  # serializa el incremento (cierra TOCTOU)
            self.select_for_update = True
            return self.existing_row
        if "INSERT INTO monitoring.alert" in normalized:
            self.insert_executed = True
            return {0: 555}
        return None

    async def execute(self, query: str, *params: object) -> str:
        normalized = " ".join(query.split())
        if "occurrence_count = occurrence_count + 1" in normalized:
            self.update_executed = True
        return "OK"

    async def __aenter__(self) -> "DedupConnection":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None


class CountingPool:
    def __init__(self, connection: DedupConnection) -> None:
        self.connection = connection
        self.acquire_count = 0

    def acquire(self) -> DedupConnection:
        self.acquire_count += 1
        return self.connection


@pytest.mark.asyncio
async def test_upsert_alert_occurrence_increments_without_nested_acquire() -> None:
    conn = DedupConnection(existing_row={"id": 42, "notified": True})
    pool = CountingPool(conn)
    repo = PostgresMonitoringRepository(pool)  # type: ignore[arg-type]

    alert = _build_alert()
    alert.dedup_key = "dedup-1"
    alert_id, created = await repo.upsert_alert_occurrence(alert)

    assert alert_id == 42
    assert created is False  # ya estaba notificada → no se re-encola
    assert conn.update_executed is True
    assert conn.insert_executed is False
    assert conn.select_for_update is True
    assert pool.acquire_count == 1  # una sola conexión: sin acquire() anidado


@pytest.mark.asyncio
async def test_upsert_alert_occurrence_inserts_when_new() -> None:
    conn = DedupConnection(existing_row=None)
    pool = CountingPool(conn)
    repo = PostgresMonitoringRepository(pool)  # type: ignore[arg-type]

    alert = _build_alert()
    alert.dedup_key = "dedup-2"
    alert_id, created = await repo.upsert_alert_occurrence(alert)

    assert created is True
    assert conn.insert_executed is True
    assert pool.acquire_count == 1
