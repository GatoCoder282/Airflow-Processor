from __future__ import annotations

import asyncio
import os
import uuid

import asyncpg
from dotenv import load_dotenv

from processor.adapters.postgres_monitoring_repo import PostgresMonitoringRepository
from processor.domain.enums import AlertSeverity, AlertType, IncidentCategory
from processor.domain.models import AlertToSend


async def main() -> int:
    load_dotenv()
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL not found in environment/.env")
        return 1

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    repo = PostgresMonitoringRepository(pool)

    test_id = uuid.uuid4().hex[:12]
    dedup_key = f"smoke:{test_id}:dedup"
    run_id = f"smoke-run-{test_id}"

    alert_id_1 = None
    alert_id_2 = None

    try:
        alert = AlertToSend(
            dag_id="system",
            region="global",
            run_id=run_id,
            task_id="smoke_task",
            alert_type=AlertType.TASK_FAILED,
            severity=AlertSeverity.WARNING,
            incident_category=IncidentCategory.DOWNLOAD_DELAY,
            title=f"smoke alert {test_id}",
            message=f"smoke alert message {test_id}",
            exception_snippet="smoke snippet",
            event_type_source="task_state_change",
            dedup_key=dedup_key,
            root_cause_task_id="smoke_task",
            id_report=999999,
            channels=["slack"],
        )

        # ── Deduplicación (occurrence_count) ──────────────────────────────────
        alert_id_1, created_1 = await repo.upsert_alert_occurrence(alert, suppressed=False, suppression_reason=None)
        alert_id_2, created_2 = await repo.upsert_alert_occurrence(alert, suppressed=False, suppression_reason=None)

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, occurrence_count, dedup_key, root_cause_task_id, id_report, last_seen_at
                FROM monitoring.alert
                WHERE id = $1
                """,
                alert_id_1,
            )

        if not row:
            print("ERROR: dedup alert row not found")
            return 1

        print(f"DEDUP created flags: first={created_1} second={created_2}")
        print(f"DEDUP occurrence_count={row['occurrence_count']} alert_id_1={alert_id_1} alert_id_2={alert_id_2}")

        assert created_1 is True
        assert created_2 is False
        assert alert_id_1 == alert_id_2
        assert int(row["occurrence_count"]) == 2

        # ── Notificación (compatibilidad notified_channel + channel) ──────────
        await repo.mark_alert_notified(alert_id_1, "slack")

        async with pool.acquire() as conn:
            notify_row = await conn.fetchrow(
                """
                SELECT notified, notified_channel, channel
                FROM monitoring.alert
                WHERE id = $1
                """,
                alert_id_1,
            )

        assert notify_row is not None
        assert bool(notify_row["notified"]) is True
        assert notify_row["notified_channel"] == "slack"
        assert notify_row["channel"] == "slack"
        print("NOTIFY compatibility OK: notified_channel + channel")

        # ── Auto-resolución (reemplaza la gestión manual ack/suppress/resolve) ─
        #    Debe marcar resolved + auto_resolved y poblar resolution_seconds (MTTR).
        resolved_count = await repo.auto_resolve_dag_alerts("system", "global", f"smoke_auto_resolve:{test_id}")
        print(f"AUTO_RESOLVE resolved_count={resolved_count}")
        assert resolved_count >= 1

        async with pool.acquire() as conn:
            resolved_row = await conn.fetchrow(
                """
                SELECT resolved, auto_resolved, resolved_reason, resolution_seconds
                FROM monitoring.alert
                WHERE id = $1
                """,
                alert_id_1,
            )

        assert resolved_row is not None
        assert bool(resolved_row["resolved"]) is True
        assert bool(resolved_row["auto_resolved"]) is True
        assert resolved_row["resolution_seconds"] is not None
        print("AUTO_RESOLVE OK: resolved + auto_resolved + resolution_seconds (MTTR)")

        print("SMOKE TEST SPRINT2: SUCCESS")
        return 0
    finally:
        async with pool.acquire() as conn:
            if alert_id_1 is not None:
                await conn.execute("DELETE FROM monitoring.alert WHERE id = $1", alert_id_1)
            else:
                await conn.execute("DELETE FROM monitoring.alert WHERE region = 'global' AND dedup_key = $1", dedup_key)

        await pool.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
