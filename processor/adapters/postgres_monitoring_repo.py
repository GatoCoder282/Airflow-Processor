from __future__ import annotations

import json
import logging

import asyncpg

from ..domain.models import AlertToSend, ProcessedEvent
from ..ports.monitoring_repository import IMonitoringRepository

logger = logging.getLogger(__name__)


class PostgresMonitoringRepository(IMonitoringRepository):
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def upsert_dag_run(self, event: ProcessedEvent) -> None:
        raw = event.raw
        duration_seconds = raw.duration
        if duration_seconds is None and raw.end_date is not None and raw.start_date is not None:
            duration_seconds = (raw.end_date - raw.start_date).total_seconds()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO monitoring.dag_run_monitor (
                    region, dag_id, run_id, run_type, state, semaphore, semaphore_reason,
                    execution_date, start_date, end_date, duration_seconds,
                    avg_duration_ref, duration_deviation,
                    created_at, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW(),NOW())
                ON CONFLICT (dag_id, run_id, region) DO UPDATE SET
                    state = EXCLUDED.state,
                    semaphore = EXCLUDED.semaphore,
                    semaphore_reason = EXCLUDED.semaphore_reason,
                    end_date = COALESCE(EXCLUDED.end_date, monitoring.dag_run_monitor.end_date),
                    duration_seconds = COALESCE(EXCLUDED.duration_seconds, monitoring.dag_run_monitor.duration_seconds),
                    avg_duration_ref = EXCLUDED.avg_duration_ref,
                    duration_deviation = EXCLUDED.duration_deviation,
                    updated_at = NOW()
                """,
                raw.region,
                raw.dag_id,
                raw.run_id,
                raw.run_type,
                raw.run_state,
                event.semaphore.color.value,
                event.semaphore.reason,
                raw.execution_date,
                raw.start_date,
                raw.end_date,
                duration_seconds,
                event.semaphore.avg_duration_ref,
                event.semaphore.duration_deviation,
            )

    async def insert_task_instance(self, event: ProcessedEvent) -> None:
        raw = event.raw
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO monitoring.task_instance (
                    dag_id, region, run_id, task_id, state,
                    start_date, end_date, duration_seconds,
                    try_number, max_tries, sla_miss,
                    upstream_task_id, downstream_task_ids,
                    log_excerpt, last_log_token,
                    created_at, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,NOW(),NOW())
                ON CONFLICT (dag_id, run_id, task_id, try_number, region) DO UPDATE SET
                    state = EXCLUDED.state,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date,
                    duration_seconds = EXCLUDED.duration_seconds,
                    sla_miss = EXCLUDED.sla_miss,
                    upstream_task_id = COALESCE(EXCLUDED.upstream_task_id, monitoring.task_instance.upstream_task_id),
                    downstream_task_ids = COALESCE(EXCLUDED.downstream_task_ids, monitoring.task_instance.downstream_task_ids),
                    log_excerpt = COALESCE(EXCLUDED.log_excerpt, monitoring.task_instance.log_excerpt),
                    last_log_token = COALESCE(EXCLUDED.last_log_token, monitoring.task_instance.last_log_token),
                    updated_at = NOW()
                """,
                raw.dag_id,
                raw.region,
                raw.run_id,
                raw.task_id,
                raw.task_state,
                raw.start_date,
                raw.end_date,
                raw.duration,
                raw.try_number,
                raw.max_tries,
                raw.sla_miss,
                raw.upstream_task_id,
                json.dumps(raw.downstream_task_ids) if raw.downstream_task_ids else None,
                raw.log_excerpt,
                raw.last_log_token,
            )

    async def ensure_run_exists(self, dag_id: str, run_id: str, region: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO monitoring.dag_run_monitor
                    (dag_id, run_id, region, state, semaphore, created_at, updated_at)
                VALUES ($1, $2, $3, 'running', 'yellow', NOW(), NOW())
                ON CONFLICT (dag_id, run_id, region) DO NOTHING
                """,
                dag_id, run_id, region,
            )

    async def update_run_active_task(
        self, dag_id: str, run_id: str, region: str,
        task_id: str, task_state: str,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE monitoring.dag_run_monitor
                SET active_task_id    = $4,
                    active_task_state = $5,
                    updated_at        = NOW()
                WHERE dag_id = $1 AND run_id = $2 AND region = $3
                """,
                dag_id, run_id, region, task_id, task_state,
            )

    @staticmethod
    async def _insert_alert(
        conn: asyncpg.Connection,
        alert: AlertToSend,
        suppressed: bool = False,
        suppression_reason: str | None = None,
    ) -> int:
        """INSERT de una alerta usando la conexión recibida (sin adquirir una nueva)."""
        row = await conn.fetchrow(
            """
            INSERT INTO monitoring.alert (
                region, severity, alert_type, incident_category,
                title, message, exception_snippet,
                event_type_source, dedup_key,
                root_cause_task_id, id_report,
                dag_id, run_id, task_id,
                resolved, suppressed, suppression_reason,
                occurrence_count, first_seen_at, last_seen_at,
                created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,FALSE,$15,$16,1,NOW(),NOW(),NOW(),NOW())
            RETURNING id
            """,
            alert.region,
            alert.severity.value,
            alert.alert_type.value,
            alert.incident_category.value,
            alert.title,
            alert.message,
            alert.exception_snippet,
            alert.event_type_source,
            alert.dedup_key,
            alert.root_cause_task_id,
            alert.id_report,
            alert.dag_id,
            alert.run_id,
            alert.task_id,
            suppressed,
            suppression_reason,
        )
        return int(row[0]) if row else 0

    async def insert_alert(self, alert: AlertToSend, suppressed: bool = False, suppression_reason: str | None = None) -> int:
        async with self._pool.acquire() as conn:
            return await self._insert_alert(conn, alert, suppressed, suppression_reason)

    async def upsert_alert_occurrence(
        self,
        alert: AlertToSend,
        suppressed: bool = False,
        suppression_reason: str | None = None,
    ) -> tuple[int, bool]:
        dedup_key = alert.dedup_key or ":".join([
            alert.dag_id,
            alert.alert_type.value,
            alert.task_id or alert.run_id or "-",
        ])
        alert.dedup_key = dedup_key

        # Una sola conexión + transacción: el SELECT...FOR UPDATE serializa el
        # incremento de occurrence_count (cierra la carrera TOCTOU) y el INSERT se hace
        # con la MISMA conexión (sin pool.acquire() anidado que podía causar deadlock).
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if not suppressed:
                    existing = await conn.fetchrow(
                        """
                        SELECT id, notified
                        FROM monitoring.alert
                        WHERE region = $1
                          AND dedup_key = $2
                          AND resolved = FALSE
                          AND suppressed = FALSE
                        ORDER BY created_at DESC
                        LIMIT 1
                        FOR UPDATE
                        """,
                        alert.region,
                        dedup_key,
                    )
                    if existing:
                        alert_id = int(existing["id"])
                        already_notified = bool(existing["notified"])
                        await conn.execute(
                            """
                            UPDATE monitoring.alert
                            SET occurrence_count = occurrence_count + 1,
                                last_seen_at = NOW(),
                                message = $2,
                                exception_snippet = COALESCE($3, exception_snippet),
                                event_type_source = COALESCE($4, event_type_source),
                                root_cause_task_id = COALESCE($5, root_cause_task_id),
                                id_report = COALESCE($6, id_report),
                                updated_at = NOW()
                            WHERE id = $1
                            """,
                            alert_id,
                            alert.message,
                            alert.exception_snippet,
                            alert.event_type_source,
                            alert.root_cause_task_id,
                            alert.id_report,
                        )
                        # Re-queue if alert was never notified (e.g. created before notifier was configured)
                        return alert_id, not already_notified

                alert_id = await self._insert_alert(conn, alert, suppressed=suppressed, suppression_reason=suppression_reason)
                return alert_id, True

    async def get_avg_duration(self, dag_id: str, region: str) -> float | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT AVG(duration_seconds) AS avg
                FROM monitoring.dag_run_monitor
                WHERE dag_id = $1 AND region = $2
                  AND state = 'success'
                  AND start_date >= NOW() - INTERVAL '30 days'
                """,
                dag_id,
                region,
            )
            value = row[0] if row else None
            return float(value) if value is not None else None

    async def get_dag_catalog_entry(self, dag_id: str, region: str) -> dict[str, object] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT criticality, sla_seconds, expected_reports_count, source_tag
                FROM monitoring.dag_catalog
                WHERE dag_id = $1 AND region = $2
                """,
                dag_id,
                region,
            )
            return dict(row) if row else None

    async def update_catalog_from_sync(self, event) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE monitoring.dag_catalog
                SET updated_at = NOW(),
                    has_import_error = CASE
                        WHEN $1 = 'import_error_new' THEN TRUE
                        WHEN $1 = 'import_error_resolved' THEN FALSE
                        ELSE has_import_error
                    END,
                    is_active = CASE
                        WHEN $1 = 'dag_paused' THEN FALSE
                        WHEN $1 = 'dag_unpaused' THEN TRUE
                        ELSE is_active
                    END
                WHERE dag_id = $2 AND region = $3
                """,
                event.sync_type,
                event.dag_id,
                event.region,
            )

    async def mark_alert_notified(self, alert_id: int, channel: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE monitoring.alert
                SET notified = TRUE,
                    notified_at = NOW(),
                    notified_channel = $2,
                    channel = $2,
                    updated_at = NOW()
                WHERE id = $1
                """,
                alert_id,
                channel,
            )

    async def _get_report_link(self, dag_id: str, region: str) -> dict[str, object] | None:
        """Devuelve el dag_report_link primario (id_report, id_file, ventana de
        publicación y pesos de prioridad). Fuente única para get_primary_report_link
        y get_publication_window."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id_report, id_file,
                       expected_publication_window,
                       report_priority_weight, source_priority_weight, frequency_priority_weight
                FROM monitoring.dag_report_link
                WHERE dag_id = $1 AND region = $2
                ORDER BY is_primary DESC, created_at DESC
                LIMIT 1
                """,
                dag_id,
                region,
            )
            if not row:
                return None
            result = dict(row)
            raw = result.get("expected_publication_window")
            if isinstance(raw, str):
                result["expected_publication_window"] = json.loads(raw)
            return result

    async def get_primary_report_link(self, dag_id: str, region: str) -> dict[str, object] | None:
        return await self._get_report_link(dag_id, region)

    async def get_root_cause_task_id(self, dag_id: str, region: str, run_id: str) -> str | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT task_id
                FROM monitoring.task_instance
                WHERE dag_id = $1
                  AND region = $2
                  AND run_id = $3
                  AND state IN ('failed', 'upstream_failed')
                ORDER BY CASE WHEN state = 'failed' THEN 0 ELSE 1 END,
                         updated_at DESC
                LIMIT 1
                """,
                dag_id,
                region,
                run_id,
            )
            return str(row[0]) if row and row[0] is not None else None

    async def get_publication_window(self, dag_id: str, region: str) -> dict[str, object] | None:
        return await self._get_report_link(dag_id, region)

    async def upsert_unknown_dag(self, dag_id: str, region: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO monitoring.dag_catalog (
                    dag_id, region, dag_type, is_active, has_import_error,
                    created_at, updated_at, last_seen
                ) VALUES ($1, $2, '?', TRUE, FALSE, NOW(), NOW(), NOW())
                ON CONFLICT (dag_id, region) DO NOTHING
                """,
                dag_id,
                region,
            )
        logger.warning("DAG desconocido insertado en dag_catalog con valores por defecto: dag_id=%s region=%s", dag_id, region)

    async def update_run_task_counts(self, dag_id: str, run_id: str, region: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE monitoring.dag_run_monitor drm
                SET total_tasks   = cnt.total,
                    success_tasks = cnt.success,
                    failed_tasks  = cnt.failed,
                    skipped_tasks = cnt.skipped,
                    updated_at    = NOW()
                FROM (
                    SELECT
                        COUNT(*)                                                      AS total,
                        COUNT(*) FILTER (WHERE state = 'success')                    AS success,
                        COUNT(*) FILTER (WHERE state IN ('failed','upstream_failed')) AS failed,
                        COUNT(*) FILTER (WHERE state = 'skipped')                    AS skipped
                    FROM monitoring.task_instance
                    WHERE dag_id = $1 AND run_id = $2 AND region = $3
                ) cnt
                WHERE drm.dag_id = $1 AND drm.run_id = $2 AND drm.region = $3
                """,
                dag_id, run_id, region,
            )

    async def resolve_stale_running_runs(self) -> None:
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE monitoring.dag_run_monitor drm
                SET state            = 'failed',
                    semaphore        = 'red',
                    semaphore_reason = 'run posiblemente interrumpido: lleva ' ||
                        ROUND(EXTRACT(EPOCH FROM (NOW() - drm.start_date)) / 60) || ' min en running',
                    updated_at       = NOW()
                WHERE drm.state = 'running'
                  AND drm.start_date IS NOT NULL
                  AND EXTRACT(EPOCH FROM (NOW() - drm.start_date)) > GREATEST(
                      (
                          SELECT COALESCE(AVG(d.duration_seconds), 21600) * 3
                          FROM monitoring.dag_run_monitor d
                          WHERE d.dag_id = drm.dag_id
                            AND d.region = drm.region
                            AND d.state  IN ('success', 'failed')
                            AND d.duration_seconds IS NOT NULL
                            AND d.start_date >= NOW() - INTERVAL '90 days'
                      ),
                      21600
                  )
                """
            )
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                logger.warning("Marcados %s runs como fallidos por estado stale", count)

    async def auto_resolve_dag_alerts(self, dag_id: str, region: str, reason: str) -> int:
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE monitoring.alert
                SET resolved           = TRUE,
                    resolved_at        = NOW(),
                    resolved_reason    = $3,
                    auto_resolved      = TRUE,
                    resolution_seconds = GREATEST(EXTRACT(EPOCH FROM (NOW() - first_seen_at))::int, 0),
                    updated_at         = NOW()
                WHERE dag_id    = $1
                  AND region    = $2
                  AND resolved  = FALSE
                  AND suppressed = FALSE
                """,
                dag_id,
                region,
                reason,
            )
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                logger.info("Auto-resolved %s alerts dag=%s region=%s reason=%s", count, dag_id, region, reason)
            return count

    async def close(self) -> None:
        return None