from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from ..domain.models import ProcessedEvent, RawEvent
from ..ports.event_consumer import IEventConsumer
from ..ports.monitoring_repository import IMonitoringRepository
from .alert_evaluator import AlertEvaluator
from .semaphore_calculator import SemaphoreCalculator

logger = logging.getLogger(__name__)


class EventProcessor:
    def __init__(
        self,
        consumer: IEventConsumer,
        repo: IMonitoringRepository,
        calculator: SemaphoreCalculator,
        evaluator: AlertEvaluator,
        alert_queue: asyncio.Queue,
    ):
        self._consumer = consumer
        self._repo = repo
        self._calculator = calculator
        self._evaluator = evaluator
        self._alert_queue = alert_queue
        self._metrics = {
            "processed_events": 0,
            "discarded_events": 0,
            "alerts_created": 0,
            "alerts_deduplicated": 0,
        }

    def get_metrics(self) -> dict[str, int]:
        return {k: int(v) for k, v in self._metrics.items()}

    @staticmethod
    def _build_dedup_key(event: RawEvent, alert_type_value: str) -> str:
        if alert_type_value == "task_failed":
            scope = event.run_id or "-"
        else:
            scope = event.task_id or event.run_id or "-"
        return f"{event.dag_id}:{alert_type_value}:{scope}"

    async def run(self) -> None:
        await self._consumer.setup()
        logger.info("EventProcessor iniciado")
        while True:
            try:
                events = await self._consumer.read_events(count=20)
                logger.debug("stage=consume status=ok batch_size=%s", len(events))
                for event in events:
                    await self._process_one(event)
            except asyncio.CancelledError:
                logger.info("EventProcessor detenido")
                break
            except Exception:
                logger.exception("Error en EventProcessor")
                await asyncio.sleep(1)

    async def _process_one(self, event: RawEvent) -> None:
        try:
            logger.debug("stage=enrich status=start msg_id=%s dag_id=%s", event.msg_id, event.dag_id)
            catalog = await self._repo.get_dag_catalog_entry(event.dag_id, event.region)
            if catalog is None:
                await self._repo.upsert_unknown_dag(event.dag_id, event.region)
                catalog = await self._repo.get_dag_catalog_entry(event.dag_id, event.region)
            report_link = await self._repo.get_primary_report_link(event.dag_id, event.region)
            avg_duration = await self._repo.get_avg_duration(event.dag_id, event.region)
            sla_seconds = (catalog or {}).get("sla_seconds")
            logger.debug("stage=enrich status=ok msg_id=%s", event.msg_id)

            semaphore = self._calculator.calculate(event, avg_duration, sla_seconds)
            alert = self._evaluator.evaluate(event, semaphore, catalog)

            processed = ProcessedEvent(
                raw=event,
                semaphore=semaphore,
                dag_criticality=(catalog or {}).get("criticality"),
                dag_sla_seconds=sla_seconds,
                should_alert=alert is not None,
                alert=alert,
            )

            if event.run_id:
                if event.run_state is not None:
                    await self._repo.upsert_dag_run(processed)
                if event.run_state in ("success", "failed"):
                    await self._repo.update_run_task_counts(event.dag_id, event.run_id, event.region)
                if event.run_state == "success":
                    await self._repo.auto_resolve_dag_alerts(
                        event.dag_id, event.region,
                        f"next_run_succeeded:{event.run_id}",
                    )
                await self._check_publication_window(event, catalog)
            if event.task_id:
                if event.run_id:
                    await self._repo.ensure_run_exists(event.dag_id, event.run_id, event.region)
                await self._repo.insert_task_instance(processed)
                if event.run_id:
                    await self._repo.update_run_active_task(
                        event.dag_id, event.run_id, event.region,
                        event.task_id, event.task_state or "",
                    )
            logger.debug("stage=persist status=ok msg_id=%s", event.msg_id)

            if alert:
                alert.dedup_key = self._build_dedup_key(event, alert.alert_type.value)
                alert.root_cause_task_id = await self._derive_root_cause_task_id(event)
                if report_link and report_link.get("id_report") is not None:
                    alert.id_report = int(report_link["id_report"])
                alert.semaphore_reason = semaphore.reason
                if event.task_id:
                    alert.active_task_id = event.task_id
                    alert.active_task_state = event.task_state
                # Sin ventana de gracia: toda alerta (incluidas las críticas) se despacha de
                # inmediato. El auto-resolve cierra la alerta cuando el siguiente run es exitoso.
                await self._dispatch_alert(alert)

            await self._consumer.acknowledge(event.msg_id)
            self._metrics["processed_events"] += 1
        except Exception:
            self._metrics["discarded_events"] += 1
            logger.exception("Error procesando evento msg_id=%s", event.msg_id)

    async def _dispatch_alert(self, alert) -> None:
        """Deduplica (occurrence_count) y encola la alerta para su notificación."""
        alert_id, created = await self._repo.upsert_alert_occurrence(alert, suppressed=False, suppression_reason=None)
        if created:
            self._metrics["alerts_created"] += 1
            await self._alert_queue.put((alert_id, alert))
            logger.debug("stage=alert status=created alert_id=%s dedup_key=%s", alert_id, alert.dedup_key)
        else:
            self._metrics["alerts_deduplicated"] += 1
            logger.debug("stage=alert status=deduplicated dedup_key=%s", alert.dedup_key)

    async def _check_publication_window(
        self,
        event: RawEvent,
        catalog: dict[str, object] | None,
    ) -> None:
        """Generate a DOWNLOAD_DELAY alert if the run finished after the expected publication window."""
        if event.run_state not in ("success", "failed"):
            return
        if not event.end_date:
            return

        pub_window = await self._repo.get_publication_window(event.dag_id, event.region)
        if not pub_window:
            return
        window_config = pub_window.get("expected_publication_window")
        if not window_config or not isinstance(window_config, dict):
            return

        days_after = int(window_config.get("days_after_period_end") or 0)
        latest_hour = int(window_config.get("latest_hour") or 23)
        tz_name = str(window_config.get("timezone") or "UTC")

        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("UTC")

        now_local = datetime.now(tz)
        # Execution date serves as the period reference
        period_ref = event.execution_date or event.end_date
        if period_ref.tzinfo is None:
            period_ref = period_ref.replace(tzinfo=timezone.utc)
        period_ref_local = period_ref.astimezone(tz)

        deadline = period_ref_local.replace(hour=latest_hour, minute=0, second=0, microsecond=0)
        deadline = deadline + timedelta(days=days_after)

        if now_local <= deadline:
            return

        from ..domain.models import AlertToSend
        from ..domain.enums import AlertType, AlertSeverity, IncidentCategory
        criticality = (catalog or {}).get("criticality", "low")
        severity = AlertSeverity.CRITICAL if criticality == "high" else AlertSeverity.WARNING
        id_report = pub_window.get("id_report")
        alert = AlertToSend(
            dag_id=event.dag_id,
            region=event.region,
            run_id=event.run_id,
            alert_type=AlertType.DOWNLOAD_DELAY,
            severity=severity,
            incident_category=IncidentCategory.DOWNLOAD_DELAY,
            title=f"Retraso de publicación: {event.dag_id}",
            message=(
                f"El run {event.run_id} finalizó pero el reporte esperado no fue publicado "
                f"dentro de la ventana ({days_after}d + {latest_hour}h {tz_name}). "
                f"Deadline: {deadline.isoformat()}"
            ),
            event_type_source=event.event_type.value,
            id_report=int(id_report) if id_report is not None else None,
            channels=["slack", "telegram"],
        )
        alert.dedup_key = self._build_dedup_key(event, AlertType.DOWNLOAD_DELAY.value)
        logger.info("DOWNLOAD_DELAY alert dag=%s run=%s deadline=%s", event.dag_id, event.run_id, deadline)
        await self._dispatch_alert(alert)

    async def _derive_root_cause_task_id(self, event: RawEvent) -> str | None:
        if not event.run_id:
            return event.task_id
        if event.task_state == "upstream_failed":
            root = await self._repo.get_root_cause_task_id(event.dag_id, event.region, event.run_id)
            return root or event.upstream_task_id or event.task_id
        return event.task_id
