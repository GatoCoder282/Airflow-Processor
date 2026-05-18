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
        alert_grace_seconds: int = 120,
    ):
        self._consumer = consumer
        self._repo = repo
        self._calculator = calculator
        self._evaluator = evaluator
        self._alert_queue = alert_queue
        self._alert_grace_seconds = alert_grace_seconds
        self._pending_critical: dict[str, tuple[RawEvent, object, datetime]] = {}
        self._metrics = {
            "processed_events": 0,
            "discarded_events": 0,
            "alerts_created": 0,
            "alerts_deduplicated": 0,
            "alerts_suppressed": 0,
        }

    def get_metrics(self) -> dict[str, int]:
        return {k: int(v) for k, v in self._metrics.items()}

    def _event_key(self, event: RawEvent) -> str:
        return "|".join([event.msg_id, event.event_type.value, event.dag_id, event.run_id or "", event.task_id or ""])

    @staticmethod
    def _build_dedup_key(event: RawEvent, alert_type_value: str) -> str:
        if alert_type_value == "task_failed":
            scope = event.run_id or "-"
        else:
            scope = event.task_id or event.run_id or "-"
        return f"{event.dag_id}:{alert_type_value}:{scope}"

    @staticmethod
    def _report_evaluation_status(expected_reports_count: int, generated_reports_count: int) -> str:
        if expected_reports_count <= 0:
            return "ok"
        if generated_reports_count <= 0:
            return "failed"
        if generated_reports_count < expected_reports_count:
            return "partial"
        return "ok"

    @staticmethod
    def _incidence_category_from_event(event: RawEvent) -> str:
        detail = (event.detail or "").lower()
        if "url" in detail and ("dead" in detail or "404" in detail or "timeout" in detail):
            return "url_dead"
        if event.event_type.value == "import_error_detected":
            return "structure_change"
        if event.event_type.value == "dag_warning":
            return "structure_change"
        return "download_delay"

    @staticmethod
    def _severity_for_missing_reports(missing_reports_count: int) -> str:
        return "critical" if missing_reports_count > 0 else "warning"

    @staticmethod
    def _priority_score(report_link: dict[str, object] | None) -> float:
        report_weight = float((report_link or {}).get("report_priority_weight") or 100)
        source_weight = float((report_link or {}).get("source_priority_weight") or 50)
        frequency_weight = float((report_link or {}).get("frequency_priority_weight") or 25)
        return (report_weight * 0.5) + (source_weight * 0.3) + (frequency_weight * 0.2)

    async def run(self) -> None:
        await self._consumer.setup()
        logger.info("EventProcessor iniciado")
        while True:
            try:
                events = await self._consumer.read_events(count=20)
                logger.debug("stage=consume status=ok batch_size=%s", len(events))
                for event in events:
                    await self._process_one(event)
                await self._process_pending_critical_alerts()
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
                await self._upsert_report_expectation_and_incidences(event, catalog, report_link, sla_seconds)
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

                if alert.severity.value == "critical":
                    self._pending_critical[self._event_key(event)] = (event, alert, datetime.now(timezone.utc))
                    logger.info("Critical alert buffered for grace window: dag=%s run=%s", event.dag_id, event.run_id)
                else:
                    alert_id, created = await self._repo.upsert_alert_occurrence(alert, suppressed=False, suppression_reason=None)
                    if created:
                        self._metrics["alerts_created"] += 1
                        await self._alert_queue.put((alert_id, alert))
                        logger.debug("stage=alert status=created alert_id=%s dedup_key=%s", alert_id, alert.dedup_key)
                    else:
                        self._metrics["alerts_deduplicated"] += 1
                        logger.debug("stage=alert status=deduplicated dedup_key=%s", alert.dedup_key)

            await self._consumer.acknowledge(event.msg_id)
            self._metrics["processed_events"] += 1
        except Exception:
            self._metrics["discarded_events"] += 1
            logger.exception("Error procesando evento msg_id=%s", event.msg_id)

    async def _process_pending_critical_alerts(self) -> None:
        if not self._pending_critical:
            return

        now = datetime.now(timezone.utc)
        keys_to_remove: list[str] = []

        for key, (event, alert, buffered_at) in self._pending_critical.items():
            grace_deadline = buffered_at + timedelta(seconds=self._alert_grace_seconds)
            if grace_deadline > now:
                continue

            has_evidence = await self._repo.has_report_evidence(event.dag_id, event.run_id, buffered_at)
            if has_evidence:
                await self._repo.upsert_alert_occurrence(
                    alert,
                    suppressed=True,
                    suppression_reason="report_evidence_detected_during_grace_window",
                )
                self._metrics["alerts_suppressed"] += 1
                logger.info("Critical alert suppressed by evidence dag=%s run=%s", event.dag_id, event.run_id)
            else:
                alert_id, created = await self._repo.upsert_alert_occurrence(alert, suppressed=False, suppression_reason=None)
                if created:
                    self._metrics["alerts_created"] += 1
                    await self._alert_queue.put((alert_id, alert))
                    logger.debug("stage=alert status=created alert_id=%s dedup_key=%s", alert_id, alert.dedup_key)
                else:
                    self._metrics["alerts_deduplicated"] += 1
                    logger.debug("stage=alert status=deduplicated dedup_key=%s", alert.dedup_key)
                logger.info("Critical alert released after grace dag=%s run=%s", event.dag_id, event.run_id)

            keys_to_remove.append(key)

        for key in keys_to_remove:
            self._pending_critical.pop(key, None)

    async def _upsert_report_expectation_and_incidences(
        self,
        event: RawEvent,
        catalog: dict[str, object] | None,
        report_link: dict[str, object] | None,
        sla_seconds: object,
    ) -> None:
        if not event.run_id:
            return

        expected_reports = int((catalog or {}).get("expected_reports_count") or 0)
        if event.run_state in {"success", "failed"}:
            generated_reports = await self._repo.count_reports_for_dag(
                event.dag_id, event.region, execution_date=event.execution_date
            )
            evaluated_at = datetime.now(timezone.utc)
        else:
            generated_reports = 0
            evaluated_at = None
        status = self._report_evaluation_status(expected_reports, generated_reports)

        expectation = await self._repo.upsert_report_run_expectation(
            dag_id=event.dag_id,
            region=event.region,
            run_id=event.run_id,
            expected_reports_count=expected_reports,
            generated_reports_count=generated_reports,
            evaluation_status=status,
            evaluated_at=evaluated_at,
        )

        missing_reports = int(expectation.get("missing_reports_count") or 0)
        if missing_reports > 0 and (report_link is None or report_link.get("id_report") is None):
            logger.warning(
                "dag=%s run=%s tiene %s reportes faltantes pero no tiene dag_report_link — incidencia omitida",
                event.dag_id, event.run_id, missing_reports,
            )
        if missing_reports > 0 and report_link and report_link.get("id_report") is not None:
            await self._repo.insert_report_incidence(
                region=event.region,
                dag_id=event.dag_id,
                run_id=event.run_id,
                id_report=int(report_link["id_report"]),
                id_file=int(report_link["id_file"]) if report_link.get("id_file") is not None else None,
                category="report_not_generated",
                severity=self._severity_for_missing_reports(missing_reports),
                priority_score=self._priority_score(report_link),
                description=(
                    f"Run {event.run_id} genero {generated_reports} reportes de "
                    f"{expected_reports} esperados"
                ),
            )

        if report_link and report_link.get("id_report") is not None and sla_seconds is not None and event.duration is not None:
            try:
                if float(event.duration) > float(sla_seconds):
                    await self._repo.insert_report_incidence(
                        region=event.region,
                        dag_id=event.dag_id,
                        run_id=event.run_id,
                        id_report=int(report_link["id_report"]),
                        id_file=int(report_link["id_file"]) if report_link.get("id_file") is not None else None,
                        category=self._incidence_category_from_event(event),
                        severity="warning",
                        priority_score=self._priority_score(report_link),
                        description=f"Run {event.run_id} excedio SLA con duracion {event.duration}s",
                    )
            except (TypeError, ValueError):
                return

        await self._check_publication_window(event, catalog)

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
        alert_id, created = await self._repo.upsert_alert_occurrence(alert, suppressed=False, suppression_reason=None)
        if created:
            self._metrics["alerts_created"] += 1
            await self._alert_queue.put((alert_id, alert))
            logger.info("DOWNLOAD_DELAY alert creada dag=%s run=%s deadline=%s", event.dag_id, event.run_id, deadline)
        else:
            self._metrics["alerts_deduplicated"] += 1

    async def _derive_root_cause_task_id(self, event: RawEvent) -> str | None:
        if not event.run_id:
            return event.task_id
        if event.task_state == "upstream_failed":
            root = await self._repo.get_root_cause_task_id(event.dag_id, event.region, event.run_id)
            return root or event.upstream_task_id or event.task_id
        return event.task_id