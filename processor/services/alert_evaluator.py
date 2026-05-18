from __future__ import annotations

from ..domain.enums import AlertSeverity, AlertType, EventType, IncidentCategory, SemaphoreColor
from ..domain.models import AlertToSend, RawEvent, SemaphoreResult


class AlertEvaluator:
    def evaluate(self, event: RawEvent, semaphore: SemaphoreResult, catalog: dict[str, object] | None) -> AlertToSend | None:
        criticality = (catalog or {}).get("criticality", "medium")
        source_tag = (catalog or {}).get("source_tag")
        cube_tag = (catalog or {}).get("cube_tag")

        if event.try_number and event.max_tries and event.try_number >= event.max_tries and event.task_state == "failed":
            return self._build_alert(
                event=event,
                alert_type=AlertType.RETRY_EXCEEDED,
                severity=AlertSeverity.CRITICAL,
                category=IncidentCategory.RETRY_EXCEEDED,
                title=f"{event.dag_id} - task {event.task_id} agoto reintentos",
                channels=["telegram"],
                source_tag=source_tag,
                cube_tag=cube_tag,
            )

        if event.event_type == EventType.SCHEDULER_UNHEALTHY:
            return self._build_alert(
                event=event,
                alert_type=AlertType.SCHEDULER_DOWN,
                severity=AlertSeverity.CRITICAL,
                category=IncidentCategory.SCHEDULER_ISSUE,
                title="Airflow scheduler CAIDO",
                channels=["telegram"],
                source_tag=source_tag,
                cube_tag=cube_tag,
            )

        if event.event_type in {EventType.IMPORT_ERROR, EventType.IMPORT_ERROR_DETECTED}:
            return self._build_alert(
                event=event,
                alert_type=AlertType.IMPORT_ERROR,
                severity=AlertSeverity.WARNING,
                category=IncidentCategory.IMPORT_ERROR,
                title=f"{event.dag_id} - import error detectado",
                channels=[],
                source_tag=source_tag,
                cube_tag=cube_tag,
            )

        if semaphore.color == SemaphoreColor.RED and event.task_state != "upstream_failed":
            severity = {
                "high": AlertSeverity.CRITICAL,
                "medium": AlertSeverity.WARNING,
                "low": AlertSeverity.INFO,
            }.get(str(criticality), AlertSeverity.WARNING)
            channels = ["telegram"] if severity == AlertSeverity.CRITICAL else []
            return self._build_alert(
                event=event,
                alert_type=AlertType.TASK_FAILED,
                severity=severity,
                category=IncidentCategory.DOWNLOAD_DELAY,
                title=f"{event.dag_id} - {semaphore.reason}",
                channels=channels,
                source_tag=source_tag,
                cube_tag=cube_tag,
            )

        return None

    def _build_alert(
        self,
        event: RawEvent,
        alert_type: AlertType,
        severity: AlertSeverity,
        category: IncidentCategory,
        title: str,
        channels: list[str],
        source_tag: str | None = None,
        cube_tag: str | None = None,
    ) -> AlertToSend:
        snippet = event.detail[:500] if event.detail else None
        return AlertToSend(
            dag_id=event.dag_id,
            region=event.region,
            run_id=event.run_id,
            task_id=event.task_id,
            alert_type=alert_type,
            severity=severity,
            incident_category=category,
            title=title,
            message=title,
            exception_snippet=snippet,
            event_type_source=event.event_type.value,
            channels=channels,
            start_date=event.start_date,
            source_tag=source_tag,
            cube_tag=cube_tag,
        )