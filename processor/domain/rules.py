from __future__ import annotations

from .enums import Semaphore, Severity
from .models import AirflowEvent, DagContext, ProcessingDecision


def _compute_duration_deviation(current: float | None, avg_ref: float | None) -> float | None:
    if current is None or avg_ref is None or avg_ref <= 0:
        return None
    return ((current - avg_ref) / avg_ref) * 100.0


def evaluate_event(
    event: AirflowEvent,
    context: DagContext,
    avg_duration_ref: float | None,
    reports_generated: int | None,
    warning_deviation_percent: float,
) -> ProcessingDecision:
    deviation = _compute_duration_deviation(event.duration, avg_duration_ref)

    if event.task_state == "failed" and event.try_number is not None and event.max_tries is not None and event.try_number >= event.max_tries:
        return ProcessingDecision(
            semaphore=Semaphore.RED,
            duration_deviation=deviation,
            should_alert=True,
            alert_severity=Severity.CRITICAL,
            alert_type="task_failed",
            incident_category="retry_exceeded",
            title=f"{event.dag_id} - task {event.task_id} agoto reintentos",
            message=(
                f"El DAG {event.dag_id} fallo en task {event.task_id} "
                f"despues de {event.max_tries} intentos"
            ),
        )

    if deviation is not None and deviation > warning_deviation_percent:
        return ProcessingDecision(
            semaphore=Semaphore.YELLOW,
            duration_deviation=deviation,
            should_alert=True,
            alert_severity=Severity.WARNING,
            alert_type="long_running",
            incident_category="duration_deviation",
            title=f"{event.dag_id} con desviacion de duracion",
            message=f"Duracion desviada en {deviation:.2f}% respecto al promedio",
        )

    if context.expected_reports_count is not None and reports_generated is not None and reports_generated < context.expected_reports_count:
        return ProcessingDecision(
            semaphore=Semaphore.RED,
            duration_deviation=deviation,
            should_alert=True,
            alert_severity=Severity.CRITICAL,
            alert_type="report_not_generated",
            incident_category="missing_report",
            title=f"{event.dag_id} no genero reportes esperados",
            message=(
                f"Reportes generados {reports_generated} / "
                f"esperados {context.expected_reports_count}"
            ),
        )

    if event.run_state == "failed" or event.task_state in {"failed", "upstream_failed"}:
        return ProcessingDecision(semaphore=Semaphore.RED, duration_deviation=deviation)

    if event.run_state == "running" or event.task_state in {"queued", "up_for_retry", "running"}:
        return ProcessingDecision(semaphore=Semaphore.YELLOW, duration_deviation=deviation)

    return ProcessingDecision(semaphore=Semaphore.GREEN, duration_deviation=deviation)
