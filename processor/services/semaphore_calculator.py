from __future__ import annotations

from ..domain.enums import SemaphoreColor
from ..domain.models import RawEvent, SemaphoreResult


class SemaphoreCalculator:
    YELLOW_THRESHOLD_PCT = 50.0
    RED_THRESHOLD_PCT = 150.0

    def calculate(self, event: RawEvent, avg_duration: float | None, sla_seconds: int | None) -> SemaphoreResult:
        deviation = None
        if avg_duration and event.duration:
            deviation = ((event.duration - avg_duration) / avg_duration) * 100.0

        if event.task_state in {"failed", "upstream_failed"}:
            return SemaphoreResult(
                color=SemaphoreColor.RED,
                reason=f"task {event.task_id} en estado {event.task_state}",
                avg_duration_ref=avg_duration,
                duration_deviation=deviation,
            )

        if event.sla_miss:
            return SemaphoreResult(
                color=SemaphoreColor.RED,
                reason="SLA miss detectado",
                avg_duration_ref=avg_duration,
                duration_deviation=deviation,
            )

        if event.try_number and event.try_number > 1:
            return SemaphoreResult(
                color=SemaphoreColor.RED,
                reason=f"intento {event.try_number} de {event.max_tries}",
                avg_duration_ref=avg_duration,
                duration_deviation=deviation,
            )

        if event.run_state == "failed":
            return SemaphoreResult(
                color=SemaphoreColor.RED,
                reason="run completo en estado failed",
                avg_duration_ref=avg_duration,
                duration_deviation=deviation,
            )

        if sla_seconds and event.duration and event.duration > sla_seconds:
            return SemaphoreResult(
                color=SemaphoreColor.RED,
                reason=f"duracion {event.duration:.0f}s supera SLA {sla_seconds}s",
                avg_duration_ref=avg_duration,
                duration_deviation=deviation,
            )

        if deviation is not None:
            if deviation > self.RED_THRESHOLD_PCT:
                return SemaphoreResult(
                    color=SemaphoreColor.RED,
                    reason=f"duracion {deviation:.0f}% sobre el promedio",
                    avg_duration_ref=avg_duration,
                    duration_deviation=deviation,
                )
            if deviation > self.YELLOW_THRESHOLD_PCT:
                return SemaphoreResult(
                    color=SemaphoreColor.YELLOW,
                    reason=f"duracion {deviation:.0f}% sobre el promedio",
                    avg_duration_ref=avg_duration,
                    duration_deviation=deviation,
                )

        state = event.task_state or event.run_state or ""
        if state == "success":
            return SemaphoreResult(color=SemaphoreColor.GREEN, reason="exitoso", avg_duration_ref=avg_duration, duration_deviation=deviation)
        if state in {"running", "queued"}:
            return SemaphoreResult(color=SemaphoreColor.YELLOW, reason="en ejecucion", avg_duration_ref=avg_duration, duration_deviation=deviation)
        return SemaphoreResult(color=SemaphoreColor.YELLOW, reason=f"estado: {state}", avg_duration_ref=avg_duration, duration_deviation=deviation)