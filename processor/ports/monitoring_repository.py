from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from ..domain.models import AlertToSend, ProcessedEvent


class IMonitoringRepository(ABC):
    @abstractmethod
    async def upsert_dag_run(self, event: ProcessedEvent) -> None:
        ...

    @abstractmethod
    async def insert_task_instance(self, event: ProcessedEvent) -> None:
        ...

    @abstractmethod
    async def ensure_run_exists(self, dag_id: str, run_id: str, region: str) -> None:
        ...

    @abstractmethod
    async def update_run_active_task(
        self, dag_id: str, run_id: str, region: str,
        task_id: str, task_state: str,
    ) -> None:
        ...

    @abstractmethod
    async def insert_alert(self, alert: AlertToSend, suppressed: bool = False, suppression_reason: str | None = None) -> int:
        ...

    @abstractmethod
    async def upsert_alert_occurrence(
        self,
        alert: AlertToSend,
        suppressed: bool = False,
        suppression_reason: str | None = None,
    ) -> tuple[int, bool]:
        ...

    @abstractmethod
    async def get_avg_duration(self, dag_id: str, region: str) -> float | None:
        ...

    @abstractmethod
    async def get_dag_catalog_entry(self, dag_id: str, region: str) -> dict[str, object] | None:
        ...

    @abstractmethod
    async def has_report_evidence(self, dag_id: str, run_id: str | None, grace_started_at: datetime) -> bool:
        ...

    @abstractmethod
    async def update_catalog_from_sync(self, event) -> None:
        ...

    @abstractmethod
    async def mark_alert_notified(self, alert_id: int, channel: str) -> None:
        ...

    @abstractmethod
    async def get_primary_report_link(self, dag_id: str, region: str) -> dict[str, object] | None:
        ...

    @abstractmethod
    async def get_root_cause_task_id(self, dag_id: str, region: str, run_id: str) -> str | None:
        ...

    @abstractmethod
    async def upsert_report_run_expectation(
        self,
        dag_id: str,
        region: str,
        run_id: str,
        expected_reports_count: int,
        generated_reports_count: int,
        evaluation_status: str,
        evaluated_at: datetime | None,
    ) -> dict[str, object]:
        ...

    @abstractmethod
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
    ) -> int:
        ...

    @abstractmethod
    async def upsert_unknown_dag(self, dag_id: str, region: str) -> None:
        ...

    @abstractmethod
    async def get_publication_window(self, dag_id: str, region: str) -> dict[str, object] | None:
        ...

    @abstractmethod
    async def count_reports_for_dag(self, dag_id: str, region: str, execution_date: datetime | None = None) -> int:
        ...

    @abstractmethod
    async def update_run_task_counts(self, dag_id: str, run_id: str, region: str) -> None:
        ...

    @abstractmethod
    async def resolve_stale_running_runs(self) -> None:
        ...

    @abstractmethod
    async def auto_resolve_dag_alerts(self, dag_id: str, region: str, reason: str) -> int:
        """Resuelve todas las alertas abiertas de un DAG. Retorna cantidad resuelta."""
        ...

    @abstractmethod
    async def close(self) -> None:
        ...