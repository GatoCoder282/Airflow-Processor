from __future__ import annotations

from abc import ABC, abstractmethod

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
    async def upsert_unknown_dag(self, dag_id: str, region: str) -> None:
        ...

    @abstractmethod
    async def get_publication_window(self, dag_id: str, region: str) -> dict[str, object] | None:
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