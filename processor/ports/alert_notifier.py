from __future__ import annotations

from abc import ABC, abstractmethod

from ..domain.models import AlertToSend


class IAlertNotifier(ABC):
    @abstractmethod
    async def send(self, alert: AlertToSend) -> bool:
        ...

    @property
    @abstractmethod
    def channel_name(self) -> str:
        ...

    async def notify_status_change(
        self,
        alert_id: int,
        action: str,
        dag_id: str,
        title: str,
        actor: str | None = None,
        suppression_reason: str | None = None,
    ) -> bool:
        """Notify that a dashboard action was taken on an alert.

        Default implementation is a no-op (returns True).
        Subclasses can override to send a message on resolve/acknowledge/suppress.
        """
        return True
