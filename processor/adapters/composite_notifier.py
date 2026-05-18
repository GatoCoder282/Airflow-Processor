from __future__ import annotations

import asyncio

from ..domain.models import AlertToSend
from ..ports.alert_notifier import IAlertNotifier


class CompositeNotifier(IAlertNotifier):
    channel_name = "composite"

    def __init__(self, notifiers: list[IAlertNotifier]):
        self._notifiers = notifiers

    async def send(self, alert: AlertToSend) -> bool:
        results = await asyncio.gather(
            *(notifier.send(alert) for notifier in self._notifiers),
            return_exceptions=True,
        )
        return any(result is True for result in results)

    async def notify_status_change(
        self,
        alert_id: int,
        action: str,
        dag_id: str,
        title: str,
        actor: str | None = None,
        suppression_reason: str | None = None,
    ) -> bool:
        results = await asyncio.gather(
            *(
                notifier.notify_status_change(alert_id, action, dag_id, title, actor, suppression_reason)
                for notifier in self._notifiers
            ),
            return_exceptions=True,
        )
        return any(result is True for result in results)
