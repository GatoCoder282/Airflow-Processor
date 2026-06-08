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
