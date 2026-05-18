from __future__ import annotations

from ..domain.models import AlertToSend
from ..ports.alert_notifier import IAlertNotifier


class NullNotifier(IAlertNotifier):
    channel_name = "null"

    async def send(self, alert: AlertToSend) -> bool:
        return True