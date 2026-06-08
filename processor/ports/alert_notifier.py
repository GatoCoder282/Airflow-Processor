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
