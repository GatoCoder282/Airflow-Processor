from __future__ import annotations

from abc import ABC, abstractmethod

from ..domain.models import RawEvent


class IEventConsumer(ABC):
    @abstractmethod
    async def read_events(self, count: int = 10) -> list[RawEvent]:
        ...

    @abstractmethod
    async def acknowledge(self, *msg_ids: str) -> None:
        ...

    @abstractmethod
    async def get_pending_count(self) -> int:
        ...

    @abstractmethod
    async def setup(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...