from __future__ import annotations
from collections.abc import Iterable
import pandas as pd
from abc import ABC, abstractmethod
from asyncio import Task
from typing import List
from aiohttp import ClientSession


class RuDataStrategy(ABC):

    @abstractmethod
    def payloads(self) -> Iterable[List[dict]] | List[dict]:
        pass

    @abstractmethod
    def create_tasks(self, chunk_payloads: List[dict], session: ClientSession) -> List[Task]:
        pass

    @abstractmethod
    async def execute_tasks(self, tasks: List[Task]) -> None:
        pass

    @abstractmethod
    async def send_requests(self) -> pd.DataFrame:
        pass
