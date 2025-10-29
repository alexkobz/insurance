from __future__ import annotations
from collections.abc import Iterable
import pandas as pd
from abc import ABC, abstractmethod
from typing import List


class RuDataStrategy(ABC):

    @abstractmethod
    def payloads(self) -> Iterable[List[dict]] | List[dict]:
        pass

    @abstractmethod
    async def send_requests(self) -> pd.DataFrame:
        pass
