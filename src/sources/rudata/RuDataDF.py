from __future__ import annotations
import asyncio
import aiohttp
import socket
import pandas as pd
from typing import List, Dict, Iterable

from src.utils.get_date import last_day_month
from src.utils.clickhouse_client import client as clickhouse_client, prepare_for_clickhouse
from src.utils.retries import retry
from src.logger.Logger import Logger
from src.sources.rudata.RuData import RuDataStrategy


LIMIT = 5
logger = Logger()


async def safe_task(task, timeout=120) -> List[dict | None]:
    try:
        return await asyncio.wait_for(task, timeout=timeout)
    except asyncio.TimeoutError:
        return []


class RuDataDF(RuDataStrategy):

    report_date = pd.to_datetime(last_day_month)
    report_yearmonth: str = last_day_month.strftime("%Y%m")
    headers: Dict[str, str] = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'content-type': 'application/json',
        'Accept': 'application/json',
    }
    semaphore: asyncio.Semaphore = asyncio.Semaphore(LIMIT)

    def __init__(self):
        self.name = self.__class__.__name__
        self._list_json: List[dict] = []
        self._df: pd.DataFrame = pd.DataFrame()

    @classmethod
    def set_headers(cls, headers: dict):
        cls.headers.update(headers)

    def _select_df(self) -> pd.DataFrame:
        try:
            df: pd.DataFrame = clickhouse_client.query_df(
                f"""
                SELECT *
                FROM "{self.name}"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )
        except Exception:
            df: pd.DataFrame = pd.DataFrame()
        return df

    def _check_account(self) -> None:
        if 'Authorization' not in self.headers or self.headers['Authorization'] is None or self.headers['Authorization'] == 'Bearer ':
            raise ValueError("Authorization header is not set. Run Account()")

    def payloads(self):
        raise NotImplemented

    def create_tasks(self, chunk_payloads: List[dict], session: aiohttp.ClientSession) -> List[asyncio.Task]:
        return [asyncio.create_task(
            self.post(session=session, payload=payload)
            ) for payload in chunk_payloads]

    @retry(
        exceptions=(TimeoutError, ConnectionError),
        tries=5,
        delay=100,
        logger=logger
    )
    async def execute_tasks(self, tasks: List[asyncio.Task]) -> bool:
        resAll: List[List[dict]] = await asyncio.gather(*(safe_task(task, 60) for task in tasks))
        result: List[dict] = [row for task_res in resAll for row in task_res]
        self._list_json.extend(result)
        logger.info(f"Chunk done {len(result)}" )
        return bool(result)

    @retry(
        exceptions=(TimeoutError, ConnectionError),
        tries=3,
        delay=600,
        logger=logger
    )
    async def send_requests(self, payloads: Iterable[List[dict]] = None) -> pd.DataFrame:
        if not payloads:
            payloads: Iterable[List[dict]] = self.payloads()
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=LIMIT,
                                               family=socket.AF_INET),
                trust_env=True,
                timeout=aiohttp.ClientTimeout(7200)
        ) as session:
            for chunk_payloads in payloads:
                tasks: List[asyncio.Task] = self.create_tasks(chunk_payloads, session)
                if tasks:
                    code: bool = await self.execute_tasks(tasks)
                    if not code and isinstance(self, RuDataPagesDF):
                        return pd.DataFrame(self._list_json)
            return pd.DataFrame(self._list_json)

    async def post(self, session, payload):
        async with self.semaphore, session.post(
                self.url,
                json=payload,
                headers=self.headers,
                timeout=60
        ) as response:
            if response.ok:
                try:
                    response_body = await response.json()
                    await asyncio.sleep(1.1)
                    return response_body or []
                except (
                    aiohttp.client_exceptions.ClientConnectorError,
                    asyncio.exceptions.TimeoutError,
                ) as e:
                    logger.exception(str(e))
                    raise ConnectionError("Restart")
            else:
                return []

    def get_sample(self, **kwargs) -> pd.DataFrame:
        df = self._select_df()
        if df.empty and kwargs:
            payload: dict = list(self.payloads())[0][0]
            for k, v in kwargs.items():
                payload[k] = v
            df: pd.DataFrame = asyncio.run(self.send_requests(payloads=[[payload]]))
        return df

    @property
    def df(self, **kwargs) -> pd.DataFrame:
        df = self._select_df()
        if df.empty:
            self._check_account()
            df: pd.DataFrame = asyncio.run(self.send_requests())
            df: pd.DataFrame = prepare_for_clickhouse(df.copy(), self.name)
            df['report_date'] = RuDataDF.report_date
            clickhouse_client.insert_df(self.name, df)
        self._df = df.loc[:, df.columns != 'report_date']
        return self._df

    @df.setter
    def df(self, value) -> None:
        self._df = value

class RuDataPagesDF(RuDataDF):
    pass
