from __future__ import annotations
import asyncio
import aiohttp
import socket
import pandas as pd
from typing import List
from sqlalchemy.exc import SQLAlchemyError

from functions.get_date import last_day_month
from functions.clickhouse_client import client as clickhouse_client, prepare_for_clickhouse
from functions.retries import retry
from logger.Logger import Logger
from rudata import DocsAPI
from rudata.RuData import RuDataStrategy
from rudata.RuDataRequest import RuDataRequest
from rudata.Token import Token


logger = Logger()

class RuDataDF(RuDataStrategy):

    client = clickhouse_client
    report_date = pd.to_datetime(last_day_month)
    report_yearmonth: str = last_day_month.strftime("%Y%m")

    def __init__(self):
        self.url: str = ''
        self.name = self.__class__.__name__
        self._list_json: List[dict] = []
        self._df: pd.DataFrame = pd.DataFrame()

    def _select_df(self) -> pd.DataFrame:
        try:
            df: pd.DataFrame = self.client.query_df(
                f"""
                SELECT *
                FROM "{self.name}"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )
        except SQLAlchemyError:
            df: pd.DataFrame = pd.DataFrame()
        return df

    @property
    def df(self) -> pd.DataFrame:
        df = self._select_df()
        if df.empty:
            if Token.instance is None:
                RuDataRequest.set_headers()
            df: pd.DataFrame = asyncio.run(self.send_requests())
            df = prepare_for_clickhouse(df.copy())
            df['report_date'] = RuDataDF.report_date
            self.client.insert_df(self.name, df)
        self._df = df
        return self._df

    @df.setter
    def df(self, value) -> None:
        self._df = value

    def payloads(self):
        raise NotImplemented

    def create_tasks(self, chunk_payloads: List[dict], session: aiohttp.ClientSession) -> List[asyncio.Task]:
        return [asyncio.create_task(
            RuDataRequest(self.url, session).post(payload=payload)
        ) for payload in chunk_payloads]

    @retry(
        exceptions=(TimeoutError, ConnectionError, Exception),
        tries=5,
        delay=100,
        logger=logger
    )

    async def execute_tasks(self, tasks: List[asyncio.Task]) -> None:
        resAll: List[List[dict]] = [await task for task in asyncio.as_completed(tasks, timeout=60)]
        result: List[dict] = [row for task_res in resAll for row in task_res]
        if result:
            self._list_json.extend(result)
        else:
            return

    @retry(
        exceptions=(TimeoutError, ConnectionError, Exception),
        tries=3,
        delay=600,
        logger=logger
    )
    async def send_requests(self) -> pd.DataFrame:
        async with aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=DocsAPI.LIMIT,
                                               family=socket.AF_INET),
                trust_env=True,
                timeout=aiohttp.ClientTimeout(7200)
        ) as session:
            for chunk_payloads in self.payloads():
                tasks: List[asyncio.Task] = self.create_tasks(chunk_payloads, session)
                if not tasks:
                    await self.execute_tasks(tasks)
            return pd.DataFrame(self._list_json)
