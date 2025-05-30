from __future__ import annotations
import asyncio
import json

import aiohttp
import socket
import pandas as pd
from typing import List, Union, Optional, Dict, Any
from inspect import currentframe
from time import sleep
from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError

from functions.get_date import last_day_month
from functions.postgres_engine import engine as postrges_engine
from logger.Logger import Logger

from rudata import DocsAPI
from rudata.RuDataRequest import RuDataRequest
from rudata.Token import Token
from functions.divide_chunks import divide_chunks

logger = Logger()

class RuDataDF:

    engine = postrges_engine
    report_monthyear: str = last_day_month.strftime("%m%Y")

    def __init__(self, key=None):
        if key:
            self.key: str = key
            self._url: str = getattr(DocsAPI, key).url
            self._payload: Dict[str, Any] = getattr(DocsAPI, key)().payload()
            self._requestType: DocsAPI.RequestType = getattr(DocsAPI, key).requestType
            self._date = None
            self._ids: List[str | int] = []
            self._ids_key = None
            self._from = None
            self._to = None
            self._list_json: List[dict] = []
            self._df: pd.DataFrame = pd.DataFrame()


    # No usage
    def set_date(self, date=datetime.today().strftime("%Y-%m-%d")):
        caller_locals = currentframe().f_back.f_locals
        keys = [name for name, value in caller_locals.items() if date is value]
        for key in keys:
            if key in self._payload:
                self._payload[key] = date
        return self

    # No usage
    def set_from(self, from_=datetime.today().strftime("%Y-%m-%d")):
        caller_locals = currentframe().f_back.f_locals
        key = [name for name, value in caller_locals.items() if from_ is value][0]
        self._from = from_
        self._payload[key] = from_
        return self

    # No usage
    def set_to(self, to=datetime.today().strftime("%Y-%m-%d")):
        caller_locals = currentframe().f_back.f_locals
        key = [name for name, value in caller_locals.items() if to is value][0]
        self._to = to
        self._payload[key] = to
        return self

    # No usage
    def set_custom(self, value, key):
        self._payload[key] = value
        return self

    async def _get_df(self) -> pd.DataFrame:

        async def create_execute_tasks(payloads: List[dict]):
            async with aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=DocsAPI.LIMIT,
                                                   family=socket.AF_INET),
                    trust_env=True,
                    timeout=aiohttp.ClientTimeout(7200)
            ) as session:
                for chunk in divide_chunks(payloads, DocsAPI.LIMIT):
                    tasks: List[asyncio.Task] = [asyncio.create_task(
                        RuDataRequest(self._url, session).post(payload=payload)
                    ) for payload in chunk]
                    resAll: List[List[dict]] = [await task for task in asyncio.as_completed(tasks, timeout=600)]
                    result: List[dict] = [row for task_res in resAll for row in task_res]
                    if result or self._requestType not in (DocsAPI.RequestType.PAGES,
                                                            DocsAPI.RequestType.FintoolReferenceData):
                        self._list_json.extend(result)
                    else:
                        return

        async def get_response_ids(key: Union[str, int]) -> None:
            if isinstance(self._payload[key], list):
                # CompanyRatingsTable
                if self._payload[key] and isinstance(self._payload[key][0], DocsAPI.CompanyId):
                    ids: List[dict] = []
                    companyIdDict: Dict[str, Any] = self._payload[key][0].__dict__.copy()
                    for id in self._ids:
                        companyIdDict["id"] = id
                        ids.append(companyIdDict.copy())
                else:
                    ids: List[str | int] = self._ids.copy()
            else:
                raise "Ids is invalid"
            payloads: List[dict] = []
            payload: Dict[str, Any] = self._payload.copy()
            for id in divide_chunks(ids, 100):
                payload[key] = id
                payloads.append(payload.copy())
            await create_execute_tasks(payloads)

        def get_payloads_pages(payloadTMP: dict, key: str) -> List[dict]:
            pagers: List[dict] = []
            for pageNum in range(1, 10000):
                payloadTMP[key] = pageNum
                pagers.append(payloadTMP.copy())
            return pagers

        async def _pages():
            payloads: List[dict] = get_payloads_pages(self._payload.copy(), "pageNum")
            await create_execute_tasks(payloads)

        async def _fintool_reference_data():
            if isinstance(self._payload["pager"], DocsAPI.Pager):
                pagers = get_payloads_pages(self._payload["pager"].__dict__.copy(), "page")
                payloads = []
                payload = self._payload.copy()
                for pager in pagers:
                    payload["pager"] = pager
                    payloads.append(payload.copy())
                await create_execute_tasks(payloads)

        async def _fininstid():
            self._ids: List[int] = (
                pd.read_sql(
                    f"""
                    SELECT DISTINCT fininstid
                    FROM "Emitents"
                    WHERE report_monthyear = '{RuDataDF.report_monthyear}'
                    """
                    , self.engine
                )['fininstid']
                .to_list()
            )
            await get_response_ids("ids")

        async def _security_rating_table():
            self._ids: List[str] = (
                pd.read_sql(
                    f"""
                                SELECT DISTINCT isincode
                                FROM "FintoolReferenceData"
                                WHERE report_monthyear = '{RuDataDF.report_monthyear}'
                                """
                    , self.engine
                )['isincode']
                .to_list()
            )
            await get_response_ids("ids")

        async def _fintoolids():
            self._ids: List[int] = (
                pd.read_sql(
                    f"""
                    SELECT DISTINCT fintoolid
                    FROM "FintoolReferenceData"
                    WHERE report_monthyear = '{RuDataDF.report_monthyear}'
                    """
                    , self.engine
                )['fintoolid']
                .to_list()
            )
            await get_response_ids("fintoolIds")

        match self._requestType:
            case DocsAPI.RequestType.PAGES:
                await _pages()
            case DocsAPI.RequestType.FintoolReferenceData:
                await _fintool_reference_data()
            case DocsAPI.RequestType.FININSTID:
                await _fininstid()
            case DocsAPI.RequestType.SecurityRatingTable:
                await _security_rating_table()
            case DocsAPI.RequestType.FINTOOLIDS:
                await _fintoolids()
            case _:
                async with aiohttp.ClientSession(
                        connector=aiohttp.TCPConnector(limit=DocsAPI.LIMIT,
                                                       family=socket.AF_INET),
                        trust_env=True,
                        timeout=aiohttp.ClientTimeout(3600)
                ) as session:
                    result: List[Optional[dict]] = await RuDataRequest(self._url, session).post(self._payload)
                self._list_json.extend(result)

        return pd.DataFrame(self._list_json)

    @property
    @Logger.logDF
    def df(self) -> pd.DataFrame:
        try:
            df: pd.DataFrame = pd.read_sql(
                    f"""
                    SELECT *
                    FROM "{self.key}"
                    where report_monthyear = '{RuDataDF.report_monthyear}'
                    """
                    , RuDataDF.engine
                )
        except SQLAlchemyError:
            df: pd.DataFrame = pd.DataFrame()
        if df.empty:
            if Token.instance is None:
                RuDataRequest.set_headers()

            self._df: pd.DataFrame = asyncio.run(self._get_df())

            to_postgres_df = self._df.copy()
            for col in to_postgres_df.columns:
                if to_postgres_df[col].apply(lambda x: isinstance(x, list)).any():
                    to_postgres_df = pd.concat(
                        [to_postgres_df, pd.DataFrame([base[0] for base in to_postgres_df[col]])], axis=1
                    ).drop(columns=[col])
            to_postgres_df['report_monthyear'] = RuDataDF.report_monthyear
            to_postgres_df.to_sql(
                name=self.key,
                con=RuDataDF.engine,
                if_exists='append',
                index=False)
            sleep(1)
            return self._df
        else:
            return df.loc[:, df.columns != 'report_monthyear']

    @df.setter
    def df(self, value) -> None:
        self._df = value
