import asyncio
import pandas as pd
import requests
from typing import List, Union, Optional
from inspect import currentframe
from time import sleep
from datetime import datetime
from logger.Logger import Logger
from rudata import DocsAPI
from rudata.RequestProcessing import RequestProcessing
from rudata.Token import Token
from rudata.SavedDF import FintoolReferenceData, Emitents, Calendar
from functions.divide_chunks import divide_chunks


class RuDataResponse:
    headers: dict = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/39.0.2171.95 Safari/537.36',
        'content-type': 'application/json',
        'Accept': 'application/json',
    }

    def __init__(self, url="", payload=None, semaphore=asyncio.Semaphore(1)):
        if payload is None:
            payload = {}
        self.__url: str = url
        self.__payload: dict = payload
        self.__semaphore: asyncio.Semaphore = semaphore
        self._result: List[Optional[dict]] = []

    @staticmethod
    def set_headers() -> None:
        token: str = str(Token())
        RuDataResponse.headers["Authorization"] = 'Bearer ' + token

    @property
    async def result(self) -> List[Optional[dict]]:
        self._result: List[Optional[dict]] = await RequestProcessing(
            self.__url, self.__payload, RuDataResponse.headers, self.__semaphore
        ).response
        return self._result

    @result.setter
    def result(self, value) -> None:
        self._result = value


class RuDataDF:

    def __init__(self, key=None):
        if key:
            self.key = key
            self.__url = getattr(DocsAPI, key).url
            self.__requestType = getattr(DocsAPI, key).requestType
            self.__payload = getattr(DocsAPI, key)().payload()
            self.__date = None
            self.__ids = None
            self.__ids_key = None
            self.__from = None
            self.__to = None
            self._list_json = []
            self._df: pd.DataFrame = pd.DataFrame()

    def set_date(self, date=datetime.today().strftime("%Y-%m-%d")):
        caller_locals = currentframe().f_back.f_locals
        keys = [name for name, value in caller_locals.items() if date is value]
        for key in keys:
            if key in self.__payload:
                self.__payload[key] = date
        return self

    def set_from(self, from_=datetime.today().strftime("%Y-%m-%d")):
        caller_locals = currentframe().f_back.f_locals
        key = [name for name, value in caller_locals.items() if from_ is value][0]
        self.__from = from_
        self.__payload[key] = from_
        return self

    def set_to(self, to=datetime.today().strftime("%Y-%m-%d")):
        caller_locals = currentframe().f_back.f_locals
        key = [name for name, value in caller_locals.items() if to is value][0]
        self.__to = to
        self.__payload[key] = to
        return self

    def set_custom(self, value, key):
        self.__payload[key] = value
        return self

    def _get_df(self) -> pd.DataFrame:

        async def create_execute_tasks(payloads: List[dict]):
            limit = DocsAPI.LIMIT
            semaphore = asyncio.Semaphore(limit)
            list_chunks = list(divide_chunks(payloads, limit))
            for chunk in list_chunks:
                tasks = [asyncio.create_task(RuDataResponse(self.__url, payload, semaphore).result) for payload in
                         chunk]
                resAll = [await task for task in asyncio.as_completed(tasks, timeout=3600)]
                result = [row for task_res in resAll for row in task_res]
                if result or self.__requestType not in (DocsAPI.RequestType.PAGES,
                                                        DocsAPI.RequestType.FintoolReferenceData):
                    self._list_json.extend(result)
                else:
                    return

        def get_response_ids(key: Union[str, int]) -> None:
            if isinstance(self.__ids, list):
                pass
            elif isinstance(self.__ids, pd.Series):
                self.__ids = list(self.__ids)
            if isinstance(self.__payload[key], list):
                # CompanyRatingsTable
                if self.__payload[key] and isinstance(self.__payload[key][0], DocsAPI.CompanyId):
                    ids = []
                    companyIdDict = self.__payload[key][0].__dict__.copy()
                    for id in self.__ids:
                        companyIdDict["id"] = id
                        ids.append(companyIdDict.copy())
                else:
                    ids = self.__ids.copy()
            else:
                raise "Ids is invalid"
            ids = list(divide_chunks(ids, 100))
            payloads = []
            payload = self.__payload.copy()
            for id in ids:
                payload[key] = id
                payloads.append(payload.copy())
            asyncio.run(create_execute_tasks(payloads))

        def get_payloads_pages(payloadTMP: dict, key: Union[str, int]) -> List[dict]:
            pagers = []
            for pageNum in range(1, 10000):
                payloadTMP[key] = pageNum
                pagers.append(payloadTMP.copy())
            return pagers

        if self.__requestType == DocsAPI.RequestType.PAGES:
            payloads = get_payloads_pages(self.__payload.copy(), "pageNum")
            asyncio.run(create_execute_tasks(payloads))
        elif self.__requestType == DocsAPI.RequestType.FintoolReferenceData:
            if isinstance(self.__payload["pager"], DocsAPI.Pager):
                pagers = get_payloads_pages(self.__payload["pager"].__dict__.copy(), "page")
                payloads = []
                payload = self.__payload.copy()
                for pager in pagers:
                    payload["pager"] = pager
                    payloads.append(payload.copy())
                asyncio.run(create_execute_tasks(payloads))
        elif self.__requestType == DocsAPI.RequestType.CompanyRatingsTable:
            self.__ids = Emitents().get_fininst()
            get_response_ids("ids")
        elif self.__requestType == DocsAPI.RequestType.SecurityRatingTable:
            self.__ids = FintoolReferenceData().get_isin()
            get_response_ids("ids")
        elif self.__requestType == DocsAPI.RequestType.FINTOOLIDS:
            self.__ids = FintoolReferenceData().get_fintool()
            get_response_ids("fintoolIds")
        elif self.__requestType == DocsAPI.RequestType.CurrencyRate:
            currencies = pd.read_csv("../data/Input/currencies.csv",
                                     header=0,
                                     encoding='cp1251',
                                     sep=';'
                                     )["Currency"].to_list()
            for currency in currencies:
                payload = self.__payload.copy()
                payload['from'] = currency
                answer = requests.post(self.__url, json=payload, headers=RuDataResponse.headers)
                sleep(1)
                answer_json = answer.json()
                answer_json["from"] = currency
                answer_json["to"] = "RUB"
                self._list_json.append(answer_json)
        elif self.__requestType == DocsAPI.RequestType.ISIN:
            self.__ids = FintoolReferenceData().get_isin()
            payloads = []
            payload = self.__payload.copy()
            for id in self.__ids:
                payload["isin"] = id
                payloads.append(payload.copy())
            asyncio.run(create_execute_tasks(payloads))
        elif self.__requestType == DocsAPI.RequestType.REGULAR:
            result: List[Optional[dict]] = asyncio.run(RuDataResponse(self.__url, self.__payload).result)
            self._list_json.extend(result)
        return pd.DataFrame(self._list_json)

    @property
    @Logger.logDF
    def df(self) -> pd.DataFrame:
        if Token.instance is None:
            RuDataResponse().set_headers()

        self._df = self._get_df()

        if self.key == "FintoolReferenceData":
            FintoolReferenceData.instance = self._df
        elif self.key == "Emitents":
            Emitents.instance = self._df
        elif self.key == "CalendarV2":
            Calendar.instance = self._df

        sleep(1)
        return self._df

    @df.setter
    def df(self, value) -> None:
        self._df = value
