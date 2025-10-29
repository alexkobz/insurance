import os
from pathlib import Path
from time import sleep
from typing import Dict, List
import pandas as pd
import requests
from dotenv import load_dotenv

from src.utils.divide_chunks import divide_chunks
from src.utils.get_date import (
    last_day_month_str,
    last_work_date_month,
    last_work_date_month_str,
    last_day_month,
    first_day_month_str,
)
from src.utils.path import get_project_root
from datetime import datetime as dt, timedelta
from src.sources.rudata.RuDataDF import RuDataDF, RuDataPagesDF, LIMIT


class Account(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Account/Login
    Авторизация пользователя. Получить авторизационный токен.
    """
    url: str = "https://dh2.efir-net.ru/v2/Account/Login"
    _instance = None

    def __init__(self):
        super().__init__()
        if self._instance.__initialized:
            return
        self._instance.__initialized = True
        self.__payload = self.payloads()
        response: Dict[str, str] = self.send_requests()
        self._token_str: str = response["token"]
        self.set_headers({"Authorization": "Bearer " + self._token_str})
        sleep(1)

    @staticmethod
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls, *args, **kwargs)
            cls._instance.__initialized = False
        return cls._instance

    def payloads(self) -> dict:
        env_path: Path = Path.joinpath(get_project_root(), '.venv/.env')
        load_dotenv(env_path)
        return {'login': os.environ["LOGIN"], 'password': os.environ["PASSWORD"]}

    def create_tasks(self, chunk_payloads, session):
        raise NotImplemented

    async def execute_tasks(self, tasks):
        raise NotImplemented

    def send_requests(self):
        return requests.post(self.url, json=self.__payload).json()

    @property
    def instance(self):
        return self._instance


class ExchangeTree(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Info/ExchangeTree?id=post-exchangetree
    Получить иерархию торговых площадок/источников, используемых Интерфакс
    """
    url = "https://dh2.efir-net.ru/v2/Info/ExchangeTree"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 300,
                } for i in range(LIMIT)
            ]


class Emitents(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Info/Emitents?id=post-emitents
    Получить краткий справочник по эмитентам.
    """
    url: str = "https://dh2.efir-net.ru/v2/Info/Emitents"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 300,
                    'filter': '',
                    'inn_as_string': True
                } for i in range(LIMIT)
            ]


class OfferorsGuarants(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Bond/OfferorsGuarants?id=post-offerorsguarants
    Возвращает список гарантов/оферентов для инструмента
    """
    url: str = "https://dh2.efir-net.ru/v2/Bond/OfferorsGuarants"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 100,
                    'fintoolIds': [],
                    'date': last_day_month_str
                } for i in range(LIMIT)
            ]

class CurrencyRateHistory(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Archive/CurrencyRate?id=post-currencyrate
    Получить кросс-курс двух валют.
    """
    url: str = "https://dh2.efir-net.ru/v2/Archive/CurrencyRate"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 100,
                    'dateFrom': '',
                    'dateTo': last_day_month_str,
                    'withHolidays': True,
                    'baseCurrency': 'RUB',
                    'quotedCurrency': '',
                } for i in range(LIMIT)
            ]

class ListScaleValues(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Rating/ListScaleValues?id=post-Listscalevalues
    Список шкал значений рейтингов
    """
    url: str = "https://dh2.efir-net.ru/v2/Rating/ListScaleValues"

    def payloads(self):
        yield [
            {
                'filter': '',
            }
        ]


class InfoSecurities(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Info/Securities?id=post-securities
    Получить краткий справочник по финансовым инструментам.
    Для акций метод возвращает только основные выпуски (по колонке SecurityKind).
    Для получения данных по дополнительным выпускам необходимо использовать метод FintoolRefrenceData.
    """
    url = "https://dh2.efir-net.ru/v2/Info/Securities"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 300,
                    'filter': ''
                } for i in range(LIMIT)
            ]


class RUPriceHistory(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/RuPrice/History
    Позволяет получить таблицу с историческими данными по одному или нескольким инструментам за заданный период времени.
    """
    url = "https://dh2.efir-net.ru/v2/RUPrice/History"

    def payloads(self):
        dateFrom = (dt.strptime(last_day_month_str, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
        dateTo = last_day_month_str
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 1000,
                    'ids': [],
                    'dateFrom': dateFrom,
                    'dateTo': dateTo
                } for i in range(LIMIT)
            ]


class CalendarV2(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Info/CalendarV2?id=post-calendarv2
    Возвращает календарь событий по инструментам за период.
    """
    url = "https://dh2.efir-net.ru/v2/Info/CalendarV2"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 1000,
                    'fintoolIds': [],
                    'eventTypes': [],
                    'fields': [],
                    'startDate': "",
                    'endDate': ""
                } for i in range(LIMIT)
            ]


class CouponsExt(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Bond/CouponsExt
    """
    url = "https://dh2.efir-net.ru/v2/Bond/CouponsExt"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 300,
                    'filter': ''
                } for i in range(LIMIT)
            ]


class MoexSecurities(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Moex/Securities?id=post-securities
    Получить список торгуемых инструментов.
    """
    url = "https://dh2.efir-net.ru/v2/Moex/Securities"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 300,
                    'filter': ''
                } for i in range(LIMIT)
            ]


class HistoryStockBonds(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url = "https://dh2.efir-net.ru/v2/Moex/History"

    def payloads(self):
        dateFrom = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
        dateTo = last_work_date_month_str
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 1000,
                    'engine': "stock",
                    'market': "bonds",
                    'dateFrom': dateFrom,
                    'dateTo': dateTo
                } for i in range(LIMIT)
            ]


class HistoryStockShares(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url = "https://dh2.efir-net.ru/v2/Moex/History"

    def payloads(self):
        dateFrom = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
        dateTo = last_work_date_month_str
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 1000,
                    'engine': "stock",
                    'market': "shares",
                    'dateFrom': dateFrom,
                    'dateTo': dateTo
                } for i in range(LIMIT)
            ]


class HistoryStockNdm(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url = "https://dh2.efir-net.ru/v2/Moex/History"

    def payloads(self):
        dateFrom = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
        dateTo = last_work_date_month_str
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 1000,
                    'engine': "stock",
                    'market': "ndm",
                    'dateFrom': dateFrom,
                    'dateTo': dateTo
                } for i in range(LIMIT)
            ]


class HistoryStockCcp(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url = "https://dh2.efir-net.ru/v2/Moex/History"

    def payloads(self):
        dateFrom = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
        dateTo = last_work_date_month_str
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 1000,
                    'engine': "stock",
                    'market': "ccp",
                    'dateFrom': dateFrom,
                    'dateTo': dateTo
                } for i in range(LIMIT)
            ]


class CompanyGroupRelations(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/CompanyGroupRelations
    Возвращает описание отношений в группах компаний
    """
    url = "https://dh2.efir-net.ru/v2/Affiliate/CompanyGroupRelations"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 100,
                    'actualDate': last_day_month_str
                } for i in range(LIMIT)
            ]


class MoexStocks(RuDataPagesDF):
    """
    https://docs.efir-net.ru/v2/Moex/Stocks
    Возвращает краткое описание ценных бумаг фондового рынка
    """
    url = "https://dh2.efir-net.ru/v2/Moex/Stocks"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 300,
                } for i in range(LIMIT)
            ]


class NsdCommonData(RuDataPagesDF):
    """
    """
    url = "https://dh2.efir-net.ru/v2/Nsd/CommonData"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 100,
                } for i in range(LIMIT)
            ]


class Multipliers(RuDataPagesDF):
    """
    https://docs.efir-net.ru/v2/Emitent/Multipliers
    Возвращает краткое описание ценных бумаг фондового рынка
    """
    url = "https://dh2.efir-net.ru/v2/Nsd/CommonData"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'pageNum': pageNum + i,
                    'pageSize': 100,
                } for i in range(LIMIT)
            ]


class FintoolReferenceData(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Info/FintoolReferenceData?id=post-fintoolreferencedata
    Получить расширенный справочник по финансовым инструментам.
    """
    url = "https://dh2.efir-net.ru/v2/Info/FintoolReferenceData"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'id': '',
                    'fields': [],
                    'filter': '',
                    'pager': {'page': pageNum + i, 'size': 300}
                } for i in range(LIMIT)
            ]


class ListRatings(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Dictionary/ListRatings?id=post-listratings
    Список рейтингов
    """
    url = "https://dh2.efir-net.ru/v2/Rating/ListRatings"

    def payloads(self):
        yield [
            {
                'filter': '',
                'count': 10000000,
            }
        ]


class CompanyRatingsTable(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Rating/CompanyRatingsTable?id=post-companyratingstable
    Получить рейтинги нескольких компаний на заданную дату.
    """
    url = "https://dh2.efir-net.ru/v2/Rating/CompanyRatingsTable"

    def payloads(self):
        fininstids: List[int] = (
            self.client.query_df(
                f"""
                SELECT DISTINCT fininstid
                FROM "Emitents"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )['fininstid']
            .to_list()
        )
        if not fininstids:
            raise ValueError('finintids must not be empty. Please check the Emitents table for data.')

        for chunk_fininstid in divide_chunks(fininstids, 100*LIMIT):
            yield [
                {
                    'count': 10000000,
                    'ids': [{"id": fininstid, "idType": "FININSTID"} for fininstid in chunk_fininstid[100*i:100*(i+1)]],
                    'date': last_day_month_str,
                    'companyName': '',
                    'filter': ''
                } for i in range(LIMIT)
            ]


class SecurityRatingTable(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Rating/SecurityRatingTable?id=post-securityratingtable
    Получить рейтинги нескольких бумаг и связанных с ними компаний на заданную дату.
    """
    url = "https://dh2.efir-net.ru/v2/Rating/SecurityRatingTable"

    def payloads(self):
        isins: List[int] = (
            self.client.query_df(
                f"""
                SELECT DISTINCT isincode
                FROM "FintoolReferenceData"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )['isincode']
            .to_list()
        )
        if not isins:
            raise ValueError('isins must not be empty. Please check the Emitents table for data.')

        for chunk_isins in divide_chunks(isins, 100*LIMIT):
            yield [
                {
                    'count': 10000000,
                    'ids': chunk_isins[100*i:100*(i+1)],
                    'date': last_day_month_str,
                } for i in range(LIMIT)
            ]


class CurrencyRate(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Archive/CurrencyRate?id=post-currencyrate
    Получить кросс-курс двух валют.
    """
    url = "https://dh2.efir-net.ru/v2/Archive/CurrencyRate"

    def payloads(self):
        currencies: List[str] = (
            self.client.query_df(
                f"""
                        SELECT DISTINCT currency
                        FROM currencies
                        """
            )['currency']
            .to_list()
        )
        if not currencies:
            raise ValueError('currencies must not be empty. Please check the currencies table for data.')

        for currency in currencies:
            yield {
                    'from': currency,
                    'to': 'RUB',
                    'date': last_day_month_str,
                }

    async def send_requests(self):
        currencies_rudata = []
        for payload in CurrencyRate().payloads():
            ans = requests.post(self.url, json=payload, headers=self.headers)
            result: dict = ans.json()
            result["currency"] = payload['from']
            currencies_rudata.append(result)
            sleep(1)
        return pd.DataFrame(currencies_rudata)


class AccruedInterestOnDate(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/AccruedInterest/AccruedInterestOnDate?id=post-accruedinterestondate
    Расчет НКД на дату
    """
    url = "https://dh2.efir-net.ru/v2/AccruedInterest/AccruedInterestOnDate"

    def payloads(self):
        fintoolids: List[int] = (
            self.client.query_df(
                f"""
                SELECT DISTINCT fintoolid
                FROM "FintoolReferenceData"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )['fintoolid']
            .to_list()
        )
        if not fintoolids:
            raise ValueError('fintoolids must not be empty. Please check the FintoolReferenceData table for data.')

        for chunk_fintoolids in divide_chunks(fintoolids, 100 * LIMIT):
            yield [
                {
                    'fintoolIds': chunk_fintoolids[100 * i:100 * (i + 1)],
                    'endDate': last_day_month_str,
                    'cashFlowCalcDate': last_day_month_str,
                } for i in range(LIMIT)
            ]


class FloatersOnPeriod(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/AccruedInterest/floaters-on-period
    Расчет НКД для флоатеров за период
    """
    url = "https://dh2.efir-net.ru/v2/AccruedInterest/floaters-on-period"

    def payloads(self):
        fintoolids: List[int] = (
            self.client.query_df(
                f"""
                SELECT DISTINCT fintoolId
                FROM "FloaterData"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )['fintoolId']
            .to_list()
        )
        if not fintoolids:
            raise ValueError('fintoolids must not be empty. Please check the FintoolReferenceData table for data.')
        for chunk_fintoolids in divide_chunks(fintoolids, 10 * LIMIT):
            yield [
                {
                    'fintoolIds': chunk_fintoolids[10 * i:10 * (i + 1)],
                    'beginDate': last_day_month_str,
                    'endDate': last_day_month_str,
                    'cashFlowCalcDate': last_day_month_str,
                    'pageSize': 100
                } for i in range(LIMIT)
            ]


class EndOfDay(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Archive/EndOfDay?id=post-endofday
    Получить данные по результатам торгов на заданную дату.
    """
    url = "https://dh2.efir-net.ru/v2/Archive/EndOfDay"

    def payloads(self):
        # isins = pd.read_excel('/Users/alexander/PycharmProjects/insurance_mine/data/Input/ISIN_072025.xlsx', dtype=str)['code_isin'].tolist()
        isins: List[str] = (
            self.client.query_df(
                f"""
                        SELECT DISTINCT isincode
                        FROM "FintoolReferenceData"
                        WHERE _partition_id = '{self.report_yearmonth}' 
                        AND tradesite IN 
                        (
                            170,
                            183,
                            193,
                            207,
                            285,
                            297,
                        )
                        """
            )['isincode']
            .to_list()
        )
        if not isins:
            raise ValueError('isins must not be empty. Please check the ISIN table for data.')

        for chunk_isins in divide_chunks(isins, LIMIT):
            yield [
                {
                    'isin': isin,
                    'date': first_day_month_str,
                    'dateType': 'LAST_TRADE_DATE',
                    'fields': [
                        "isin", "seccode", "secname", "name", "fintoolId", "id_iss", "id_trade_site",
                        "add_date", "update_date", "mat_date", "last_time",
                        "close", "last", "open", "high", "low", "mprice", "avge_prce", "bid", "ask",
                        "vol", "val", "val_usd", "vol_lots", "deal_acc",
                        "yield_bid", "yield_ask", "last_yield", "yield_agg", "yield_swa",
                        "duration", "pvbp", "convexity", "spread",
                        "boardid", "boardname", "exch", "currency"
                    ]
                } for isin in chunk_isins if isin
            ]


class EndOfDayOnExchanges(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Archive/EndOfDayOnExchanges?id=post-endofdayonexchanges
    Получить данные по результатам торгов на заданную дату.
    """
    url = "https://dh2.efir-net.ru/v2/Archive/EndOfDayOnExchanges"

    def payloads(self):
        isins: List[str] = (
            self.client.query_df(
                f"""
                SELECT DISTINCT isincode
                FROM "FintoolReferenceData"
                WHERE _partition_id = '{self.report_yearmonth}' AND
                    (
                    fintooltype = 'Облигация' AND (toDate(endmtydate) > today() - INTERVAL 1 YEAR OR endmtydate IS NULL) OR
                    fintooltype = 'Фонд' OR
                    fintooltype = 'Акция' OR
                    fintooltype = 'Депозитарная расписка' OR
                    fintooltype = 'Выпуск акции'
                    )
                """
            )['isincode']
            .to_list()
        )
        if not isins:
            raise ValueError('isins must not be empty. Please check the ISIN table for data.')

        for chunk_isins in divide_chunks(isins, 20 * LIMIT):
            yield [
                {
                    'codes': chunk_isins[20*i:20*(i+1)],
                    'dateFrom': (last_day_month - timedelta(days=30)).strftime("%Y-%m-%d"),
                    'dateTo': first_day_month_str,
                    'fields': [
                        "isin", "seccode", "secname", "name", "fintoolId", "id_iss", "id_trade_site",
                        "add_date", "update_date", "mat_date", "last_time",
                        "close", "last", "open", "high", "low", "mprice", "avge_prce", "bid", "ask",
                        "vol", "val", "val_usd", "vol_lots", "deal_acc",
                        "yield_bid", "yield_ask", "last_yield", "yield_agg", "yield_swa",
                        "duration", "pvbp", "convexity", "spread",
                        "boardid", "boardname", "exch", "currency"
                    ]
                } for i in range(LIMIT)
            ]


class FloaterData(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Bond/FloaterData?id=post-floaterdata
    Возвращает описания правил расчета ставок для бумаг с плавающей купонной ставкой
    """
    url = "https://dh2.efir-net.ru/v2/Bond/FloaterData"

    def payloads(self):
        fintoolids: List[int] = (
            self.client.query_df(
                f"""
                SELECT DISTINCT fintoolid
                FROM "FintoolReferenceData"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )['fintoolid']
            .to_list()
        )
        if not fintoolids:
            raise ValueError('fintoolids must not be empty. Please check the FintoolReferenceData table for data.')

        for chunk_fintoolids in divide_chunks(fintoolids, 100 * LIMIT):
            yield [
                {
                    'fintoolIds': chunk_fintoolids[100 * i:100 * (i + 1)],
                    'date': last_day_month_str,
                    'showFuturePeriods': True,
                } for i in range(LIMIT)
            ]


class AffiliateTypes(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/types
    Возвращает справочник типов аффилированности
    """
    url = "https://dh2.efir-net.ru/v2/Affiliate/types"

    def payloads(self):
        yield [{}]


class CompanyGroupMembers(RuDataDF):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/CompanyGroupMembers
    Получить информацию о принадлежности компаний к группам компаний
    """
    url = "https://dh2.efir-net.ru/v2/Affiliate/CompanyGroupMembers"

    def payloads(self):
        inns: List[int] = (
            self.client.query_df(
                f"""
                SELECT DISTINCT inn
                FROM "Emitents"
                WHERE _partition_id = '{self.report_yearmonth}'
                """
            )['inn']
            .to_list()
        )
        if not inns:
            raise ValueError('inns must not be empty. Please check the Emitents table for data.')

        for chunk_inns in divide_chunks(inns, LIMIT):
            yield [
                {
                    'memberInns': chunk_inns[i],
                    'actualDate': last_day_month_str,
                } for i in range(LIMIT)
            ]


class CompanyGroups(RuDataPagesDF):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/CompanyGroups
    Получить состав групп по идентификаторам групп
    """
    url = "https://dh2.efir-net.ru/v2/Affiliate/CompanyGroups"

    def payloads(self):
        for pageNum in range(1, 10_000, LIMIT):
            yield [
                {
                    'groupIds': [],
                    'actualDate': last_day_month_str,
                    'pageNum': pageNum + i,
                    'pageSize': 1000,
                } for i in range(LIMIT)
            ]
