"""
https://docs.efir-net.ru/dh2/#/
"""

from typing import Type, List
from dataclasses import dataclass, field
from enum import Enum, IntEnum
from datetime import datetime as dt, timedelta

from functions.get_date import last_day_month_str, last_work_date_month, last_work_date_month_str


# Ограничение по запросам в секунду
LIMIT = 5


class RequestType(IntEnum):
    REGULAR = 1
    PAGES = 2
    FintoolReferenceData = 3
    INNS = 4
    CompanyGroups = 5
    FININSTID = 6
    SecurityRatingTable = 7
    CurrencyRate = 8
    FINTOOLIDS = 9
    ISIN = 10


class Request:

    @property
    def url(self) -> dict:
        return self.__dict__["url"]

    @property
    def requestType(self) -> dict:
        return self.__dict__["requestType"]

    def payload(self) -> dict:
        return {
            key.replace("_", ""): value
            for key, value in self.__dict__.items()
            if key not in ("url", "requestType")
        }


@dataclass
class Account(Request):
    """
    https://docs.efir-net.ru/dh2/#/Account/Login
    Авторизация пользователя. Получить авторизационный токен.
    """
    url: str = "https://dh2.efir-net.ru/v2/Account/Login"
    requestType: RequestType = RequestType.REGULAR
    login: str = ""
    password: str = ""


@dataclass
class ListScaleValues(Request):
    """
    https://docs.efir-net.ru/dh2/#/Rating/ListScaleValues?id=post-Listscalevalues
    Список шкал значений рейтингов
    """
    url: str = "https://dh2.efir-net.ru/v2/Rating/ListScaleValues"
    requestType: RequestType = RequestType.REGULAR
    filter: str = ""


@dataclass
class ExchangeTree(Request):
    """
    https://docs.efir-net.ru/dh2/#/Info/ExchangeTree?id=post-exchangetree
    Получить иерархию торговых площадок/источников, используемых Интерфакс
    """
    url: str = "https://dh2.efir-net.ru/v2/Info/ExchangeTree"
    requestType: RequestType = RequestType.PAGES
    filter: str = ""
    pageNum: int = 1
    pageSize: int = 300


@dataclass
class Pager:
    page: int
    size: int

    def __init__(self: Type["Pager"], obj: dict):
        self.page = obj["page"]
        self.size = obj["size"]


@dataclass
class FintoolReferenceData(Request):
    """
    https://docs.efir-net.ru/dh2/#/Info/FintoolReferenceData?id=post-fintoolreferencedata
    Получить расширенный справочник по финансовым инструментам.
    """
    url: str = "https://dh2.efir-net.ru/v2/Info/FintoolReferenceData"
    requestType: RequestType = RequestType.FintoolReferenceData
    id: str = ""
    fields: List[str] = field(default_factory=lambda: [])
    filter: str = ""
    pager: Type["Pager"] = field(default_factory=lambda: Pager({"page": 1, "size": 300}))


@dataclass
class Emitents(Request):
    """
    https://docs.efir-net.ru/dh2/#/Info/Emitents?id=post-emitents
    Получить краткий справочник по эмитентам.
    """
    url: str = "https://dh2.efir-net.ru/v2/Info/Emitents"
    requestType: RequestType = RequestType.PAGES
    filter: str = ""
    pageNum: int = 1
    pageSize: int = 300
    inn_as_string: bool = True


@dataclass
class OfferorsGuarants(Request):
    """
    https://docs.efir-net.ru/dh2/#/Bond/OfferorsGuarants?id=post-offerorsguarants
    Возвращает список гарантов/оферентов для инструмента
    """
    url: str = "https://dh2.efir-net.ru/v2/Bond/OfferorsGuarants"
    requestType: RequestType = RequestType.PAGES
    fintoolIds: List[int] = field(default_factory=lambda: [])
    date: str = last_day_month_str
    pageNum: int = 1
    pageSize: int = 100


@dataclass
class ListRatings(Request):
    """
    https://docs.efir-net.ru/dh2/#/Dictionary/ListRatings?id=post-listratings
    Список рейтингов
    """
    url: str = "https://dh2.efir-net.ru/v2/Rating/ListRatings"
    requestType: RequestType = RequestType.REGULAR
    filter: str = ""
    count: int = 10000000


@dataclass
class CompanyId:
    id: int
    idType: str

    def __init__(self: Type["CompanyId"], obj: dict):
        self.id = obj["id"]
        self.idType = obj["idType"]


@dataclass
class CompanyRatingsTable(Request):
    """
    https://docs.efir-net.ru/dh2/#/Rating/CompanyRatingsTable?id=post-companyratingstable
    Получить рейтинги нескольких компаний на заданную дату.
    """
    url: str = "https://dh2.efir-net.ru/v2/Rating/CompanyRatingsTable"
    requestType: RequestType = RequestType.FININSTID
    count: int = 10000000
    ids: List[CompanyId] = field(default_factory=lambda: [CompanyId({"id": 0, "idType": "FININSTID"})]) # Идентификатор эмитента
    date: str = last_day_month_str
    companyName: str = ""
    filter: str = ""


@dataclass
class SecurityRatingTable(Request):
    """
    https://docs.efir-net.ru/dh2/#/Rating/SecurityRatingTable?id=post-securityratingtable
    Получить рейтинги нескольких бумаг и связанных с ними компаний на заданную дату.
    """
    url: str = "https://dh2.efir-net.ru/v2/Rating/SecurityRatingTable"
    requestType: RequestType = RequestType.SecurityRatingTable
    count: int = 10000000
    ids: List[str] = field(default_factory=lambda: [])  # Идентификаторы бумаг – ISIN, рег.коды (обязательный).
    date: str = last_day_month_str


@dataclass
class CurrencyRate(Request):
    """
    https://docs.efir-net.ru/dh2/#/Archive/CurrencyRate?id=post-currencyrate
    Получить кросс-курс двух валют.
    """
    url: str = "https://dh2.efir-net.ru/v2/Archive/CurrencyRate"
    requestType: RequestType = RequestType.CurrencyRate
    from_: str = ""
    to: str = "RUB"
    date: str = last_day_month_str


@dataclass
class CurrencyRateHistory(Request):
    """
    https://docs.efir-net.ru/dh2/#/Archive/CurrencyRate?id=post-currencyrate
    Получить кросс-курс двух валют.
    """
    url: str = "https://dh2.efir-net.ru/v2/Archive/CurrencyRate"
    requestType: RequestType = RequestType.PAGES
    baseCurrency: str = "RUB"
    quotedCurrency: str = ""
    dateFrom: str = ""
    dateTo: str = last_day_month_str
    withHolidays: bool = True
    pageNum: int = 1
    pageSize: int = 100


@dataclass
class InfoSecurities(Request):
    """
    https://docs.efir-net.ru/dh2/#/Info/Securities?id=post-securities
    Получить краткий справочник по финансовым инструментам.
    Для акций метод возвращает только основные выпуски (по колонке SecurityKind).
    Для получения данных по дополнительным выпускам необходимо использовать метод FintoolRefrenceData.
    """
    url: str = "https://dh2.efir-net.ru/v2/Info/Securities"
    requestType: RequestType = RequestType.PAGES
    filter: str = ""
    pageNum: int = 1
    pageSize: int = 300


@dataclass
class AccruedInterestOnDate(Request):
    """
    https://docs.efir-net.ru/dh2/#/AccruedInterest/AccruedInterestOnDate?id=post-accruedinterestondate
    Расчет НКД на дату
    """
    url: str = "https://dh2.efir-net.ru/v2/AccruedInterest/AccruedInterestOnDate"
    requestType: RequestType = RequestType.FINTOOLIDS
    fintoolIds: List[int] = field(default_factory=lambda: [])  # Идентификаторы инструментов в базе Интерфакс
    cashFlowCalcDate: str = last_day_month_str  # Дата расчета. Необязательный.


@dataclass
class RUPriceHistory(Request):
    """
    https://docs.efir-net.ru/dh2/#/RuPrice/History
    Позволяет получить таблицу с историческими данными по одному или нескольким инструментам за заданный период времени.
    """
    url: str = "https://dh2.efir-net.ru/v2/RUPrice/History"
    requestType: RequestType = RequestType.PAGES
    ids: List[int] = field(default_factory=lambda: [])
    dateFrom: str = (dt.strptime(last_day_month_str, "%Y-%m-%d") - timedelta(days=30)).strftime("%Y-%m-%d")
    dateTo: str = last_day_month_str
    pageNum: int = 1
    pageSize: int = 1000


@dataclass
class EndOfDay(Request):
    """
    https://docs.efir-net.ru/dh2/#/Archive/EndOfDay?id=post-endofday
    Получить данные по результатам торгов на заданную дату.
    """
    url: str = "https://dh2.efir-net.ru/v2/Archive/EndOfDay"
    requestType: RequestType = RequestType.ISIN
    isin: str = ""
    date: str = last_day_month_str
    dateType: str = "LAST_TRADE_DATE"
    fields: List[str] = field(default_factory=lambda: [])


@dataclass
class Calendar(Request):
    """
    https://docs.efir-net.ru/dh2/#/Info/Calendar?id=post-calendar
    Возвращает календарь событий по инструментам за период.
    Не используется. Используется CalendarV2
    """
    url: str = "https://dh2.efir-net.ru/v2/Info/Calendar"
    requestType: RequestType = RequestType.PAGES
    isinIds: List[str] = field(default_factory=lambda: [])
    fintoolIds: List[int] = field(default_factory=lambda: [])
    eventTypes: List[str] = field(default_factory=lambda: [])
    fields: List[str] = field(default_factory=lambda: [])
    startDate: str = field(default_factory=lambda: "")
    endDate: str = last_day_month_str
    pageNum: int = 1
    pageSize: int = 1000


@dataclass
class CalendarV2(Request):
    """
    https://docs.efir-net.ru/dh2/#/Info/CalendarV2?id=post-calendarv2
    Возвращает календарь событий по инструментам за период.
    """
    url: str = "https://dh2.efir-net.ru/v2/Info/CalendarV2"
    requestType: RequestType = RequestType.PAGES
    fintoolIds: List[int] = field(default_factory=lambda: [])
    eventTypes: List[str] = field(default_factory=lambda: [])
    fields: List[str] = field(default_factory=lambda: [])
    startDate: str = field(default_factory=lambda: "")
    endDate: str = field(default_factory=lambda: "")
    pageNum: int = 1
    pageSize: int = 1000


@dataclass
class CouponsExt(Request):
    url: str = "https://dh2.efir-net.ru/v2/Bond/CouponsExt"
    requestType: RequestType = RequestType.PAGES
    filter: str = ""
    pageNum: int = 1
    pageSize: int = 300


@dataclass
class MoexSecurities(Request):
    """
    https://docs.efir-net.ru/dh2/#/Moex/Securities?id=post-securities
    Получить список торгуемых инструментов.
    """
    url: str = "https://dh2.efir-net.ru/v2/Moex/Securities"
    requestType: RequestType = RequestType.PAGES
    filter: str = ""
    pageNum: int = 1
    pageSize: int = 300


@dataclass
class FloaterData(Request):
    """
    https://docs.efir-net.ru/dh2/#/Bond/FloaterData?id=post-floaterdata
    Возвращает описания правил расчета ставок для бумаг с плавающей купонной ставкой
    """
    url: str = "https://dh2.efir-net.ru/v2/Bond/FloaterData"
    requestType: RequestType = RequestType.FINTOOLIDS
    fintoolIds: List[int] = field(default_factory=lambda: [])
    date: str = last_day_month_str
    showFuturePeriods: bool = True


@dataclass
class HistoryStockBonds(Request):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url: str = "https://dh2.efir-net.ru/v2/Moex/History"
    requestType: RequestType = RequestType.PAGES
    engine: str = "stock"
    market: str = "bonds"
    dateFrom: str = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
    dateTo: str = last_work_date_month_str
    pageNum: int = 1
    pageSize: int = 1000


@dataclass
class HistoryStockShares(Request):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url: str = "https://dh2.efir-net.ru/v2/Moex/History"
    requestType: RequestType = RequestType.PAGES
    engine: str = "stock"
    market: str = "shares"
    dateFrom: str = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
    dateTo: str = last_work_date_month_str
    pageNum: int = 1
    pageSize: int = 1000


@dataclass
class HistoryStockNdm(Request):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url: str = "https://dh2.efir-net.ru/v2/Moex/History"
    requestType: RequestType = RequestType.PAGES
    engine: str = "stock"
    market: str = "ndm"
    dateFrom: str = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
    dateTo: str = last_work_date_month_str
    pageNum: int = 1
    pageSize: int = 1000


@dataclass
class HistoryStockCcp(Request):
    """
    https://docs.efir-net.ru/dh2/#/Moex/History
    Получить официальные итоги по набору конкретных инструментов или по всем инструментам заданного рынка, группы режимов или одного режима торгов.
    """
    url: str = "https://dh2.efir-net.ru/v2/Moex/History"
    requestType: RequestType = RequestType.PAGES
    engine: str = "stock"
    market: str = "ccp"
    dateFrom: str = (last_work_date_month - timedelta(days=30)).strftime("%Y-%m-%d")
    dateTo: str = last_work_date_month_str
    pageNum: int = 1
    pageSize: int = 1000

    
@dataclass
class AffiliateTypes(Request):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/types
    Возвращает справочник типов аффилированности    
    """
    url: str = "https://dh2.efir-net.ru/v2/Affiliate/types"
    requestType: RequestType = RequestType.REGULAR


@dataclass
class CompanyGroupMembers(Request):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/CompanyGroupMembers
    Получить информацию о принадлежности компаний к группам компаний
    """
    url: str = "https://dh2.efir-net.ru/v2/Affiliate/CompanyGroupMembers"
    requestType: RequestType = RequestType.INNS
    memberInns: List[str] = field(default_factory=lambda: [])
    actualDate: str = last_day_month_str


@dataclass
class CompanyGroups(Request):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/CompanyGroups
    Получить состав групп по идентификаторам групп
    """
    url: str = "https://dh2.efir-net.ru/v2/Affiliate/CompanyGroups"
    requestType: RequestType = RequestType.CompanyGroups
    groupIds: List[int] = field(default_factory=lambda: [])
    actualDate: str = last_day_month_str
    pageNum: int = 1
    pageSize: int = 1000


@dataclass
class CompanyGroupRelations(Request):
    """
    https://docs.efir-net.ru/dh2/#/Affiliate/CompanyGroupRelations
    Возвращает описание отношений в группах компаний
    """
    url: str = "https://dh2.efir-net.ru/v2/Affiliate/CompanyGroupRelations"
    requestType: RequestType = RequestType.PAGES
    actualDate: str = last_day_month_str
    pageNum: int = 1
    pageSize: int = 100


@dataclass
class MoexStocks(Request):
    """
    https://docs.efir-net.ru/v2/Moex/Stocks
    Возвращает краткое описание ценных бумаг фондового рынка
    """
    url: str = "https://dh2.efir-net.ru/v2/Moex/Stocks"
    requestType: RequestType = RequestType.PAGES
    pageNum: int = 1
    pageSize: int = 300


@dataclass
class NsdCommonData(Request):
    """
    https://docs.efir-net.ru/v2/Moex/Stocks
    Возвращает краткое описание ценных бумаг фондового рынка
    """
    url: str = "https://dh2.efir-net.ru/v2/Nsd/CommonData"
    requestType: RequestType = RequestType.PAGES
    pageNum: int = 1
    pageSize: int = 100
