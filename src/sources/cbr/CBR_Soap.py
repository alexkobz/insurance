import pandas as pd
from collections import OrderedDict
import xmltodict
import xml.etree.ElementTree as ET
from contextlib import suppress
from datetime import datetime as dt
from lxml import etree
from zeep import Client, xsd
from zeep.plugins import HistoryPlugin


class CBR_Soap:
    """
    Класс для загрузки любой статистики с сайта Банка России.
    - Для получения списка возможных операций (запросов) нужно вызвать функцию show_operations()
    - Для получения списка параметров для выбранного запроса нужно вызвать функцию show_arguments('название операции')
    - Для получения информации по выбранному запросу нужно вызвать get_data('название операции', аргументы, tag для парсинга xml).
    Если tag остается пустым, предполагается, что он такой же, как название операции. Работает для большинства запросов.
    - Предусмотрена отдельная функция для загрузки курса выбранной валюты: get_exchange_rates(dateFrom,dateTo,currency). Даты в формате string 'YYYY-MM-DD'.
    - Функция simple_transform запускается автоматически и служит для форматирования выходного DataFrame. Параметры форматирования для каждого запроса задаются в словаре tr_dict, имеющего следующую структуру (на примере ROISfix):
    'ROISfix':{'colsInit':['D0','R1W','R2W','R1M','R2M','R3M','R6M'],
                    'to_div_by_100':['R1W','R2W','R1M','R2M','R3M','R6M'],
                    'index':'D0',
                    'colsNew':['ROISfix 1W','ROISfix 2W','ROISfix 1M','ROISfix 2M','ROISfix 3M','ROISfix 6M']}
    Ключ словаря - название операции.
    'colsInit' - какие колонки в исходном DataFrame нужно оставить
    'to_div_by_100'- какие колонки представляют собой процентные ставки и их нужно поделить на 100
    'index' - какое название у колонки с датами, чтобы следать ее индексом
    'colsNew'-новые названия выходных колонок
    Если операция не перечислена в словаре, функция simple_transform не запускается и возвращается исходный DataFrame. Если не получилось обернуть загруженный XML в DataFrame, возвращается XML.
    """

    cbr_namespace = "http://web.cbr.ru/"
    url_daily = "http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"
    wsdl_url_daily = "http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx?wsdl"
    url_sec = "http://www.cbr.ru/secinfo/secinfo.asmx"
    wsdl_url_sec = "http://www.cbr.ru/secinfo/secinfo.asmx?wsdl"
    tr_dict = {'KeyRate': {'colsInit': ['DT', 'Rate'],
                           'colsNew': ['KeyRate'],
                           'index': 'DT',
                           'to_div_by_100': ['Rate']},
               'Ruonia': {'colsInit': ['D0', 'ruo'],
                          'colsNew': ['RUONIA'],
                          'index': 'D0',
                          'to_div_by_100': ['ruo']},
               'ROISfix': {'colsInit': ['D0', 'R1W', 'R2W', 'R1M', 'R2M', 'R3M', 'R6M'],
                           'to_div_by_100': ['R1W', 'R2W', 'R1M', 'R2M', 'R3M', 'R6M'],
                           'index': 'D0',
                           'colsNew': ['ROISfix 1W', 'ROISfix 2W', 'ROISfix 1M', 'ROISfix 2M', 'ROISfix 3M',
                                       'ROISfix 6M']},
               'MosPrime': {'colsInit': ['MP_Date', 'TON', 'T1W', 'T2W', 'T1M', 'T2M', 'T3M', 'T6M'],
                            'to_div_by_100': ['TON', 'T1W', 'T2W', 'T1M', 'T2M', 'T3M', 'T6M'],
                            'index': 'MP_Date',
                            'colsNew': ['MosPrime ON', 'MosPrime 1W', 'MosPrime 2W', 'MosPrime 1M', 'MosPrime 2M',
                                        'MosPrime 3M', 'MosPrime 6M']},
               }
    tags = {'GetCursDynamic': 'ValuteData'}

    def __init__(self):
        daily_history = HistoryPlugin()
        sec_history = HistoryPlugin()
        daily_client = Client(self.wsdl_url_daily, plugins=[daily_history])
        sec_client = Client(self.wsdl_url_sec, plugins=[sec_history])

        self.wsdl_info = {}
        self.wsdl_info.update(self._build_wsdl_info(daily_client, daily_history))
        self.wsdl_info.update(self._build_wsdl_info(sec_client, sec_history))

    @staticmethod
    def _map_xsd_type(xsd_type):
        if isinstance(xsd_type, (xsd.types.builtins.Date, xsd.types.builtins.DateTime)):
            return dt
        if isinstance(xsd_type, xsd.types.builtins.Boolean):
            return bool
        return str

    def _build_wsdl_info(self, client, history):
        wsdl_info = {}
        for service in client.wsdl.services.values():
            for port in service.ports.values():
                for op_name, operation in port.binding._operations.items():
                    params = OrderedDict()
                    body = operation.input.body
                    if body is not None:
                        if hasattr(body, "type") and hasattr(body.type, "elements"):
                            for name, element in body.type.elements:
                                params[name] = self._map_xsd_type(element.type)
                        elif hasattr(body, "elements"):
                            for name, element in body.elements:
                                params[name] = self._map_xsd_type(element.type)
                    wsdl_info[op_name] = {
                        'documentation': getattr(operation, "documentation", None),
                        'input': {'args': params},
                        'client': client,
                        'history': history,
                    }
        return wsdl_info

    def show_operations(self):
        for op in self.wsdl_info:
            print(op, self.wsdl_info[op]['documentation'])

    def show_arguments(self, operation):
        if operation not in self.wsdl_info.keys():
            raise KeyError("Operation not recognised:" + operation,
                           "Use function CBR_Soap.show_operations() to view all available operations")
        op_info = self.wsdl_info[operation]
        op_params = op_info['input'][next(iter(op_info['input']))]
        print('Operation %s requires following arguments: %s' % (operation, op_params))

    def make_params_dict(self, operation):
        if operation not in self.wsdl_info.keys():
            raise KeyError("Operation not recognised:" + operation,
                           "Use function CBR_Soap.show_operations() to view all available operations")
        op_info = self.wsdl_info[operation]
        op_params = op_info['input'][next(iter(op_info['input']))]

        if len(self.args) != len(op_params):
            raise Exception('Operation %s requires following arguments: %s' % (operation, op_params))

        params = OrderedDict()
        for i, param in enumerate(op_params):
            value = self.args[i]
            if op_params[param] is dt:
                if not isinstance(value, str):
                    value = value.strftime("%Y-%m-%d")
            if op_params[param] is bool:
                if isinstance(value, str):
                    value = value.strip().lower() in ("true", "1", "yes")
                else:
                    value = bool(value)
            params[param] = value
        return params

    def _call_operation(self, operation, params):
        op_info = self.wsdl_info[operation]
        client = op_info['client']
        history = op_info['history']
        if params:
            getattr(client.service, operation)(**params)
        else:
            getattr(client.service, operation)()
        envelope = history.last_received.get("envelope")
        if envelope is None:
            raise RuntimeError("SOAP response not available in history plugin")
        return etree.tostring(envelope, encoding="utf-8")

    def get_data(self, operation, *args, tag=""):
        """SOAP call to CBR backend"""

        name = operation[:-3] if operation[-3:] == "XML" else operation
        if name == 'GetCursDynamic':
            raise Exception("Use function CBR_Soap.get_exchange_rates() to download exchange rates")
        self.operation = operation
        self.args = args
        params = self.make_params_dict(self.operation)
        response_xml = self._call_operation(self.operation, params)

        if len(tag) > 0:
            name = tag
        elif name in self.tags.keys():
            name = self.tags[name]
        try:
            df = pd.read_xml(response_xml, xpath=f".//{name}/*")
        except ValueError:
            try:
                df = pd.read_xml(response_xml, xpath=f".//{name}")
            except ValueError:
                return response_xml
        name = operation[:-3] if operation[-3:] == "XML" else operation
        if name in self.tr_dict.keys():
            df = self.simple_transform(df, name)
        return df

    def get_exchange_rates(self, dateFrom, dateTo, currency):
        self.operation = 'GetCursDynamic'
        Vcode = CBR_Soap().get_data('EnumValutes', False).set_index('VcharCode').loc[currency, 'Vcode']
        self.args = [dateFrom, dateTo, Vcode]
        params = self.make_params_dict(self.operation)
        response_xml = self._call_operation(self.operation, params)
        df = pd.read_xml(response_xml, xpath=f".//ValuteData/*")
        df['Vcurs'] /= df.loc[0, 'Vnom']
        df['CursDate'] = df['CursDate'].apply(lambda x: dt.strptime(x[:10], "%Y-%m-%d"))
        df = df.loc[:, ['CursDate', 'Vcurs']].set_index('CursDate').sort_index()
        df.columns = [currency]
        df.index.rename('Date', inplace=True)
        return df

    def simple_transform(self, data, name):
        data = data.loc[:, self.tr_dict[name]['colsInit']].dropna()
        if 'to_div_by_100' in self.tr_dict[name].keys():
            data.loc[:, self.tr_dict[name]['to_div_by_100']] /= 100
        data.loc[:, self.tr_dict[name]['index']] = data.loc[:, self.tr_dict[name]['index']].apply(
            lambda x: dt.strptime(x[:10], "%Y-%m-%d"))
        data.set_index(self.tr_dict[name]['index'], inplace=True)
        data.columns = self.tr_dict[name]['colsNew']
        data.index.rename('Date', inplace=True)
        data.sort_index(inplace=True)
        return data

    def get_discounts(self, currency="RUB", date=""):
        self.operation = 'IDRepo' + currency + 'XML'
        self.args = [date]
        params = self.make_params_dict(self.operation)
        response_xml = self._call_operation(self.operation, params)
        root = ET.fromstring(response_xml)
        di = xmltodict.parse(root.findall(".//SRC")[0].text)
        cols = []
        for col in di['InfoDirectRepo' + currency]['head']['dt']:
            cols.append("-".join(col.values()))
        cols2 = ['ISIN', 'RegN', 'Name', 'Maturity', 'Price']
        cols2.extend([i + "_" + j for i in cols for j in ['Beg', 'Min', 'Max']])
        disc = pd.DataFrame(columns=cols2)
        for i in di['InfoDirectRepo' + currency]['item']:
            d = {'ISIN': i['@ISIN'], 'RegN': i['@RegN'], 'Name': i['@Em'],
                 'Maturity': dt.strptime(i['@DateRedemption'], "%d.%m.%Y")}
            with suppress(ValueError):
                d['Price'] = float(i['@Price_fnd'])
            for c in range(len(cols)):
                for j in ['Beg', 'Min', 'Max']:
                    with suppress(ValueError): d[cols[c] + '_' + j] = float(i['dt'][0]['@' + j]) / 100
            disc = pd.concat([disc, pd.DataFrame(index=[0], data=d)], axis=0)
        return disc
