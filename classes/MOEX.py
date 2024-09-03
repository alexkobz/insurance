import requests
import pandas as pd
from time import sleep
from rudata.RuDataDF import RuDataRequest


class MOEX:

    headers = {key: value for key, value in RuDataRequest.headers.items()
               if key in ("User-Agent", "content-type", "Accept")}

    def __init__(self):
        # Пишем функцию для получения SECID, основного режима торгов, рынка инструмента
        moex_answ = requests.get(
            'https://iss.moex.com/iss/index.json',
            headers=MOEX.headers
        )
        self.moex_index = moex_answ.json()
        self.bb_ind = self.moex_index['boards']['columns'].index('boardid')
        self.bm_ind = self.moex_index['boards']['columns'].index('market_id')
        self.all_boards = [self.moex_index['boards']['data'][i][self.bb_ind] for i in
                           range(len(self.moex_index['boards']['data']))]
        self.mi_ind = self.moex_index['markets']['columns'].index('id')
        self.me_ind = self.moex_index['markets']['columns'].index('trade_engine_name')
        self.mm_ind = self.moex_index['markets']['columns'].index('market_name')
        self.all_markets = [self.moex_index['markets']['data'][i][self.mi_ind] for i in
                            range(len(self.moex_index['markets']['data']))]

    def get_moex_params(self, isin):
        payload = {'q': isin}
        while True:
            try:
                moex_answ = requests.get('https://iss.moex.com/iss/securities.json', params=payload,
                                         headers=self.headers)
                moex_data = moex_answ.json()['securities']
                b_ind = moex_data['columns'].index('primary_boardid')
                id_ind = moex_data['columns'].index('secid')
                board = moex_data['data'][0][b_ind]
                secid = moex_data['data'][0][id_ind]
            except OSError:
                sleep(10)
                continue
            except:
                return
            break
        b_data = self.moex_index['boards']['data'][self.all_boards.index(board)]
        m_id = b_data[self.bm_ind]
        m_data = self.moex_index['markets']['data'][self.all_markets.index(m_id)]
        engine = m_data[self.me_ind]
        market = m_data[self.mm_ind]
        return engine, market, board, secid

    def get_currency(self, engine, market, board, secid):
        moex_answ = requests.get(f'https://iss.moex.com/iss/securities/{secid}.json', headers=self.headers)
        try:
            moex_data = moex_answ.json()['boards']
            en_ind = moex_data['columns'].index('engine')
            b_ind = moex_data['columns'].index('boardid')
            m_ind = moex_data['columns'].index('market')
            s_ind = moex_data['columns'].index('secid')
            cur_ind = moex_data['columns'].index('currencyid')
        except:
            return
        for row in moex_data['data']:
            if (row[en_ind] == engine) & (row[b_ind] == board) & (row[m_ind] == market) & (row[s_ind] == secid):
                return row[cur_ind]
        return

    # Функция для загрузки истории торгов
    def get_moex_prices(self, isin, first_date, last_date):
        engine, market, board, secid = self.get_moex_params(isin)
        payload = {'q': isin}
        moex_answ = requests.get(
            f'http://iss.moex.com/iss/history/engines/{engine}/markets/{market}/boards/{board}/securities/{secid}.json?from={first_date}&till={last_date}',
            params=payload, headers=self.headers)
        df = pd.DataFrame(columns=moex_answ.json()['history']['columns'], data=moex_answ.json()['history'][
            'data'])  # [['TRADEDATE','SHORTNAME','SECID','VALUE','WAPRICE','ACCINT','FACEVALUE','CURRENCYID']].dropna()
        return df
