import numpy as np
import pandas as pd
from datetime import datetime as dt


class Bond:
    """
    Класс для ванильных облигаций. Содержит ряд общих для всех облигаций функций.
    """

    def __init__(self, bond_cfs, common_data, date=dt.today().strftime("%Y-%m-%d")):

        self.bond_cfs = bond_cfs
        self.common_data = common_data
        if isinstance(date, dt):
            date = date.strftime("%Y-%m-%d")
        if isinstance(date, str):
            self.date = date

    def init_on_date(self):
        """
        Функция для расчета вспомогательных переменных при запуске расчета цены/спреда на новую дату
        """

        bonds_before_date = self.bond_cfs[self.bond_cfs.Date <= self.date]
        bonds_after_date = self.bond_cfs[self.bond_cfs.Date > self.date]

        # bondsCPN = bonds_before_date[bonds_before_date["Type"] == "CPN"]
        # CPNPrevDate = bondsCPN.loc[bondsCPN.groupby('ISIN').Date.idxmax(), "Date"]
        # CPNPrevRate = bondsCPN.loc[bondsCPN.groupby('ISIN').Rate.idxmax(), "Rate"]

        # рассчитываем процент ранее погашенного номинала
        isMTY = self.bond_cfs[self.bond_cfs["Type"] == "MTY"].groupby("ISIN").Rate.sum()
        rates = pd.Series(self.bond_cfs.ISIN.unique()).rename('ISIN').to_frame()\
            .merge(isMTY, how='left', left_on="ISIN", right_index=True).fillna(0).set_index("ISIN")
        rates = rates.merge(self.common_data[["ISIN", "FaceValue"]], how='left', left_index=True, right_index=True)
        # рассчитываем процент ранее непогашенного номинала
        rates["Nominal"] = rates["FaceValue"] * (1 - rates["Rate"])

        # # если у нас perpetual, то есть нет даты погашения, добавляем в конец прогнозных потоков погашение
        # bondsNoMTY = bonds_after_date[
        #     ~bonds_after_date.ISIN.isin(bonds_after_date[bonds_after_date.Type == 'MTY'].ISIN)]
        # tmpMTY = bondsNoMTY.groupby("ISIN").head(1)
        # MTYupdate = tmpMTY["ISIN"].map(paid_nominal_perc.set_index("ISIN")["NominalNotPaid"]).fillna(
        #     0).to_frame().rename(columns={"ISIN": "CF"})
        # bonds_after_date.update(MTYupdate)

        # обрезаем потоки по первую дату оферты
        bonds_after_date = bonds_after_date[bonds_after_date.Type.isin(['CALL', 'PUT'])] \
            .groupby("ISIN") \
            .head(1) \
            .reset_index(drop=True)

    def modify_cfs(self):
        """
        Корректируем поток для облигаций с индексируемым номиналом
        """

        # FIXME только Инфляционно-Индексируемый?
        # TransformedFintoolReferenceData.Principal_type.unique()
        # ['Постоянный', None, 'Амортизируемый', 'Частично-досрочный ABS',
        #  'Неизвестный', 'Амортизируемый (КД)', 'Структурный',
        #  'Защита капитала (100%)', 'Инфляционно-Индексируемый',
        #  'Инфляц-Индекс+Амортизация', 'Индексируемый', 'Капитализация',
        #  'Част-досроч (баз. актив)', 'Индексация по активу',
        #  'Индексац.(актив)+ Аморт.', 'Защита капитала (< 100%)']
        if self.A.common_data.copy().loc[self.isin, 'Principal_type'] == 'Инфляционно-Индексируемый' and \
                self.bond_cfs.shape[0] > 0:
            nom = self.A.common_data.copy().loc[self.isin, 'FaceValue']
            cpn_dates = self.bond_cfs[self.bond_cfs['Type'] == 'CPN'].index
            if len(cpn_dates) > 0:
                prev_cpn_dates = cpn_dates.insert(0, self.prev_c_date)[:-1]
            for i in range(len(self.bond_cfs.index)):
                date = self.bond_cfs.iloc[i, :].name
                paid_nom = self.paid_nominal_perc + self.bond_cfs.loc[self.bond_cfs['Type'] == 'MTY', 'Rate'].loc[
                                                    :date].sum()
                paid_nom = min(paid_nom, 1)
                if self.bond_cfs.iloc[i, :].loc['Type'] == 'CPN':
                    self.bond_cfs.iloc[i, self.bond_cfs.columns.get_loc('CF')] = \
                        self.bond_cfs.iloc[i, self.bond_cfs.columns.get_loc('Rate')] * nom * (1 - paid_nom) * \
                        (cpn_dates[cpn_dates.get_loc(date)] - prev_cpn_dates[cpn_dates.get_loc(date)]).days / 365
                else:
                    self.bond_cfs.iloc[i, self.bond_cfs.columns.get_loc('CF')] = \
                        self.bond_cfs.iloc[i, self.bond_cfs.columns.get_loc('Rate')] * nom * (1 - paid_nom)
        else:
            pass


class Floater(Bond):
    """
    Класс для анализа облигаций с плавающей ставкой и/или с индексируемым номиналом.
    """

    def __init__(self, bond_cfs, common_data, floater_data, date=dt.today().strftime("%Y-%m-%d")):
        super().__init__(bond_cfs, common_data, date=date)
        self.floater_data = floater_data

    def init_on_date(self):
        """
        Инициализация облигации на дату расчета цены/спреда
        """
        # Сначала инициализируем общие параметры облигации из материнского класса
        super().init_on_date()
        # if self.float_rate==True:#инициализация параметров облигации с плавающей ставкой
        # некоторые флоатеры содержат функцию вида макс(функция1,функция2) для расчета ставки купона. Например, максимум из инфляции и ВВП
        # по другим флоатерам функция меняется во времени: меняется спред или дюрация ставки.
        # В self.bases будет храниться информация по каждой такой функции
        self.bases = []
        # копия исходной информации из базы
        self.floater_data = self.A.floater_data.copy().loc[self.A.floater_data['ISIN'] == self.isin, :].reset_index(
            drop=True)
        # display(self.floater_data)
        for base in self.floater_data.index:  # цикл по каждой базе
            d = {}
            d['model_start'] = self.floater_data.loc[base, 'beg_period']  # дата начала действия базы
            d['model_end'] = self.floater_data.loc[base, 'end_period']  # дата окончания действия базы
            # премия к базовой ставке
            d['premium'] = self.floater_data.loc[base, 'premium_to_base']
            d['beg_or_end'] = self.floater_data.loc[base, 'start_point']
            self.bases.append(d)

    def modify_cfs(self):
        '''
        Главная функция расчета потоков по флоатеру
        '''
        if self.bond_cfs.shape[0] > 0:  # проверяем, что есть будущие потоки
            cpn_dates = self.bond_cfs[self.bond_cfs['Type'] == 'CPN'].index  # запоминаем даты купонов
            if len(cpn_dates) > 0:
                prev_cpn_dates = cpn_dates.insert(0, self.prev_c_date)[:-1]
            for i in range(len(self.bond_cfs.index)):  # пробегаемся по будущим денежным потокам
                date = self.bond_cfs.iloc[i, :].name
                # if self.float_rate==True:#определяем плавающую ставку, если требуется
                if self.bond_cfs.iloc[i, :].loc['Type'] == 'CPN':  # только если этот поток - купон
                    cpn_rates = [np.nan] * len(self.bases)
                    if self.floater_data.shape[0] > 0:
                        for base in self.bases:  # перебираем все базы
                            try:
                                if date <= base['model_start']: continue  # если формула еще не действует
                                if date > base[
                                    'model_end']: continue  # или перестала действовать, переходим к следующей базе (формуле)
                            except:
                                pass
                            if base['beg_or_end'] != 'E' and i == 0:
                                if self.bond_cfs['Rate'].iloc[i] > 0: self.prev_c_rate = self.bond_cfs['Rate'].iloc[i]
                                continue  # в этом случае купон уже известен
                            cpn_rates[self.bases.index(base)] = base['premium']
                        # ситуация, когда мы не смогли посчитать ставку купона.
                        # Может быть в случае, например, когда в базе данных на указанную дату нет валидной формулы расчета купона
                        if np.isnan(cpn_rates).all() == True: continue  # ;print('nan',date,cpn_rates)
                    elif i == 0:
                        if self.bond_cfs['Rate'].iloc[i] > 0: self.prev_c_rate = self.bond_cfs['Rate'].iloc[i]
                    # если у флоатера несколько баз, выбираем максимальную ставку (стандартная формула для флоатеров с несколькими базами)
                    cpn_rates.append(self.prev_c_rate)
                    fin_rate = np.nanmax(cpn_rates)
                nom = self.A.common_data.copy().loc[self.isin, 'FaceValue']
                paid_nom = self.paid_nominal_perc + self.bond_cfs.loc[self.bond_cfs['Type'] == 'MTY', 'Rate'].loc[
                                                    :date].sum()
                paid_nom = min(paid_nom, 1)
                if self.bond_cfs.iloc[i, :].loc['Type'] == 'CPN':
                    mask = (self.bond_cfs['Type'] == 'CPN') & (self.bond_cfs.index == date)
                    self.bond_cfs.loc[mask, 'Rate'] = fin_rate
                    # корректируем процент ранее уплаченного номинала
                    self.bond_cfs.iloc[i, self.bond_cfs.columns.get_loc('CF')] = fin_rate * nom * (1 - paid_nom) * \
                                                                                 (cpn_dates[cpn_dates.get_loc(date)] -
                                                                                  prev_cpn_dates[cpn_dates.get_loc(
                                                                                      date)]).days / 365
                elif self.A.common_data.copy().loc[self.isin, 'Principal_type'] == 'Инфляционно-Индексируемый':
                    self.bond_cfs.iloc[i, self.bond_cfs.columns.get_loc('CF')] = \
                        self.bond_cfs.iloc[i, self.bond_cfs.columns.get_loc('Rate')] * nom * (1 - paid_nom)
