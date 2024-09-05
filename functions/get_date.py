from datetime import date as d, timedelta
import pandas as pd
import requests


# def get_date():
#     """
#     try:
#         url = f"https://xmlcalendar.ru/data/ru/{last_work_day_month.year}/calendar.txt"
#     except:
#         url = "https://raw.githubusercontent.com/szonov/data-gov-ru-calendar/master/calendar.csv"
#     """
#     last_work_day_month = pd.to_datetime(date.today() - timedelta(days=14) + pd.offsets.MonthEnd(n=1))
#     try:
#         url = f"https://xmlcalendar.ru/data/ru/{last_work_day_month.year}/calendar.txt"
#         holidays = requests.get(
#             url,
#             headers={
#                 "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) "
#                               "Chrome/83.0.4103.97 Safari/537.36"
#             })
#         holidays = pd.to_datetime(pd.Series(holidays.text.split())).tolist()
#         while last_work_day_month.day in holidays:
#             last_work_day_month -= timedelta(days=1)
#     except:
#         holidays = pd.read_csv("./data/Input/calendar.csv", header=None, sep=";")
#         holidays = holidays.loc[holidays[0] == last_work_day_month.year, last_work_day_month.month].values[0].split(',')
#         holidays = [int(day) for day in holidays]
#         while last_work_day_month.day in holidays:
#             last_work_day_month -= timedelta(days=1)
#     return last_work_day_month.strftime("%Y-%m-%d")


DATE = pd.to_datetime(d.today() - timedelta(days=14) + pd.offsets.MonthEnd(n=1)).strftime("%Y-%m-%d")
