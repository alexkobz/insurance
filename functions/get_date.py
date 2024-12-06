from datetime import date, timedelta
import pandas as pd
import requests


# for manual run change the varibale last_day_month: date = date(1970, 1, 1)
last_day_month: date = pd.to_datetime(date.today() - timedelta(days=28) + pd.offsets.MonthEnd(n=1))

def get_last_work_date_month() -> date:
    """
    try:
        url = f"https://xmlcalendar.ru/data/ru/{last_day_month.year}/calendar.txt"
    except:
        url = "https://raw.githubusercontent.com/szonov/data-gov-ru-calendar/master/calendar.csv"
    """
    last_day_month_copy = last_day_month
    try:
        url = f"https://xmlcalendar.ru/data/ru/{last_day_month.year}/calendar.txt"
        holidays = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/83.0.4103.97 Safari/537.36"
            }
        )
        holidays = pd.to_datetime(pd.Series(holidays.text.split())).tolist()
        while last_day_month_copy in holidays:
            last_day_month_copy -= timedelta(days=1)
    except:
        holidays = pd.read_csv("./data/Input/calendar.csv", header=None, sep=";")
        holidays = holidays.loc[holidays[0] == last_day_month.year, last_day_month.month].values[0].split(',')
        holidays = [int(day) for day in holidays]
        while last_day_month_copy.day in holidays:
            last_day_month_copy -= timedelta(days=1)
    return last_day_month_copy


last_day_month_str: str = last_day_month.strftime("%Y-%m-%d")
last_work_date_month: date = get_last_work_date_month()
last_work_date_month_str: str = last_work_date_month.strftime("%Y-%m-%d")
