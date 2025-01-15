from datetime import date, timedelta
from io import BytesIO

import pandas as pd
import requests

from rudata.RuDataDF import RuDataDF

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
    holidays = pd.read_sql(
        """
        SELECT holiday_date, holiday_year
        FROM holidays
        """
        , RuDataDF.engine
    )
    if last_day_month.year in holidays['holiday_year'].tolist():
        while last_day_month_copy in holidays['holiday_date']:
            last_day_month_copy -= timedelta(days=1)
    else:
        url = f"https://xmlcalendar.ru/data/ru/{last_day_month.year}/calendar.txt"
        holidays_request = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/83.0.4103.97 Safari/537.36"
            }
        )
        holidays = pd.read_csv(BytesIO(holidays_request.content), header=None).rename(columns={0: 'holiday_date'})
        holidays['holiday_date'] = pd.to_datetime(holidays['holiday_date'])
        holidays['holiday_year'] = holidays['holiday_date'].dt.year
        holidays.to_sql(
            name='holidays',
            con=RuDataDF.engine,
            if_exists='append',
            index=False)
        while last_day_month_copy in holidays['holiday_date']:
            last_day_month_copy -= timedelta(days=1)
    return last_day_month_copy


last_day_month_str: str = last_day_month.strftime("%Y-%m-%d")
last_work_date_month: date = get_last_work_date_month()
last_work_date_month_str: str = last_work_date_month.strftime("%Y-%m-%d")
