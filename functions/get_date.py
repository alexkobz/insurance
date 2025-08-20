from datetime import date, timedelta
from io import BytesIO

import pandas as pd
import requests

from functions.clickhouse_client import client as clickhouse_client

# for manual run change the varibale last_day_month: date = date(1970, 1, 1)
last_day_month: date = date.today().replace(day=1) - timedelta(days=1)
# last_day_month: date = date(2025, 7, 31)

def get_last_work_date_month() -> date:
    last_day_month_copy = last_day_month
    holidays = clickhouse_client.query_df(
        f"""
        SELECT holiday_date
        FROM holidays
        WHERE holiday_year = {last_day_month.year}
        """
    )
    if holidays.empty:
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
        clickhouse_client.insert_df('holidays', holidays)
    while last_day_month_copy in holidays['holiday_date']:
        last_day_month_copy -= timedelta(days=1)
    return last_day_month_copy


last_day_month_str: str = last_day_month.strftime("%Y-%m-%d")
last_work_date_month: date = get_last_work_date_month()
last_work_date_month_str: str = last_work_date_month.strftime("%Y-%m-%d")
