import os
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import OperationalError
from dotenv import load_dotenv
from src.utils.path import get_project_root, Path

env_path: Path = get_project_root() / '.venv' / '.env'
load_dotenv(env_path)

try:
    client = get_client(
        host=os.environ['CLICKHOUSE_HOST'],
        port=int(os.environ['CLICKHOUSE_PORT']),
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'])
except ConnectionRefusedError as e:
    print(e)
    print('Clickhouse is not running')
    raise
except OperationalError as e:
    print(e)
    client = get_client(
        host='localhost',
        port=int(os.environ['CLICKHOUSE_PORT']),
        username=os.environ['CLICKHOUSE_USER'],
        password=os.environ['CLICKHOUSE_PASSWORD'])

import pandas as pd
import numpy as np
import json


def prepare_for_clickhouse(
    df: pd.DataFrame,
    table: str,
    database: str = 'default',
) -> pd.DataFrame:
    """
    Cast pandas DataFrame строго под схему ClickHouse таблицы.
    Типы берутся из DESCRIBE TABLE.
    """
    df = df.copy()

    schema = client.query(
        f"DESCRIBE TABLE {database}.{table}"
    ).result_rows

    for col, ch_type, *_ in schema:
        if col not in df.columns:
            df[col] = None

        base_type = ch_type.replace('Nullable(', '').replace(')', '')

        # ---------- STRING ----------
        if base_type == 'String':
            df[col] = df[col].astype('string')

        # ---------- INTEGER ----------
        elif base_type in (
            'Int8','Int16','Int32','Int64',
            'UInt8','UInt16','UInt32','UInt64'
        ):
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].astype('Int64')

        # ---------- FLOAT ----------
        elif base_type in ('Float32', 'Float64'):
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # ---------- DATE ----------
        elif base_type == 'Date':
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # ---------- DATETIME ----------
        elif base_type.startswith('DateTime'):
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # ---------- BOOL (через UInt8) ----------
        elif base_type == 'Bool':
            df[col] = df[col].astype('boolean').astype('UInt8')

        # ---------- FALLBACK ----------
        else:
            df[col] = df[col].astype('string')

    # упорядочиваем колонки строго как в таблице
    ordered_cols = [c[0] for c in schema]
    df = df[ordered_cols]

    return df
