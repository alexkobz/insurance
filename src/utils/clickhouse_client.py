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


def prepare_for_clickhouse(df: pd.DataFrame):
    df = df.copy()
    ch_types = {}

    for col in df.columns:
        s = df[col]
        dtype = s.dtype

        # ---------- BOOL ----------
        if pd.api.types.is_bool_dtype(dtype):
            ch_types[col] = 'Nullable(UInt8)' if s.isna().any() else 'UInt8'
            df[col] = s.astype('UInt8')
            continue

        # ---------- INTEGER ----------
        if pd.api.types.is_integer_dtype(dtype):
            ch_types[col] = 'Nullable(Int64)'
            continue

        # ---------- FLOAT ----------
        if pd.api.types.is_float_dtype(dtype):
            ch_types[col] = 'Nullable(Float64)'
            continue

        # ---------- DATETIME ----------
        if pd.api.types.is_datetime64_any_dtype(dtype):
            ch_types[col] = 'Nullable(DateTime64(3))'
            df[col] = pd.to_datetime(s)
            continue

        # ---------- OBJECT ----------
        if pd.api.types.is_object_dtype(dtype):
            # detect actual content
            sample = s.dropna().iloc[0] if not s.dropna().empty else None

            if isinstance(sample, (dict, list)):
                ch_types[col] = 'Nullable(String)'
                df[col] = s.apply(
                    lambda x: json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (dict, list)) else None
                )
                continue

            if isinstance(sample, bool):
                ch_types[col] = 'Nullable(UInt8)'
                df[col] = s.apply(
                    lambda x: int(x) if isinstance(x, bool) else None
                )
                continue

            # default STRING
            ch_types[col] = 'Nullable(String)'
            df[col] = s.astype(str).where(s.notna(), None)
            continue

        # ---------- FALLBACK ----------
        ch_types[col] = 'Nullable(String)'
        df[col] = s.astype(str).where(s.notna(), None)

    return df
