import os

from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import OperationalError
from dotenv import load_dotenv
from functions.path import get_project_root, Path

env_path: Path = Path.joinpath(get_project_root(), '.venv/.env')
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

def get_type_map():
    return {
        'bool': ('UInt8', lambda s: s.astype(np.uint8)),
        'int8': ('Int8', lambda s: s),
        'int16': ('Int16', lambda s: s),
        'int32': ('Int32', lambda s: s),
        'int64': ('Int64', lambda s: s),
        'uint8': ('UInt8', lambda s: s),
        'uint16': ('UInt16', lambda s: s),
        'uint32': ('UInt32', lambda s: s),
        'uint64': ('UInt64', lambda s: s),
        'float32': ('Float32', lambda s: s),
        'float64': ('Float64', lambda s: s),
        'category': ('String', lambda s: s.astype(str)),
        'datetime64[ns]': ('DateTime', lambda s: s),
        'object': ('String', lambda s: s.apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x)
        )),
    }

def prepare_for_clickhouse(df: pd.DataFrame):
    type_map = get_type_map()
    df_converted = pd.DataFrame()
    ch_types = {}

    for col in df.columns:
        dtype = str(df[col].dtype)

        # Special fallback for pandas treating dicts/lists as "object"
        if dtype == 'object':
            # Check first non-null value
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else ''
            if isinstance(sample, (dict, list)):
                dtype = 'object'  # handled by mapping
            else:
                dtype = 'object'

        # Map dtype → (ClickHouse type, conversion function)
        ch_type, convert_func = type_map.get(dtype, ('String', lambda s: s.astype(str)))
        df_converted[col] = convert_func(df[col])
        ch_types[col] = ch_type

    return df_converted
