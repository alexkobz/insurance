from pathlib import Path

def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


RATINGS_CSV = get_project_root() / "data" / "output" / "ratings.csv"
CASH_FLOW_CSV = get_project_root() / "data" / "output" / "cash_flows.csv"
STOCKS_DATA_CSV = get_project_root() / "data" / "output" / "stocks_data.csv"
PRICES_CSV = get_project_root() / "data" / "output" / "prices.csv"
CURRENCIES_CSV = get_project_root() / "data" / "output" / "currency.csv"
