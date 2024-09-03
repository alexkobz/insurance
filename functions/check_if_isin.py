from re import compile as regex_compile


def check_if_isin(isin: str) -> bool:
    return regex_compile('([A-Z]{2})([A-Z0-9]{9})([0-9])$').match(isin) is not None
