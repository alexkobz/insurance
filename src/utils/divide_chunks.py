from typing import Iterable, Any


def divide_chunks(l: list, n: int) -> Iterable[Any]:
    """
    Функция для разрезания списка (напр, ISIN) на равные куски
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]
