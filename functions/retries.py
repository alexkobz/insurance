import time
from functools import wraps
from logging import Logger
from typing import Callable, Optional, Union, Tuple, Type
import asyncio


def retry(
        exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
        tries: int = 3,
        delay: float = 1,
        logger=None,
):
    """
    Decorator to retry a function with specified parameters.

    Args:
        exceptions: Exception(s) to catch (can be a tuple of exceptions)
        tries: Maximum number of attempts
        delay: Initial delay between attempts in seconds
        logger: Optional logger to log retry attempts (e.g., logging.warning)
    """

    def decorator(func):
        if asyncio.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                remaining_tries, current_delay = tries, delay
                for attempt in range(1, remaining_tries + 1):
                    try:
                        if logger:
                            logger.info(f'attempt {attempt}')
                        res = await func(*args, **kwargs)
                        return res
                    except exceptions as e:
                        if attempt == remaining_tries:
                            logger.info(f'attempt {attempt}, {remaining_tries}')
                            raise
                        if logger:
                            logger.exception(f"Retrying {func.__name__} after {current_delay}s due to {type(e).__name__}: "
                                             f"{e} (attempt {attempt} of {tries})")
                        await asyncio.sleep(current_delay)
                return None
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                remaining_tries, current_delay = tries, delay
                for attempt in range(1, remaining_tries + 1):
                    try:
                        # logger.info('attempt', attempt)
                        res = func(*args, **kwargs)
                        return res
                    except exceptions as e:
                        if attempt == remaining_tries:
                            raise
                        if logger:
                            logger.exception(f"Retrying {func.__name__} after {current_delay}s due to {type(e).__name__}: "
                                             f"{e} (attempt {attempt} of {tries})")
                        time.sleep(current_delay)
                return None
            return sync_wrapper
    return decorator