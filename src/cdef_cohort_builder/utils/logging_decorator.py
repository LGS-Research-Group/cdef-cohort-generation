import functools
from collections.abc import Callable
from typing import Any, TypeVar

from cdef_cohort_builder.logging_config import logger

T = TypeVar("T")


def log_processing(func: Callable[..., T]) -> Callable[..., T]:
    @functools.wraps(func)
    def wrapper(**kwargs: Any) -> T:
        register_name = func.__name__.replace("process_", "").upper()
        logger.debug(f"Starting {register_name} processing")
        logger.debug(f"Input kwargs for {register_name}: {kwargs}")

        try:
            result = func(**kwargs)
            logger.debug(f"Finished {register_name} processing")
            return result
        except Exception as e:
            logger.error(f"Error processing {register_name} data: {str(e)}")
            raise

    return wrapper
