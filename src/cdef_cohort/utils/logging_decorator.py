import functools
from collections.abc import Callable
from typing import Any, TypeVar

from cdef_cohort.logging_config import logger

T = TypeVar("T")


def log_processing(func: Callable[..., T]) -> Callable[..., T]:
    """
    A decorator that logs the start, finish, and
    any errors during the execution of a processing function.

    This decorator wraps the given function,
    adding logging statements before and after its execution.
    It logs the start of processing, the input parameters,
    successful completion, and any errors that occur.

    Args:
        func (Callable[..., T]): The processing function to be decorated.

    Returns:
        Callable[..., T]: A wrapper function that includes logging around the original function.

    Raises:
        Exception: Reraises any exception caught during the execution of the decorated function.

    Example:
        @log_processing
        def process_data(**kwargs):
            # Process data here
            pass
    """

    @functools.wraps(func)
    def wrapper(**kwargs: Any) -> T:
        """
        Wrapper function that adds logging to the decorated function.

        Args:
            **kwargs: Keyword arguments passed to the original function.

        Returns:
            T: The return value of the original function.

        Raises:
            Exception: Reraises any exception caught during execution.
        """
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
