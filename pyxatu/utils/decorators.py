"""
Decorators for PyXatu functionality.
"""

import time
import logging
from functools import wraps
from typing import Any, Callable, TypeVar

F = TypeVar('F', bound=Callable[..., Any])

def retry_on_failure(max_retries: int = 1, initial_wait: float = 1.0, backoff_factor: float = 2.0) -> Callable[[F], F]:
    """Decorator to retry a function if an exception occurs."""
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempt = 0
            wait_time = initial_wait
            while attempt < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logging.warning(f"Attempt {attempt + 1}/{max_retries} failed: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    wait_time *= backoff_factor
                    attempt += 1
            logging.error(f"Max retries reached. Failed to complete operation.")
            return None
        return wrapper  # type: ignore
    return decorator