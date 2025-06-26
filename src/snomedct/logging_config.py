"""Global logging configuration.

This module provides a simplified interface for configuring Python's
logging system.

Examples:
    Basic usage with default INFO level:
        >>> logger = get_logger(__name__)
        >>> logger.info("Application started")

    Setting specific log level for a module:
        >>> logger = get_logger(__name__, "DEBUG")
        >>> logger.debug("Detailed debugging information")

    Changing root logger level affects all inherited loggers:
        >>> import logging
        >>> logging.get_logger().setLevel("DEBUG")
        >>> logger = get_logger(__name__)  # Will inherit DEBUG level

"""

import functools
import logging
from collections.abc import Callable
from typing import ParamSpec, TypeVar

from rich.logging import RichHandler


def get_logger(module_name: str, level: str | None = None) -> logging.Logger:
    """Get logger for module_name using RichHandler.

    Args:
        module_name (str): Name of logger.
        level (str, optional): Set logging level for logger.

    Returns:
        logging.Logger: Logger instance

    TODO: Add check if function is called from dagster.

    """
    logger = logging.getLogger(module_name)
    if level is not None:
        logger.setLevel(level)

    if not logger.handlers:
        log_format = "%(message)s"
        logging.basicConfig(format=log_format, datefmt="[%X]", handlers=[RichHandler()])

    return logger


P = ParamSpec("P")
R = TypeVar("R")


def set_log_level(level: int | str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Set log level just for decorated function."""

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            logger = logging.getLogger(func.__module__)
            logger.warning("Setting logger to %r for %r", level, func)
            original_level = logger.getEffectiveLevel()
            logger.setLevel(level)
            try:
                return func(*args, **kwargs)
            finally:
                logging.getLogger().setLevel(original_level)
                logger.warning("Reset logger to %r after %r", original_level, func)

        return wrapper

    return decorator
