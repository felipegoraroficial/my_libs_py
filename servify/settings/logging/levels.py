from enum import Enum
from typing import Any, Callable, TypedDict, Union

__all__ = ["LEVEL_SEVERITY", "LogLevel", "HandlerConfig"]

LEVEL_SEVERITY = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}


class LogLevel(Enum):

    DEBUG = "\x1b[38;21m"
    INFO = "\x1b[38;21m"
    WARNING = "\x1b[38;21m"
    ERROR = "\x1b[38;21m"
    CRITICAL = "\x1b[38;21m"
    RESET = "\x1b[38;21m"


class HandlerConfig(TypedDict, total=False):

    sink: Callable[[Any], Any]
    format: Union[str, Callable[[dict[str, Any]], str]]
    colorize: bool
