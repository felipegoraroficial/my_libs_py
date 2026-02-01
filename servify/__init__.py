from pyspark.sql import DataFrame

from .commons import servify_read

_reader: servify_read | None = None
_last_log_state: bool | None = None


def read_data(path: str, formato: str, **kwargs) -> DataFrame:
    global _reader, _last_log_state

    from .servify_configs import LOG_ENABLED  # pylint: disable=import-outside-toplevel

    if _reader is None or _last_log_state != LOG_ENABLED:
        _reader = servify_read(log_enabled=LOG_ENABLED)
        _last_log_state = LOG_ENABLED

    return _reader.read_data(path, formato, **kwargs)


__all__ = ["read_data", "servify_read"]
