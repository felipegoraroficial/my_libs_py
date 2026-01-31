import os
from collections.abc import Callable, Sequence
from datetime import datetime
from enum import Enum
from typing import Any, Optional, TypedDict, Union

from loguru import logger as loguru_logger
from pyspark.sql import Row
from pyspark.sql import SparkSession as GenericSparkSession
from tqdm import tqdm

from app.paths import EstaticPath


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


class Logger:

    def __init__(
        self,
        spark: GenericSparkSession,
        timezone: str = "America/Sao_Paulo",
        spark_log_path: Optional[Union[str, Sequence[str]]] = None,
    ):

        self.spark = spark
        self.timezone = timezone
        self.base_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} [{level}]  {module}.{function}:{line} - {message}"
        self._set_timezone()
        self._configure_logger()

        default_paths = [EstaticPath.monitor_log]

        if spark_log_path is None:
            self.spark_log_path = default_paths
        elif isinstance(spark_log_path, str):
            self.spark_log_path = [spark_log_path] + [
                p for p in default_paths if p != spark_log_path
            ]
        else:
            seen = set(spark_log_path)
            self.spark_log_path = list(spark_log_path) + [
                p for p in default_paths if p not in seen
            ]

    def _set_timezone(self) -> None:

        os.environ["TZ"] = self.timezone

    def _get_log_format(self, level_color: str) -> str:

        return f"{level_color}" + self.base_format + f"{LogLevel.RESET.value}"

    def _log_sink(self, message):

        record = message.record
        tqdm.write(str(message), end="\n")

        try:
            self._persist_log_spark(record)
        except Exception as e:
            raise ValueError(f"Failed to persist log to Spark: {e}") from e

    def _configure_logger(self) -> None:

        log_formats = {
            "DEBUG": self._get_log_format(LogLevel.DEBUG.value),
            "INFO": self._get_log_format(LogLevel.INFO.value),
            "WARNING": self._get_log_format(LogLevel.WARNING.value),
            "ERROR": self._get_log_format(LogLevel.ERROR.value),
            "CRITICAL": self._get_log_format(LogLevel.CRITICAL.value),
        }

        def custom_format(record: dict[str, Any]) -> str:

            return log_formats.get(record["level"].name, self.base_format)

        loguru_logger.remove()

        handlers: Sequence[dict[str, Any]] = [
            {
                "sink": self._log_sink,
                "format": custom_format,
                "colorize": True,
            }
        ]

        loguru_logger.configure(handlers=handlers)  # type: ignore[arg-type]

    def _resolve_target_table(self) -> Optional[str]:

        if not getattr(self, "spark_log_path", None):
            self._target_table = None
            return None

        for target in self.spark_log_path:
            parts = target.split(".")
            try:
                if len(parts) == 3:
                    catalog, schema, table = parts
                    query = f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '{table}'"
                    rows = self.spark.sql(query).collect()
                    exists = any(getattr(r, "tableName", None) == table for r in rows)

                elif len(parts) == 2:
                    schema, table = parts
                    query = f"SHOW TABLES IN `{schema}` LIKE '{table}'"
                    rows = self.spark.sql(query).collect()
                    exists = any(getattr(r, "tableName", None) == table for r in rows)

                else:
                    exists = self.spark.catalog.tableExists(target)

                if exists:
                    self._target_table = target
                    return target
            except Exception:
                continue
        self._target_table = None
        return None

    def _persist_log_spark(self, record: dict[str, Any]) -> None:

        if not getattr(self, "_target_table", None):
            return

        target_table = getattr(self, "_target_table", None)
        if not target_table:
            target_table = self._resolve_target_table()
            if not target_table:
                return

        log_row = Row(
            date_log=datetime.now().date(),
            time_log=datetime.now().isoformat(),
            level_log=record["level"].name,
            message_log=record["message"],
            filename_log=record["file"].name,
            function_log=record["function"],
            line_log=record["line"],
        )

        df = self.spark.createDataFrame([log_row])

        try:
            df.write.mode("append").saveAsTable(target_table)
        except Exception:
            self._target_table = None
            return

    @property
    def debug(self) -> Callable[..., None]:
        return loguru_logger.debug

    @property
    def info(self) -> Callable[..., None]:
        return loguru_logger.info

    @property
    def warning(self) -> Callable[..., None]:
        return loguru_logger.warning

    @property
    def error(self) -> Callable[..., None]:
        return loguru_logger.error

    @property
    def critical(self) -> Callable[..., None]:
        return loguru_logger.critical
