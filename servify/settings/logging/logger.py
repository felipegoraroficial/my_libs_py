import inspect
import os
from collections.abc import Callable, Sequence
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

from loguru import logger as loguru_logger
from pyspark.sql import SparkSession as GenericSparkSession
from pyspark.sql.types import StructType
from tqdm import tqdm

from .coercion import coerce_log_value
from .levels import LEVEL_SEVERITY, LogLevel
from .table import LOG_TABLE_SCHEMA, resolve_target_table

__all__ = ["Logger"]


class Logger:

    def __init__(
        self,
        spark: GenericSparkSession,
        timezone: str = "America/Sao_Paulo",
        show_logs: bool = True,
    ):

        self.spark = spark
        self.timezone = timezone
        self._timezone_info = ZoneInfo(self.timezone)
        self.base_format = (
            "{extra[local_time]} [{level}]  {module}.{function}:{line} - {message}"
        )
        self.show_logs = show_logs
        self._set_timezone()
        self._configure_logger()

        self._target_schema: Optional[StructType] = None
        self._target_table: Optional[str] = None
        self._log_target_checked = False
        self.main_caller_cache: dict[str, str] = {}

    def _set_timezone(self) -> None:

        os.environ["TZ"] = self.timezone

    def _to_local_time(self, value: datetime) -> datetime:

        if value.tzinfo is None:
            return value.replace(tzinfo=self._timezone_info)

        return value.astimezone(self._timezone_info)

    def _get_log_format(self, level_color: str) -> str:

        return f"{level_color}{self.base_format}{LogLevel.RESET.value}"

    def _log_sink(self, message):
        # pylint: disable=import-outside-toplevel
        from servify.settings.config.flags import (
            LOG_ENABLED,
            PERSIST_LOG_MIN_LEVEL,
            PERSIST_LOGS,
        )

        record = message.record

        if LOG_ENABLED and self.show_logs:
            tqdm.write(str(message), end="\n")

        if not PERSIST_LOGS:
            return

        record_level = record["level"].name
        min_level = str(PERSIST_LOG_MIN_LEVEL).upper()
        min_severity = LEVEL_SEVERITY.get(min_level, LEVEL_SEVERITY["WARNING"])
        if LEVEL_SEVERITY.get(record_level, 0) < min_severity:
            return

        try:
            self._persist_log_spark(record)
        except Exception as e:
            import traceback

            tqdm.write(f"[Logger] Failed to persist log to Spark table: {e!r}")
            tqdm.write(traceback.format_exc())

    def _configure_logger(self) -> None:

        log_formats = {
            "DEBUG": self._get_log_format(LogLevel.DEBUG.value),
            "INFO": self._get_log_format(LogLevel.INFO.value),
            "WARNING": self._get_log_format(LogLevel.WARNING.value),
            "ERROR": self._get_log_format(LogLevel.ERROR.value),
            "CRITICAL": self._get_log_format(LogLevel.CRITICAL.value),
        }

        def custom_format(record: dict[str, Any]) -> str:

            record["extra"]["local_time"] = self._to_local_time(
                record["time"]
            ).strftime("%Y-%m-%d %H:%M:%S")
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

    def _get_main_caller(self, logged_file: Optional[str] = None) -> str:

        ignore_fragments = (
            os.path.join("src", "settings"),
            os.path.join("src", "commons"),
            os.sep + "loguru" + os.sep,
        )

        if logged_file and logged_file in self.main_caller_cache:
            return self.main_caller_cache[logged_file]

        try:
            for frame_info in inspect.stack():
                filename = frame_info.filename

                if "site-packages" in filename or "dist-packages" in filename:
                    continue

                if any(fragment in filename for fragment in ignore_fragments):
                    continue

                if os.sep + "servify" + os.sep in filename:
                    main_caller = os.path.basename(filename)
                    if logged_file:
                        self.main_caller_cache[logged_file] = main_caller
                    return main_caller
        except Exception:
            loguru_logger.opt(exception=True).debug(
                "Failed to determine main caller from stack frames"
            )

        fallback = logged_file or "unknown"
        if logged_file:
            self.main_caller_cache[logged_file] = fallback
        return fallback

    def _build_log_dataframe(
        self, target_table: str, log_values: dict[str, Any]
    ) -> Any:

        target_schema = self._target_schema
        if target_schema is None:
            try:
                target_schema = self.spark.table(target_table).schema
            except Exception:
                target_schema = LOG_TABLE_SCHEMA
            self._target_schema = target_schema

        row = tuple(
            coerce_log_value(log_values.get(field.name), field.dataType)
            for field in target_schema.fields
        )
        return self.spark.createDataFrame([row], schema=target_schema)

    def _persist_log_spark(self, record: dict[str, Any]) -> None:

        target_table = self._target_table

        if not target_table:
            if self._log_target_checked:
                return
            target_table = resolve_target_table(self.spark)
            self._target_table = target_table
            self._log_target_checked = True
            if not target_table:
                return

        local_now = self._to_local_time(datetime.now())

        log_values = {
            "dgerdr_log": local_now.date(),
            "hgerdr_log": local_now,
            "igerdr_log": record["level"].name,
            "imsgem_log": record["message"],
            "iarq_log": record["file"].name,
            "ifncao_log": record["function"],
            "nlin_log": record["line"],
            "iarq_princ_log": self._get_main_caller(record["file"].name),
        }

        try:
            df = self._build_log_dataframe(target_table, log_values)
        except Exception as exc:
            tqdm.write(f"[Logger] Failed to build log DataFrame: {exc}")

            self._target_table = None
            self._target_schema = None
            return

        try:
            df.write.mode("append").saveAsTable(target_table)
        except Exception as exc:
            tqdm.write(
                f"[Logger] Failed to persist log to Spark table '{target_table}': {exc}"
            )

            self._target_table = None
            self._target_schema = None
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
