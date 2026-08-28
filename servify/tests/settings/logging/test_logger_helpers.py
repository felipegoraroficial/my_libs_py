from datetime import datetime
from types import SimpleNamespace

import pytest
from pyspark.sql import types as T

from servify.settings.logging.logger import Logger
from servify.settings.logging.table import resolve_target_table
from servify.src.commons.shared.apply_schema import aplicar_schema_df


class Log:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_logger_helpers_and_target_table(monkeypatch, spark):
    logger = Logger(spark, show_logs=False)
    now = datetime(2024, 1, 2, 3, 4, 5)
    assert logger._to_local_time(now).tzinfo is not None
    assert logger._get_log_format("X").endswith(logger.base_format + "\x1b[38;21m")
    assert logger._get_main_caller("known.py") == "logger.py"
    assert logger._get_main_caller("known.py") == "logger.py"
    assert callable(logger.debug)

    monkeypatch.setattr("servify.settings.config.flags.PERSIST_LOGS_CATALOG", None)
    assert resolve_target_table(spark) is None
    monkeypatch.setattr("servify.settings.config.flags.PERSIST_LOGS_CATALOG", "cat")
    monkeypatch.setattr("servify.settings.config.flags.PERSIST_LOGS_SCHEMA", "schema")
    fake_spark = SimpleNamespace(sql=lambda statement: None)
    assert resolve_target_table(fake_spark).startswith("`cat`.`schema`")
