from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
from pyspark.sql import types as T

from servify.settings.config import flags
from servify.settings.config.spark_config import SparkConfig
from servify.settings.logging.coercion import (
    coerce_log_value,
    coerce_to_date,
    coerce_to_float,
    coerce_to_int,
    coerce_to_string,
    coerce_to_timestamp,
)
from servify.src.helpers.read.csv.analisar_quote_for_path import analisar_quote_for_path
from servify.src.helpers.read.xslx.remove_header_rows import remove_header_rows


class Log:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_flags_update_and_restore(monkeypatch, capsys):
    monkeypatch.setattr(flags, "PERSIST_LOGS_CATALOG", None)
    monkeypatch.setattr(flags, "PERSIST_LOGS_SCHEMA", None)
    flags.set_logging(True)
    assert flags.LOG_ENABLED is True
    flags.set_persist_logs(True)
    assert flags.PERSIST_LOGS is False
    assert "Catalog and schema" in capsys.readouterr().out
    flags.set_persist_logs(True, catalog="cat", schema="schema")
    assert flags.PERSIST_LOGS is True
    flags.set_persist_log_min_level(" debug ")
    assert flags.PERSIST_LOG_MIN_LEVEL == "DEBUG"
