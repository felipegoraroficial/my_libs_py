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


def test_coercion_converts_all_supported_values():
    moment = datetime(2024, 1, 2, 3, 4, 5)
    day = date(2024, 1, 2)
    assert coerce_to_string(moment) == "2024-01-02T03:04:05"
    assert coerce_to_string(day) == "2024-01-02"
    assert coerce_to_timestamp(moment) is moment
    assert coerce_to_timestamp(day) == moment.replace(hour=0, minute=0, second=0)
    assert coerce_to_timestamp("x") is None
    assert coerce_to_date(moment) == day
    assert coerce_to_date(day) is day
    assert coerce_to_date("x") is None
    assert coerce_to_int("10") == 10
    assert coerce_to_int("x") is None
    assert coerce_to_float("1.5") == 1.5
    assert coerce_to_float("x") is None

    assert coerce_log_value("x", T.StringType()) == "x"
    assert coerce_log_value(day, T.TimestampType()) == datetime(2024, 1, 2)
    assert coerce_log_value(moment, T.DateType()) == day
    assert coerce_log_value("2", T.IntegerType()) == 2
    assert coerce_log_value("2.5", T.DoubleType()) == 2.5
    assert coerce_log_value("x", T.ArrayType(T.StringType())) == "x"
    assert coerce_log_value(None, T.StringType()) is None
