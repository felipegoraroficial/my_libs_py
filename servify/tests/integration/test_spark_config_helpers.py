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


def test_spark_config_helpers_with_fake_spark(monkeypatch):
    class Conf:
        def __init__(self):
            self.values = {}

        def set(self, key, value):
            self.values[key] = value

        def get(self, key, default=None):
            return self.values.get(key, default)

    fake = SimpleNamespace(conf=Conf())
    SparkConfig.apply_performance_defaults(
        fake, shuffle_partitions=3, broadcast_threshold_mb=2
    )
    assert fake.conf.values["spark.sql.shuffle.partitions"] == "3"
    assert fake.conf.values["spark.databricks.delta.optimizeWrite.enabled"] == "true"
    SparkConfig.apply_defaults_once(fake)
    assert fake.conf.values["app.performance.defaults.applied"] == "true"
    before = dict(fake.conf.values)
    SparkConfig.apply_defaults_once(fake)
    assert fake.conf.values == before

    monkeypatch.setattr("servify.settings.config.spark_config.DBUtils", None)
    monkeypatch.setattr(
        "servify.settings.config.spark_config.dbutils", SimpleNamespace(), raising=False
    )
    assert SparkConfig.get_dbutils(fake) is not None
    monkeypatch.setattr(
        "servify.settings.config.spark_config.dbutils", None, raising=False
    )
    assert SparkConfig.get_dbutils(fake) is None
