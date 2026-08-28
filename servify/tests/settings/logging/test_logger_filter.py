from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from servify.settings.config.spark_config import SparkConfig
from servify.settings.logging import logger as logger_module
from servify.settings.logging.logger import Logger


class FakeBuilder:
    def __init__(self, spark):
        self.spark = spark
        self.calls = []

    def appName(self, value):
        self.calls.append(("appName", value))
        return self

    def master(self, value):
        self.calls.append(("master", value))
        return self

    def config(self, key, value):
        self.calls.append(("config", key, value))
        return self

    def enableHiveSupport(self):
        self.calls.append(("hive",))
        return self

    def getOrCreate(self):
        return self.spark


def test_logger_sink_filters_levels_and_persists(monkeypatch):
    spark = MagicMock()
    logger = Logger(spark, show_logs=False)
    record = {"level": SimpleNamespace(name="INFO")}
    message = SimpleNamespace(record=record)
    monkeypatch.setattr(logger_module, "tqdm", SimpleNamespace(write=MagicMock()))
    monkeypatch.setattr("servify.settings.config.flags.PERSIST_LOGS", True)
    monkeypatch.setattr("servify.settings.config.flags.PERSIST_LOG_MIN_LEVEL", "ERROR")
    logger._persist_log_spark = MagicMock()
    logger._log_sink(message)
    logger._persist_log_spark.assert_not_called()

    record["level"] = SimpleNamespace(name="ERROR")
    logger._log_sink(message)
    logger._persist_log_spark.assert_called_once_with(record)
