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


def test_spark_config_apply_defaults_handles_config_error(monkeypatch, capsys):
    class BrokenConf:
        def get(self, key, default=None):
            raise RuntimeError("config unavailable")

    monkeypatch.setattr("servify.settings.config.flags.LOG_ENABLED", True)
    SparkConfig.apply_defaults_once(SimpleNamespace(conf=BrokenConf()))
    assert "Could not apply Spark defaults" in capsys.readouterr().out
