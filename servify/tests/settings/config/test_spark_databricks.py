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


def test_spark_config_databricks_and_failure_paths(monkeypatch, capsys):
    context = SimpleNamespace(
        setLogLevel=MagicMock(side_effect=RuntimeError("blocked"))
    )
    spark = SimpleNamespace(sparkContext=context)
    monkeypatch.setattr(
        SparkConfig, "is_running_in_databricks", staticmethod(lambda: True)
    )
    monkeypatch.setattr(
        "servify.settings.config.spark_config.SparkSession",
        SimpleNamespace(getActiveSession=lambda: spark, builder=MagicMock()),
    )
    assert SparkConfig.get_or_create_spark(log_level="INFO") is spark
    assert "blocked" in capsys.readouterr().out

    builder = MagicMock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.side_effect = RuntimeError("bad gateway")
    monkeypatch.setattr(
        SparkConfig, "is_running_in_databricks", staticmethod(lambda: False)
    )
    monkeypatch.setattr(
        "servify.settings.config.spark_config.SparkSession",
        SimpleNamespace(builder=builder),
    )
    with pytest.raises(RuntimeError, match="bad gateway"):
        SparkConfig.get_or_create_spark()
