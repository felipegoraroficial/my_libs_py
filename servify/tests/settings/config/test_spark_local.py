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


def test_spark_config_local_builder_options(monkeypatch):
    spark = SimpleNamespace(sparkContext=SimpleNamespace(setLogLevel=MagicMock()))
    builder = FakeBuilder(spark)
    monkeypatch.setattr(
        SparkConfig, "is_running_in_databricks", staticmethod(lambda: False)
    )
    monkeypatch.setattr(
        "servify.settings.config.spark_config.SparkSession",
        SimpleNamespace(builder=builder),
    )
    monkeypatch.setenv("SPARK_MASTER", "local[1]")
    result = SparkConfig.get_or_create_spark(
        app_name="test",
        enable_hive=True,
        packages="pkg:a:1",
        extra_confs={"custom.key": "value"},
    )
    assert result is spark
    assert ("master", "local[1]") in builder.calls
    assert ("config", "spark.jars.packages", "pkg:a:1") in builder.calls
    assert ("hive",) in builder.calls
    spark.sparkContext.setLogLevel.assert_called_once_with("WARN")
