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


def test_logger_persistence_no_target_and_write_failure(monkeypatch):
    logger = Logger(MagicMock(), show_logs=False)
    logger._log_target_checked = True
    logger._persist_log_spark({"level": SimpleNamespace(name="ERROR")})
    assert logger._target_table is None

    logger._target_table = "table"
    logger._build_log_dataframe = MagicMock(
        return_value=SimpleNamespace(
            write=SimpleNamespace(
                mode=lambda value: SimpleNamespace(
                    saveAsTable=MagicMock(side_effect=RuntimeError("write failed"))
                )
            )
        )
    )
    logger._persist_log_spark(
        {
            "level": SimpleNamespace(name="ERROR"),
            "message": "message",
            "file": SimpleNamespace(name="file.py"),
            "function": "function",
            "line": 1,
        }
    )
    assert logger._target_table is None
