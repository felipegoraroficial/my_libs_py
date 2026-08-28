import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

os.environ["PYTEST_RUNNING"] = "1"
os.environ.setdefault(
    "JAVA_HOME", r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("servify-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class SilentLog:
    show_logs = False

    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


@pytest.fixture
def log():
    return SilentLog()


def reader_chain():
    reader = MagicMock()
    reader.option.return_value = reader
    return reader


def fake_reader(spark, helper, dbutils):
    from servify.settings.config import flags
    from servify.src.commons.functions.read import servify_read

    reader = servify_read(spark=spark, log_enabled=False)
    reader._log = SilentLog()
    helper.log = SilentLog()
    helper.log.show_logs = flags.LOG_ENABLED
    reader._helper = helper
    reader.settings = SimpleNamespace(require_dbutils=lambda value: dbutils)
    return reader
