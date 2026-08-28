import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import types as T

from servify.src.commons.functions.read import servify_read
from servify.src.helpers.read.xslx.read_excel_with_pandas import read_excel_with_pandas
from servify.tests.conftest import fake_reader


class Log:
    show_logs = False

    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_read_facade_csv_and_empty_paths():
    spark = MagicMock()
    frame = MagicMock()
    frame.isEmpty.return_value = False
    helper = SimpleNamespace(
        resolve_accessible_path=lambda path, dbutils: "resolved.csv",
        read_by_format=lambda fmt, path: frame,
    )
    reader = fake_reader(spark, helper, SimpleNamespace())
    assert reader.read_data("input.csv", "csv") is frame

    frame.isEmpty.return_value = True
    with pytest.raises(ValueError, match="No data found"):
        reader.read_data("input.csv", "csv")
