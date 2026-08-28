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


def test_read_facade_partition_filters_and_validates_column(spark):
    frame = MagicMock()
    frame.isEmpty.return_value = False
    frame.columns = ["dt", "value"]
    selected = MagicMock()
    selected.collect.return_value = [{"max_partition": "2024-01-02"}]
    frame.select.return_value = selected
    frame.filter.return_value = frame
    helper = SimpleNamespace(
        resolve_accessible_path=lambda path, dbutils: path,
        read_by_format=lambda fmt, path: frame,
    )
    reader = fake_reader(spark, helper, SimpleNamespace())
    assert reader.read_data("data", "delta", partition_column="dt") is frame
    frame.columns = ["value"]
    with pytest.raises(ValueError, match="Partition column"):
        reader.read_data("data", "delta", partition_column="dt")
