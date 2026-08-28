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


def test_read_facade_xlsx_and_table_paths():
    spark = MagicMock()
    frame = MagicMock()
    frame.isEmpty.return_value = False
    helper = SimpleNamespace(
        list_xlsx_paths=lambda path: ["a.xlsx"],
        concat_ps_dfs=lambda paths, schema: frame,
        read_by_format=lambda fmt, path: frame,
    )
    reader = fake_reader(spark, helper, SimpleNamespace())
    assert reader.read_data("folder", "xlsx") is frame

    spark.catalog.tableExists.return_value = False
    with pytest.raises(ValueError, match="does not exist"):
        reader.read_data("missing_table", "table")
    spark.catalog.tableExists.return_value = True
    assert reader.read_data("existing_table", "table") is frame
