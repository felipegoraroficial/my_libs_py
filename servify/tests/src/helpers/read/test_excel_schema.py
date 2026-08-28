import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from pyspark.sql import types as T

from servify.src.commons.functions.read import servify_read
from servify.src.helpers.read.xslx.read_excel_with_pandas import read_excel_with_pandas


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


def test_read_excel_with_schema(monkeypatch, spark):
    import pandas as pd

    data = pd.DataFrame([["name", "age"], ["alice", "30"]])
    monkeypatch.setattr(pd, "read_excel", lambda *args, **kwargs: data)
    shared = MagicMock()
    shared.aplicar_schema_df.side_effect = lambda dataframe, target_schema: dataframe
    excel_module = importlib.import_module(
        "servify.src.helpers.read.xslx.read_excel_with_pandas"
    )
    monkeypatch.setattr(
        excel_module,
        "remove_header_rows",
        lambda dataframe, log: dataframe,
    )
    schema = T.StructType(
        [
            T.StructField("name", T.StringType()),
            T.StructField("age", T.StringType()),
        ]
    )
    result = read_excel_with_pandas(
        spark=spark,
        log=Log(),
        commons_shared=shared,
        xlsx_path="book.xlsx",
        schema=schema,
    )
    assert result.columns == ["name", "age", "source_file"]
    shared.aplicar_schema_df.assert_called_once()
