import importlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from servify.tests.conftest import reader_chain

read_by_format_module = importlib.import_module(
    "servify.src.helpers.read.read_by_format"
)

read_by_format_fn = read_by_format_module.read_by_format


class Log:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_read_by_format_json_flatten_and_simple(monkeypatch, spark):
    spark = MagicMock()
    reader = reader_chain()
    spark.read = reader
    simple = MagicMock()
    simple.columns = ["value", "other"]
    reader.json.return_value = simple
    simple.withColumn.return_value = simple
    monkeypatch.setattr(
        read_by_format_module, "detectar_json_multiline", lambda path, log: False
    )
    assert (
        read_by_format_fn(
            spark, file_format="json", path_validado="local.json", log=Log()
        )
        is simple
    )

    nested = MagicMock()
    nested.columns = ["items"]
    nested.schema = {
        "items": SimpleNamespace(
            dataType=__import__("pyspark").sql.types.ArrayType(
                __import__("pyspark").sql.types.StructType([])
            )
        )
    }
    nested.withColumn.return_value = nested
    nested.select.return_value = nested
    reader.json.return_value = nested
    result = read_by_format_fn(
        spark, file_format="json", path_validado="file:/tmp/a.json", log=Log()
    )
    assert result is nested
