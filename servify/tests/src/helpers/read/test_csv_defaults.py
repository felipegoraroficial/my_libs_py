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


def test_read_by_format_csv_defaults_and_metadata(monkeypatch, spark):
    spark = MagicMock()
    reader = reader_chain()
    spark.read = reader
    frame = MagicMock()
    reader.csv.return_value = frame
    frame.withColumn.return_value = frame
    monkeypatch.setattr(
        read_by_format_module,
        "analisar_quote_for_path",
        lambda path, log: {
            "needs_multiline": True,
            "quote_suggestion": '"',
            "escape_style": "backslash",
        },
    )

    result = read_by_format_fn(
        spark, file_format="csv", path_validado="local.csv", log=Log()
    )
    assert result is frame
    reader.csv.assert_called_once_with("local.csv")
    assert reader.option.call_count >= 7
    frame.withColumn.assert_called_once()
