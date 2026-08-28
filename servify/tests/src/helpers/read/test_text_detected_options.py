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


def test_read_by_format_text_detected_options(monkeypatch, spark):
    spark = MagicMock()
    reader = reader_chain()
    spark.read = reader
    frame = MagicMock()
    reader.csv.return_value = frame
    frame.withColumn.return_value = frame
    monkeypatch.setattr(
        read_by_format_module,
        "detectar_delimitador",
        lambda path, log: ";",
    )
    monkeypatch.setattr(
        read_by_format_module,
        "obter_encoding",
        lambda path, log: "latin-1",
    )
    monkeypatch.setattr(
        read_by_format_module,
        "analisar_quote_for_path",
        lambda path, log: {
            "needs_multiline": False,
            "quote_suggestion": "'",
            "escape_style": "csv_double",
        },
    )
    read_by_format_fn(
        spark, file_format="txt", path_validado="file:/tmp/a.txt", log=Log()
    )
    reader.csv.assert_called_once_with("file:/tmp/a.txt")
    assert ("sep", ";") in [call.args for call in reader.option.call_args_list]
    assert ("encoding", "latin-1") in [
        call.args for call in reader.option.call_args_list
    ]
