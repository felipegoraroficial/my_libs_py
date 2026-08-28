from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from pyspark.sql import types as T

from servify.src.helpers.read.commons.resolve_accessible_path import (
    resolve_accessible_path,
)
from servify.src.helpers.read.commons.resolve_latest_file import resolve_latest_file
from servify.src.helpers.read.commons.sample_bytes import sample_bytes
from servify.src.helpers.read.csv.detect_dominant_quote_char import (
    detect_dominant_quote_char,
)
from servify.src.helpers.read.csv.detect_escape_style import detect_escape_style
from servify.src.helpers.read.csv.detectar_delimitador import detectar_delimitador
from servify.src.helpers.read.csv.has_quoted_newline_from_text import (
    has_quoted_newline_from_text,
)
from servify.src.helpers.read.csv.line_quote_balance_stats import (
    line_quote_balance_stats,
)
from servify.src.helpers.read.json.detectar_json_multiline import (
    detectar_json_multiline,
)
from servify.src.helpers.read.xslx.list_xlsx_paths import list_xlsx_paths
from servify.src.helpers.read.xslx.sanitize_columns import sanitize_columns
from servify.src.helpers.read.xslx.schema_inference import infer_schema


class DummyLog:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


class DummyFs:
    def __init__(self, failures=()):
        self.failures = set(failures)
        self.calls = []

    def ls(self, path):
        self.calls.append(path)
        if path in self.failures:
            raise RuntimeError(path)
        return []


def test_xlsx_helpers_cover_paths_columns_and_schema(tmp_path, log):
    workbook = tmp_path / "book.xlsx"
    workbook.write_text("placeholder")
    other = tmp_path / "notes.txt"
    other.write_text("x")
    assert list_xlsx_paths(str(workbook), log=log) == [str(workbook)]
    assert list_xlsx_paths(str(tmp_path), log=log) == [str(workbook)]
    with pytest.raises(ValueError):
        list_xlsx_paths(str(other), log=log)
    with pytest.raises(FileNotFoundError):
        list_xlsx_paths(str(tmp_path / "missing"), log=log)

    schema = T.StructType([T.StructField("provided", T.StringType())])
    assert sanitize_columns(
        ["Name", None, "Name", 123, "a\nb"], prefer_from_schema=schema
    ) == ["Name", "_c2", "Name__1", "_c123", "a_b"]
    assert sanitize_columns(["", float("nan"), "x y", "a-b"]) == [
        "_c1",
        "_c2",
        "x_y",
        "ab",
    ]

    frame = pd.DataFrame(
        {
            "flag": pd.Series([True], dtype="bool"),
            "count": pd.Series([1], dtype="int64"),
            "ratio": pd.Series([1.0], dtype="float64"),
            "when": pd.to_datetime(["2024-01-01"]),
            "text": ["x"],
        }
    )
    inferred = infer_schema(frame)
    assert [field.dataType for field in inferred] == [
        T.BooleanType(),
        T.LongType(),
        T.DoubleType(),
        T.TimestampType(),
        T.StringType(),
    ]
