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


def test_resolve_accessible_path_dbutils_and_file_fallback(tmp_path):
    file_path = str(tmp_path / "data.csv")
    Path(file_path).write_text("x")

    dbutils = SimpleNamespace(fs=DummyFs())
    assert resolve_accessible_path(file_path, dbutils) == file_path

    dbutils = SimpleNamespace(fs=DummyFs(failures={file_path}))
    assert resolve_accessible_path(file_path, dbutils) == f"file:{file_path}"

    missing = str(tmp_path / "missing.csv")
    dbutils = SimpleNamespace(fs=DummyFs(failures={missing, f"file:{missing}"}))
    with pytest.raises(FileNotFoundError):
        resolve_accessible_path(missing, dbutils)

    wildcard = str(tmp_path / "*.csv")
    assert resolve_accessible_path(wildcard, dbutils) == wildcard
    with pytest.raises(FileNotFoundError):
        resolve_accessible_path(str(tmp_path / "*.json"), dbutils)
