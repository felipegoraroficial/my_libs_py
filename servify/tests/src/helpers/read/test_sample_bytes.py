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


def test_sample_bytes_reads_latest_file_and_wraps_errors(tmp_path, log):
    path = tmp_path / "sample.txt"
    path.write_bytes(b"abcdef")
    assert sample_bytes(str(path), sample_bytes=3, log=log) == b"abc"
    with pytest.raises(ValueError, match="Nao foi possivel|Não foi possível"):
        sample_bytes(str(tmp_path / "missing.txt"), sample_bytes=3, log=log)
