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


def test_read_excel_rejects_empty_workbook(monkeypatch, spark):
    import pandas as pd

    monkeypatch.setattr(
        pd,
        "read_excel",
        lambda *args, **kwargs: pd.DataFrame([[None, None]]),
    )
    with pytest.raises(ValueError, match="All rows are empty"):
        read_excel_with_pandas(
            spark=spark,
            log=Log(),
            commons_shared=MagicMock(),
            xlsx_path="empty.xlsx",
            schema=None,
        )
