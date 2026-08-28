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


def test_read_facade_rejects_unknown_format():
    reader = fake_reader(MagicMock(), SimpleNamespace(), SimpleNamespace())
    with pytest.raises(ValueError, match="not support"):
        reader.read_data("x", "xml")
