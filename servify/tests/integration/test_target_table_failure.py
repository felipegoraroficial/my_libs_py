from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from servify.settings.logging.table import resolve_target_table
from servify.src.commons.shared.core import Shared_Commons
from servify.src.helpers.read.commons import obter_encoding as obter_encoding_fn
from servify.src.helpers.read.xslx.concat_ps_dfs import concat_ps_dfs


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


def test_target_table_sql_failure(monkeypatch):
    monkeypatch.setattr("servify.settings.config.flags.PERSIST_LOGS_CATALOG", "cat")
    monkeypatch.setattr("servify.settings.config.flags.PERSIST_LOGS_SCHEMA", "schema")
    spark = SimpleNamespace(
        sql=MagicMock(side_effect=RuntimeError("delta unavailable"))
    )
    assert resolve_target_table(spark) is None
