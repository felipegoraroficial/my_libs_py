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


def test_shared_commons_lazy_log(monkeypatch, spark):
    monkeypatch.setattr("servify.settings.config.flags.LOG_ENABLED", False)
    shared = Shared_Commons(spark)
    first = shared.log
    assert first is shared.log
    monkeypatch.setattr("servify.settings.config.flags.LOG_ENABLED", True)
    assert shared.log is not first
