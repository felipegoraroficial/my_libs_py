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


def test_encoding_supported_fallback_and_error(monkeypatch):
    module = __import__(
        "servify.src.helpers.read.commons.obter_encoding", fromlist=["x"]
    )
    monkeypatch.setattr(module, "sample_bytes", lambda path, sample_bytes, log: b"abc")
    monkeypatch.setattr(
        module.chardet, "detect", lambda raw: {"encoding": "ASCII", "confidence": 1.0}
    )
    assert module.obter_encoding("x", log=Log()) == "us-ascii"
    monkeypatch.setattr(module.chardet, "detect", lambda raw: {"encoding": "KOI8-R"})
    assert module.obter_encoding("x", log=Log()) == "utf-8"
    monkeypatch.setattr(
        module.chardet, "detect", MagicMock(side_effect=RuntimeError("bad"))
    )
    with pytest.raises(ValueError, match="Error detecting encoding"):
        module.obter_encoding("x", log=Log())
