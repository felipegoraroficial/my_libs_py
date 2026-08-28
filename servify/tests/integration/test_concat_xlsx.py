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


def test_concat_xlsx_paths(monkeypatch):
    module = __import__("servify.src.helpers.read.xslx.concat_ps_dfs", fromlist=["x"])
    first = MagicMock()
    second = MagicMock()
    first.unionByName.return_value = second
    monkeypatch.setattr(
        module, "read_excel_with_pandas", MagicMock(side_effect=[first, second])
    )
    with pytest.raises(FileNotFoundError):
        module.concat_ps_dfs(
            spark=MagicMock(),
            log=Log(),
            lista_files=[],
            schema=None,
            commons_shared=MagicMock(),
        )
    assert (
        module.concat_ps_dfs(
            spark=MagicMock(),
            log=Log(),
            lista_files=["a"],
            schema=None,
            commons_shared=MagicMock(),
        )
        is first
    )
    module.read_excel_with_pandas.reset_mock()
    module.read_excel_with_pandas.side_effect = [first, second]
    assert (
        module.concat_ps_dfs(
            spark=MagicMock(),
            log=Log(),
            lista_files=["a", "b"],
            schema=None,
            commons_shared=MagicMock(),
        )
        is second
    )
