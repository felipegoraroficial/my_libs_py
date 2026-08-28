import pytest

import servify
from servify.settings.config import flags


def test_flags_validate_and_normalize_values(capsys):
    original = (
        flags.LOG_ENABLED,
        flags.PERSIST_LOGS,
        flags.PERSIST_LOG_MIN_LEVEL,
        flags.PERSIST_LOGS_CATALOG,
        flags.PERSIST_LOGS_SCHEMA,
    )
    try:
        flags.set_logging(True)
        assert flags.LOG_ENABLED is True
        flags.set_persist_log_min_level(" error ")
        assert flags.PERSIST_LOG_MIN_LEVEL == "ERROR"
        flags.set_persist_logs(True)
        assert flags.PERSIST_LOGS is False
        assert "Catalog and schema" in capsys.readouterr().out
        flags.set_persist_logs(True, catalog="catalog", schema="schema")
        assert flags.PERSIST_LOGS is True
        with pytest.raises(ValueError, match="Invalid log level"):
            flags.set_persist_log_min_level("verbose")
    finally:
        (
            flags.LOG_ENABLED,
            flags.PERSIST_LOGS,
            flags.PERSIST_LOG_MIN_LEVEL,
            flags.PERSIST_LOGS_CATALOG,
            flags.PERSIST_LOGS_SCHEMA,
        ) = original
