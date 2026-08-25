from typing import Optional

LOG_ENABLED = False

VALID_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

PERSIST_LOGS = False

PERSIST_LOG_MIN_LEVEL = "WARNING"

PERSIST_LOGS_CATALOG: Optional[str] = None

PERSIST_LOGS_SCHEMA: Optional[str] = None

PERSIST_LOG_TABLE_NAME = "servify_logs"


def set_logging(enabled: bool) -> None:
    global LOG_ENABLED
    LOG_ENABLED = enabled


def set_persist_logs(
    enabled: bool,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
) -> None:

    global PERSIST_LOGS, PERSIST_LOGS_CATALOG, PERSIST_LOGS_SCHEMA

    if catalog:
        PERSIST_LOGS_CATALOG = catalog
    if schema:
        PERSIST_LOGS_SCHEMA = schema

    if enabled and (not PERSIST_LOGS_CATALOG or not PERSIST_LOGS_SCHEMA):
        print(
            "[WARNING] Persisting logs is unable: Catalog and schema must be set."
            "use: sf.PERSIST_LOGS_CATALOG = 'my_catalog'; sf.PERSIST_LOGS_SCHEMA = 'my_schema'"
            "before enabling persisting logs."
        )

        PERSIST_LOGS = False
        return

    PERSIST_LOGS = enabled


def set_persist_log_min_level(level: str) -> None:

    normalized = str(level).strip().upper()

    if normalized not in VALID_LOG_LEVELS:
        raise ValueError(
            f"Invalid log level: {level}. Valid levels are: {', '.join(VALID_LOG_LEVELS)}"
        )
    global PERSIST_LOG_MIN_LEVEL
    PERSIST_LOG_MIN_LEVEL = normalized
