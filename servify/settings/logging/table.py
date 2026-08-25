from typing import Optional

from loguru import logger as loguru_logger
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

__all__ = ["LOG_TABLE_COLUMNS", "LOG_TABLE_SCHEMA", "resolve_target_table"]

LOG_TABLE_COLUMNS = (
    "dgerdr_log DATE, hgerdr_log STRING, igerdr_log STRING, "
    "imsgem_log STRING, iarq_log STRING, ifncao_log STRING, "
    "nlin_log INT, iarq_princ_log STRING"
)


LOG_TABLE_SCHEMA = StructType(
    [
        StructField("dgerdr_log", DateType(), True),
        StructField("hgerdr_log", StringType(), True),
        StructField("igerdr_log", StringType(), True),
        StructField("imsgem_log", StringType(), True),
        StructField("iarq_log", StringType(), True),
        StructField("ifncao_log", StringType(), True),
        StructField("nlin_log", IntegerType(), True),
        StructField("iarq_princ_log", StringType(), True),
    ]
)


def resolve_target_table(spark) -> Optional[str]:

    # pylint: disable=import-outside-toplevel
    from servify.settings.config.flags import (
        PERSIST_LOG_TABLE_NAME,
        PERSIST_LOGS_CATALOG,
        PERSIST_LOGS_SCHEMA,
    )

    if not PERSIST_LOGS_CATALOG or not PERSIST_LOGS_SCHEMA:
        return None

    target = f"`{PERSIST_LOGS_CATALOG}.{PERSIST_LOGS_SCHEMA}.{PERSIST_LOG_TABLE_NAME}`"

    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target} ({LOG_TABLE_COLUMNS}) USING DELTA"
        )
        return target
    except Exception as e:
        loguru_logger.opt(exception=True).error(
            f"Failed to create or access the log table {target}. Error: {e}"
        )
        return None
