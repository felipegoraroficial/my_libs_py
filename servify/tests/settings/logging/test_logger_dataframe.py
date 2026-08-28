from datetime import datetime
from types import SimpleNamespace

import pytest
from pyspark.sql import types as T

from servify.settings.logging.logger import Logger
from servify.settings.logging.table import resolve_target_table
from servify.src.commons.shared.apply_schema import aplicar_schema_df


class Log:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        pass

    def error(self, message, **kwargs):
        pass


def test_logger_builds_log_dataframe(spark):
    logger = Logger(spark, show_logs=False)
    logger._target_schema = T.StructType(
        [
            T.StructField("dgerdr_log", T.DateType()),
            T.StructField("hgerdr_log", T.TimestampType()),
            T.StructField("igerdr_log", T.StringType()),
            T.StructField("nlin_log", T.IntegerType()),
        ]
    )
    df = logger._build_log_dataframe(
        "unused",
        {
            "dgerdr_log": datetime(2024, 1, 2),
            "hgerdr_log": datetime(2024, 1, 2, 3),
            "igerdr_log": "INFO",
            "nlin_log": "12",
        },
    )
    assert df.schema == logger._target_schema
    assert df.columns == ["dgerdr_log", "hgerdr_log", "igerdr_log", "nlin_log"]
