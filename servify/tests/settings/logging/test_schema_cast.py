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


def test_apply_schema_renames_and_casts_columns(spark):
    df = spark.sql("SELECT CAST(1 AS INT) AS raw_id, 'ok' AS raw_name")
    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("name", T.StringType()),
        ]
    )
    result = aplicar_schema_df(df, schema, Log())
    assert result.columns == ["id", "name"]
    assert dict(result.dtypes) == {"id": "int", "name": "string"}
