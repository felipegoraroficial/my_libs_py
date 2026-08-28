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


def test_apply_schema_rejects_invalid_inputs(spark):
    schema = T.StructType([T.StructField("id", T.IntegerType())])
    with pytest.raises(ValueError, match="nulo"):
        aplicar_schema_df(None, schema, Log())
    with pytest.raises(ValueError, match="vazio"):
        aplicar_schema_df(spark.sql("SELECT 1 AS id"), T.StructType(), Log())
    with pytest.raises(ValueError, match="different"):
        aplicar_schema_df(
            spark.sql("SELECT 1 AS id"),
            T.StructType(
                [
                    T.StructField("id", T.IntegerType()),
                    T.StructField("x", T.StringType()),
                ]
            ),
            Log(),
        )
    with pytest.raises(TypeError, match="incompatível"):
        aplicar_schema_df(spark.sql("SELECT 'x' AS id"), schema, Log())
    with pytest.raises(ValueError, match="duplicadas"):
        aplicar_schema_df(
            spark.sql("SELECT 1 AS id, 2 AS other").selectExpr("id AS x", "other AS x"),
            T.StructType(
                [
                    T.StructField("a", T.IntegerType()),
                    T.StructField("b", T.IntegerType()),
                ]
            ),
            Log(),
        )
