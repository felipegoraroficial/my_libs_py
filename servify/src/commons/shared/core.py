from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from servify.settings.logging import Logger
from servify.src.commons.shared.apply_schema import aplicar_schema_df


class Shared_Commons:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._log = None

        if not hasattr(self, "_shared_commons_initialized"):
            self._shared_commons_initialized = True

    @property
    def log(self):
        # pylint: disable=import-outside-toplevel
        from servify.settings.config.flags import LOG_ENABLED

        if self._log is None or self._log.show_logs != LOG_ENABLED:
            self._log = Logger(self.spark, show_logs=LOG_ENABLED)

        return self._log

    def aplicar_schema_df(
        self,
        df: DataFrame,
        schema: T.StructType,
    ) -> DataFrame:
        return aplicar_schema_df(
            df,
            schema,
            self.log,
        )
