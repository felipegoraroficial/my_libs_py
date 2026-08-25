from __future__ import annotations

# Built-inimport os
import os
from typing import Any, Dict, Optional

# Third-party
from py4j.protocol import Py4JError  # type: ignore[import-untyped]
from pyspark.errors.exceptions.base import PySparkAttributeError
from pyspark.sql import SparkSession

from .exceptions import ConfigError

# Nem sempre existe 'py4j.security'. Para satisfazer mypy/pylint e manter o runtime:
Py4JSecurityException = Exception  # alias seguro para captura em ambientes com Py4J

# Importar DBUtils de 'pyspark.dbutils' quando disponivel
# Em ambiente fora do Databricks, esse importe pode falhar.
# nesse caso, mantemos a variável DBUtils = None e tratamos no runtime.

try:
    from pyspark.dbutils import DBUtils  # type: ignore
except Exception:  # pragma: no cover
    DBUtils = None  # type: ignore

__all__ = [
    "SparkConfig",
]


class SparkConfig:

    def __init__(self, spark: SparkSession):
        self.spark = spark

        if not hasattr(self, "_env_spark_settings_initialized"):
            self._env_spark_settings_initialized = True

    @staticmethod
    def is_running_in_databricks() -> bool:
        """
        Verifica se o código está sendo executado em um ambiente Databricks.
        Utiliza a variável de ambiente 'DATABRICKS_RUNTIME_VERSION'.
        """

        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            return True

        try:
            spark = SparkSession.getActiveSession()
            if spark is None:
                return False

            return (
                spark.conf.get("spark.databricks.clusterUsageTags.clusterName", None)
                is not None
            )
        except Exception:
            return False

    @staticmethod
    def get_or_create_spark(
        app_name: str = "MySparkApp",
        *,
        master: Optional[str] = None,
        enable_hive: bool = False,
        log_level: str = "WARN",
        extra_confs: Optional[Dict[str, str]] = None,
        packages: Optional[str] = None,
    ) -> SparkSession:
        """
        Obtem a SparkSession ativa ou cria uma nova sessão Spark.
        """

        if SparkConfig.is_running_in_databricks():
            spark = (
                SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
            )
            sc = getattr(spark, "sparkContext", None)

            if sc is not None and log_level:
                try:
                    sc.setLogLevel(log_level)
                except (
                    Py4JSecurityException,
                    Py4JError,
                    PySparkAttributeError,
                    AttributeError,
                    Exception,
                ) as e:
                    print(
                        f"setLogLevel block to this env ({type(e).__name__}): {e}. Ignored"
                    )

            else:
                print("setLogLevel not available. Ignored setLogLevel")

            return spark

        builder = SparkSession.builder.appName(app_name)

        effective_master: str = (
            master if master is not None else os.getenv("SPARK_MASTER", "local[*]")
        )

        builder = builder.master(effective_master)

        if packages:
            builder = builder.config("spark.jars.packages", packages)

        default_confs = {
            "spark.sql.session.timeZone": os.getenv("SPARK_SQL_TIMEZONE", "UTC"),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.shuffle.partitions": os.getenv(
                "SPARK_SQL_SHUFFLE_PARTITIONS", "16"
            ),
        }

        if extra_confs:
            default_confs.update(extra_confs)
        for k, v in default_confs.items():
            builder = builder.config(k, v)

        if enable_hive:
            builder = builder.enableHiveSupport().config(
                "spark.sql.warehouse.dir",
                os.getenv("SPARK_WAREHOUSE_DIR", "./spark_warehouse"),
            )

        try:
            spark = builder.getOrCreate()
            try:
                spark.sparkContext.setLogLevel(log_level)
            except (
                Py4JSecurityException,
                Py4JError,
                PySparkAttributeError,
                AttributeError,
                Exception,
            ) as e:
                print(
                    f"setLogLevel block to this env ({type(e).__name__}): {e}. Ignored"
                )

            return spark
        except Exception as exc:
            raise RuntimeError(
                f"Error creating SparkSession: master = {effective_master}"
                "Verify Java/Scala/Spark are already installed"
                f"Error: {exc}"
            ) from exc

    @staticmethod
    def get_dbutils(spark: Any) -> Optional[Any]:
        """
        Obtem uma instancia de 'dbutils' de forma robusta para uso em Databricks
        ou ambientes compativeis.
        """

        injected = globals().get("dbutils")
        if injected is not None:
            return injected

        if DBUtils is not None:
            return DBUtils(spark)

        return None

    @staticmethod
    def require_dbutils(spark: Any) -> Any:

        dbutils = SparkConfig.get_dbutils(spark)
        if dbutils is None:
            raise ConfigError(
                "dbutils is not available: neither injected nor via dbutils(spark)"
            )
        return dbutils

    @staticmethod
    def apply_performance_defaults(
        spark: SparkSession,
        *,
        shuffle_partitions: int = 200,
        broadcast_threshold_mb: int = 128,
        enable_auto_optimeze: bool = True,
        enable_delta_optimeze: bool = True,
    ) -> None:

        from .flags import LOG_ENABLED  # pylint: disable=import-outside-toplevel

        confs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.shuffle.partitions": str(shuffle_partitions),
            "spark.sql.autoBroadcastJoinThreshold": str(
                broadcast_threshold_mb * 1024 * 1024
            ),
            "spark.sql.parquet.filterPushdown": "true",
            "spark.sql.parquet.mergeSchema": "false",
            "spark.sql.files.ignoreCorruptFiles": "true",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
        }

        if enable_delta_optimeze:
            confs.update(
                {
                    "spark.databricks.delta.optimizeWrite.enabled": "true",
                    "spark.databricks.delta.autoCompact.enabled": "true",
                }
            )

        if enable_auto_optimeze:
            confs.update(
                {
                    (
                        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite"
                    ): "true",
                    (
                        "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact"
                    ): "true",
                }
            )

        for key, value in confs.items():

            try:
                spark.conf.set(key, value)
            except (
                Py4JSecurityException,
                Py4JError,
                PySparkAttributeError,
                AttributeError,
                Exception,
            ) as e:
                if LOG_ENABLED:
                    print(
                        f"Failed to set Spark config '{key}' to '{value}' ({type(e).__name__}): {e}. Ignored"
                    )

    @staticmethod
    def apply_defaults_once(spark: SparkSession) -> None:

        from .flags import LOG_ENABLED  # pylint: disable=import-outside-toplevel

        try:
            if spark.conf.get("app.performance.defaults.applied", "false") != "true":
                SparkConfig.apply_performance_defaults(
                    spark,
                    shuffle_partitions=int(
                        os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "256")
                    ),
                    broadcast_threshold_mb=int(
                        os.getenv("SPARK_SQL_BROADCAST_MB", "128")
                    ),
                )

                spark.conf.set("app.performance.defaults.applied", "true")

                if LOG_ENABLED:
                    print(
                        "[INFO] Spark performance defaults applied "
                        "(apply_defaults_once)."
                    )

        except Exception as e:
            if LOG_ENABLED:
                print(
                    f"[WARN] Could not apply Spark defaults (once): "
                    f"{type(e).__name__}: {e}"
                )
