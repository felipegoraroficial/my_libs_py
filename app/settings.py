from __future__ import annotations

# Built-in
import os
from typing import Any, Dict, Optional

# Third-party
from py4j.protocol import Py4JError  # type: ignore[import-untyped]
from pyspark.errors.exceptions.base import PySparkAttributeError
from pyspark.sql import SparkSession

# Local
from app.logging import Logger

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
    "ConfigError",
    "DataValidationError",
    "IoError",
    "EnvSparkSettings",
]


class ConfigError(Exception):
    """Exceção para erros de configuração."""


class DataValidationError(Exception):
    """Exceção para erros de validação de dados."""


class IoError(Exception):
    """Exceção para erros de entrada/saída."""


class EnvSparkSettings:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.log = Logger(spark)

        if not hasattr(self, "_env_spark_settings_initialized"):
            self.log.info("Class EnvSparkSettings initialized")
            self._env_spark_settings_initialized: bool = True

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

        dbutils = EnvSparkSettings.get_dbutils(spark)
        if dbutils is None:
            raise ConfigError(
                "dbutils is not available: neither injected nor via dbutils(spark)"
            )
        return dbutils

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
                spark.conf.get("spark.databricks.ckusterUsageTags,clusterName", None)
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

        if EnvSparkSettings.is_running_in_databricks():
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
                "SPARK_SQL_SHUFFLE_PARTITIONS", "200"
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
