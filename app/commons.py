from __future__ import annotations

# Built-in
import csv
import glob
import os
import re
from typing import Any, Dict, List, Optional, Tuple, cast

# Third-party
import chardet
import pandas as pd
import pyspark.pandas as ps  # type: ignore[import-untyped]
from py4j.protocol import Py4JError  # type: ignore[import-untyped]
from pyspark.errors.exceptions.base import PySparkAttributeError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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


def require_dbutils(spark: Any) -> Any:

    dbutils = get_dbutils(spark)
    if dbutils is None:
        raise ConfigError(
            "dbutils is not available: neither injected nor via dbutils(spark)"
        )
    return dbutils


__all__ = [
    "ConfigError",
    "DataValidationError",
    "IoError",
    "reading_data",
]


class ConfigError(Exception):
    """Exceção para erros de configuração."""


class DataValidationError(Exception):
    """Exceção para erros de validação de dados."""


class IoError(Exception):
    """Exceção para erros de entrada/saída."""


class reading_data:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.log = Logger(spark)

        if not hasattr(self, "_commons_initialized"):
            self.log.info("Class Reading Dara initialized")
            self._commons_initialized: bool = True

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

        if reading_data.is_running_in_databricks():
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

    def resolve_latest_file(self, path: str) -> str:

        self.log.debug(f"Resolving path: {path}")

        try:
            path_resolvido = path.replace("file:", "")
        except Exception as e:
            self.log.error(f"Error resolving path: {e}")
            raise ValueError(f"Error resolving path: {e}") from e

        if "*" in path_resolvido:
            arquivos = glob.glob(path_resolvido)
            self.log.debug(f"Found files with wildcard: {arquivos}")

            if not arquivos:
                self.log.error(
                    f"No files found for path with wildcard: {path_resolvido}"
                )
                raise FileNotFoundError(f"No files found for path: {path_resolvido}")

            arquivos.sort(key=os.path.getmtime, reverse=True)
            escolhido = arquivos[0]
            self.log.info(f"Latest file selected: {escolhido}")
            return escolhido

        self.log.info(f"Path resolved without wildcard: {path_resolvido}")
        return path_resolvido

    def obter_enconding(
        self, path: str, *, sample_bytes: int = 4096
    ) -> Tuple[str, str]:

        self.log.info(f"Inizialazed encoding detection for file: {path}")

        arquivo_escolhido = self.resolve_latest_file(path)

        try:
            with open(arquivo_escolhido, "rb") as f:
                rawdata = f.read(sample_bytes)
                result = chardet.detect(rawdata) or {}
                encoding_detectado: str = result.get("encoding") or "utf-8"

                conf = result.get("confidence")
                self.log.debug(
                    f"Encoding detected: {encoding_detectado} with confidence: {conf} and language: {result.get('language')}"
                )
        except Exception as e:
            self.log.error(
                f"Error detecting encoding for file {arquivo_escolhido}: {e}",
                exc_info=True,
            )
            raise ValueError(
                f"Error detecting encoding for file {arquivo_escolhido}: {e}"
            ) from e

        self.log.info(
            f"Encoding detection completed for file: {arquivo_escolhido} - Encoding: {encoding_detectado}"
        )

        return arquivo_escolhido, encoding_detectado

    def detectar_delimitador(self, path: str) -> str:

        self.log.info(f"Starting delimiter detection for file: {path}")

        arquivo_escolhido, encoding_detectado = self.obter_enconding(path)

        try:
            with open(
                arquivo_escolhido, "r", encoding=encoding_detectado, newline=""
            ) as f:
                linha = f.readline()
                if not linha:
                    self.log.warning(f"File is empty: {arquivo_escolhido}.")

                self.log.debug(
                    f"first line read for delimiter detection: {linha.rstrip("\n")}"
                )

        except Exception as e:
            self.log.error(
                f"Error reading file {arquivo_escolhido}: {e}", exc_info=True
            )
            raise ValueError(f"Error reading file {arquivo_escolhido}: {e}") from e

        delimitadores = [",", ";", "\t", "|"]

        contagem = {d: len(re.findall(re.escape(d), linha)) for d in delimitadores}
        self.log.debug(f"Delimiter counts: {contagem}")

        if all(c == 0 for c in contagem.values()):
            self.log.warning(
                f"No delimiters found in the first line of file: {arquivo_escolhido}. Trying csv.Sniffer...."
            )
            try:
                dialect = csv.Sniffer().sniff(linha, delimiters="," ";|")
                detected = dialect.delimiter
                self.log.info(f"Delimiter detected by csv.Sniffer: {detected}")
                return detected
            except Exception as e:
                self.log.error(
                    f"csv.Sniffer failed to detect delimiter for file {arquivo_escolhido}: {e}",
                    exc_info=True,
                )
                self.log.warning(
                    f"Using default delimiter ',' for file: {arquivo_escolhido}."
                )
                return ","

        delimitador_detectado = max(contagem.items(), key=lambda kv: kv[1])[0]
        self.log.info(
            f"Delimiter detected: {delimitador_detectado} for file: {arquivo_escolhido}"
        )
        return delimitador_detectado

    def detectar_json_multiline(self, path: str) -> bool:

        self.log.info(f"Starting JSON multiline detection for file: {path}")

        arquivo_escolhido, encoding_detectado = self.obter_enconding(path)

        try:
            with open(arquivo_escolhido, "r", encoding=encoding_detectado) as f:
                linhas = f.readlines()
            self.log.info(f"File {arquivo_escolhido} read successfully.")
        except Exception as e:
            self.log.error(f"Error reading file {arquivo_escolhido}: {e}")
            raise ValueError(f"Error reading file {arquivo_escolhido}: {e}") from e

        primeira_linha = linhas[0].strip()
        self.log.debug(f"First line for JSON multiline detection: {primeira_linha}")

        if primeira_linha.startswith("{") or (
            primeira_linha.startswith("[") and len(linhas) >= 1
        ):
            self.log.info(f"JSON multiline detected for file: {arquivo_escolhido}")
            return True
        self.log.info(f"JSON single line detected for file: {arquivo_escolhido}")
        return False

    def resolve_accessible_path(self, path: str, dbutils) -> str:
        """
        Valida/resolve um path para leitura DBFS ou 'file:'.
        - Com wildcard: garante que existe ao menos um arquivo, mantém wildcard para leitura.
        - Sem wildcard: tenta DBFS, se não, tenta 'file:' se nada der certo, lança FileNotFoundError
        """

        if "*" in path:
            arquivos = glob.glob(path.replace("file:", ""))
            if not arquivos:
                raise FileNotFoundError(f"No file founded in: {path}")

            primeiro = arquivos[0]
            try:
                dbutils.fs.ls(primeiro)
            except Exception:
                arquivo_file = f"file:{primeiro}"
                try:
                    dbutils.fs.ls(arquivo_file)
                except Exception as exc_file:
                    raise FileNotFoundError(
                        f"File '{arquivo_file}' is not accessible by DBFS netheir 'file:'."
                    ) from exc_file

            return path

        try:
            dbutils.fs.ls(path)
            return path
        except Exception:
            path_file = f"file:{path}"
            try:
                dbutils.fs.ls(path_file)
                return path_file
            except Exception as exc_file:
                raise FileNotFoundError(
                    f"File '{path_file}' is not accessible by DBFS netheir 'file:'."
                ) from exc_file

    def read_by_format(self, file_format: str, path_validado: str) -> DataFrame:
        """
        Lê dados conforme o formato e adiciona 'source_file' para formatos baseados em arquivos.
        """
        id_file_based = file_format in {"csv", "txt", "json", "parquet", "delta"}

        if file_format in {"csv", "txt"}:
            if path_validado.startswith(("file:", "/Volumes")):
                sep = self.detectar_delimitador(path_validado)
                self.log.info(f"sseparator detected: {sep}")
            else:
                sep = ","
                self.log.warning(f"Using default separator ',' for {path_validado}.")

            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "false")
                .option("samplingRatio", 0.1)
                .option("sep", sep)
                .csv(path_validado)
            )

        elif file_format == "json":
            if path_validado.startswith(("file:", "/Volumes")):
                multiline = self.detectar_json_multiline(path_validado)
                self.log.info(f"JSON multiline detected: {multiline}")
            else:
                self.log.warning(
                    f"Using default JSON multiline 'false' for {path_validado}."
                )
                df = self.spark.read.json(path_validado)

        elif file_format == "parquet":
            df = self.spark.read.parquet(path_validado)

        elif file_format == "delta":
            df = self.spark.read.format("delta").load(path_validado)

        elif file_format == "table":
            df = self.spark.table(path_validado)

        else:
            self.log.error(f"file format '{file_format}' not support.")
            raise ValueError(f"file format '{file_format}' not support.")

        if id_file_based:
            df = df.withColumn(
                "source_file", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
            )

        return df

    def read_data(
        self, path: str, file_format: str, partition_column: Optional[str] = None
    ) -> DataFrame:
        """
        Lê dados de um path especificado, detectando encoding, delimitador e JSON multiline quando aplicável.
        """

        formatos_validos = {"csv", "txt", "json", "parquet", "delta", "table"}

        if file_format not in formatos_validos:
            self.log.error(f"file format '{file_format}' not support.")
            raise ValueError(f"file format '{file_format}' not support.")

        self.log.info(f"Starting data read for path: {path} with format: {file_format}")

        if partition_column:
            self.log.debug(f"Partition column: {partition_column}")
        else:
            self.log.debug("No partition column specified.")

        dbutils = require_dbutils(self.spark)

        if file_format in {"csv", "txt", "json", "parquet", "delta"}:
            path_validado = self.resolve_accessible_path(path, dbutils)
        else:
            if not self.spark.catalog.tableExists(path):
                self.log.error(f"Table '{path}' does not exist in the catalog.")
                raise ValueError(f"Table '{path}' does not exist in the catalog.")

        self.log.info(f"Table {path} exists in catalog.")
        path_validado = path

        self.log.info(f"Reading date from: {path_validado}")

        df = self.read_by_format(file_format, path_validado)

        if partition_column and file_format in {"delta", "table"}:
            if partition_column not in df.columns:
                self.log.error(
                    f"Partition column '{partition_column}' not found in data columns."
                )
                raise ValueError(
                    f"Partition column '{partition_column}' not found in data columns."
                )

            self.log.info(f"Filtering by last partitito from: {partition_column}")

            ultima_particao = df.select(
                F.max(F.col(partition_column)).alias("max_partition")
            ).collect()[0]["max_partition"]
            self.log.info(f"Last partition value: {ultima_particao}")
            df = df.filter(F.col(partition_column) == F.lit(ultima_particao))

        if df.isEmpty():
            self.log.warning(
                f"No data found in path: {path_validado} with format: {file_format}"
            )
            raise ValueError(
                f"No data found in path: {path_validado} with format: {file_format}"
            )

        self.log.info(f"Data read completed for path: {path_validado}")

        return df

    def list_xslx_paths(self, dir_path: str) -> List[str]:
        """
        Lista todos os arquivos .xlsx em um diretório especificado.
        """

        self.log.info(f"Listing .xlsx files in directory: {dir_path}")

        paths = [
            os.path.join(dir_path, nome)
            for nome in os.listdir(dir_path)
            if nome.lower().endswith(".xlsx")
        ]

        if not paths:
            self.log.warning(f"No .xlsx files found in directory: {dir_path}")
            raise FileNotFoundError(f"No .xlsx files found in directory: {dir_path}")

        self.log.info(f"Found {len(paths)} .xlsx files in directory: {dir_path}")
        return paths

    def ensure_ps_df(self, obj: Any) -> ps.DataFrame:
        """
        Garante que o objeto é um DataFrame do Pandas on Spark API.
        """

        if not isinstance(obj, dict):
            frames: List[ps.DataFrame] = [self.ensure_ps_df(v) for v in obj.values()]
            concat_arg: List[ps.DataFrame | ps.Series] = cast(
                List[ps.DataFrame | ps.Series], frames
            )

            return cast(ps.DataFrame, ps.concat(concat_arg, ignore_index=True))

        if isinstance(obj, ps.Series):
            return obj.to_frame()

        if hasattr(obj, "to_series") and not isinstance(obj, ps.DataFrame):
            return obj.to_series().to_frame()
        return cast(ps.DataFrame, obj)

    def concat_ps_dfs(self, dfs: List[ps.DataFrame]) -> ps.DataFrame:
        """
        Concatena uma lista de DataFrames do Pandas on Spark API.
        """

        self.log.info(f"Concatenating {len(dfs)} Pandas on Spark DataFrames")

        if not dfs:
            self.log.warning("No DataFrames provided for concatenation.")
            raise ValueError("No DataFrames provided for concatenation.")

        if len(dfs) == 1:
            self.log.info("Only one DataFrame provided, returning it directly.")
            return dfs[0]

        concat_arg: List[ps.DataFrame | ps.Series] = cast(
            List[ps.DataFrame | ps.Series], dfs
        )

        self.log.info("DataFrames concatenated successfully.")
        return cast(ps.DataFrame, ps.concat(concat_arg, ignore_index=True))

    def to_spark_with_schema(
        self, df_union: ps.DataFrame, schema: T.StructType
    ) -> DataFrame:
        """
        Converte um DataFrame do Pandas on Spark API para um DataFrame do Spark com o schema especificado.
        """

        if not schema:
            self.log.error("Schema must be provided for conversion.")
            raise ValueError("Schema must be provided for conversion.")

        if not isinstance(schema, T.StructType):
            self.log.error("Provided schema is not a valid StructType.")
            raise ValueError("Provided schema is not a valid StructType.")

        self.log.info(
            "Converting Pandas on Spark DataFrame to Spark DataFrame with specified schema"
        )

        try:
            spark_df_com_schema = self.spark.createDataFrame(
                df_union.to_pandas(), schema=schema
            )
            self.log.info(
                "Conversion to Spark DataFrame with specified schema completed"
            )
            return spark_df_com_schema
        except Exception as e:
            self.log.error(
                f"Error converting to Spark DataFrame with specified schema: {e}"
            )
            raise ValueError(
                f"Error converting to Spark DataFrame with specified schema: {e}"
            ) from e

    def remove_header_rows(self, spark_df: DataFrame) -> DataFrame:
        """
        Remove linhas de cabeçalho duplicadas de um DataFrame do Spark.
        """

        self.log.info("Verify if having heands on lines to remove")

        primeira_coluna = spark_df.columns[0]

        valor_normalizado = F.lower(
            F.regexp_replace(
                F.trim(F.regexp_replace(F.col(primeira_coluna), "_", "")),
                r"\s+",
                " ",
            )
        )

        nome_normalizado = primeira_coluna.lower().replace("_", "").strip()

        existe_header = (
            spark_df.filter(valor_normalizado == F.lit(nome_normalizado)).count() > 0
        )

        if existe_header:
            self.log.info("Header rows detected, removing them")
            spark_df_limpo = spark_df.filter(
                valor_normalizado != F.lit(nome_normalizado)
            )
            self.log.info("Header rows removed successfully")
            return spark_df_limpo

        self.log.info("No header rows detected, returning original DataFrame")
        return spark_df

    def read_xlsx_to_ps_df(self, xlsx_path: str) -> ps.DataFrame:
        """
        Lê um arquivo .xlsx em um DataFrame do Pandas on Spark API.
        """

        self.log.info(f"Reading .xlsx file: {xlsx_path} by API spark.pandas")

        try:
            ps_obj = ps.read_excel(
                xlsx_path,
                engine="openpyxl",
                header=0,
                sheet_name=0,
            )
            ps_df = self.ensure_ps_df(ps_obj)
            self.log.info(f".xlsx file read successfully: {xlsx_path}")

        except Exception as e:
            self.log.warning(
                f"Error reading .xlsx file {xlsx_path} by API spark.pandas: {e}"
            )

            self.log.info(f"Trying to read .xlsx file {xlsx_path} by pandas")

            try:
                pd_df = pd.read_excel(
                    xlsx_path,
                    engine="openpyxl",
                    header=0,
                    sheet_name=0,
                    dtype=str,
                )
                ps_df = self.ensure_ps_df(ps.from_pandas(pd_df))
                self.log.info(f".xlsx file read successfully by pandas: {xlsx_path}")
            except Exception as ex:
                self.log.error(f"Error reading .xlsx file {xlsx_path} by pandas: {ex}")
                raise ValueError(f"Error reading .xlsx file {xlsx_path}: {ex}") from ex

        ps_df = ps_df.astype("string").assign(source_file=os.path.basename(xlsx_path))

        return ps_df

    def read_xlsx(self, dir_path: str, schema: T.StructType) -> DataFrame:
        """
        Lê todos os arquivos .xlsx em um diretório e retorna um DataFrame do Spark com o schema especificado.
        """

        self.log.info(f"Reading all .xlsx files in directory: {dir_path}")

        self.spark.conf.set("spark.sql.amsi.enabled", "false")
        self.log.debug("Disabled AMSI for reading .xlsx files")

        paths = self.list_xslx_paths(dir_path)

        dfs: List[ps.DataFrame] = [self.read_xlsx_to_ps_df(p) for p in paths]

        df_union: ps.DateFrame = self.concat_ps_dfs(dfs)

        spark_df: DataFrame = self.to_spark_with_schema(df_union, schema)

        spark_df_cleaned: DataFrame = self.remove_header_rows(spark_df)

        self.log.info(
            f"All .xlsx files read and combined successfully from directory: {dir_path}"
        )

        return spark_df_cleaned
