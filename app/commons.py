from __future__ import annotations

# Built-in
import csv
import glob
import math
import os
import re
from typing import Any, Dict, List, Optional

# Third-party
import chardet
import numpy as np
import pandas as pd
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
        self.dataservy = dataservy(spark)

        if not hasattr(self, "_reading_data_initialized"):
            self.log.info("Class Reading Data initialized")
            self._reading_data_initialized: bool = True

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

    def obter_enconding(self, path: str, *, sample_bytes: int = 4096) -> str:

        arquivo_escolhido = self.resolve_latest_file(path)

        self.log.info(f"Initialized encoding detection for file: {path}")

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

        return encoding_detectado

    def detectar_delimitador(self, path: str) -> str:

        arquivo_escolhido = self.resolve_latest_file(path)

        encoding_detectado = self.obter_enconding(path)

        self.log.info(f"Starting delimiter detection for file: {arquivo_escolhido}")

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

        arquivo_escolhido = self.resolve_latest_file(path)
        encoding_detectado = self.obter_enconding(arquivo_escolhido)

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

        if path_validado.startswith(("/Workspace")):
            path_validado = f"file:{path_validado}"

        if file_format in {"csv", "txt"}:
            if path_validado.startswith(("/Volumes", "dbfs:", "file:")):
                sep = self.detectar_delimitador(path_validado)
                encoding = self.obter_enconding(path_validado)
                self.log.info(
                    f"separator detected: {sep} | encoding detected: {encoding}"
                )
            else:
                sep = ","
                encoding = "utf-8"
                self.log.warning(
                    f"Using default separator ',' and encoding 'utf-8' for {path_validado}."
                )

            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "false")
                .option("samplingRatio", 0.1)
                .option("sep", sep)
                .option("encoding", encoding)
                .csv(path_validado)
            )

        elif file_format == "json":
            if path_validado.startswith(("/Volumes", "dbfs:", "file:")):
                multiline = self.detectar_json_multiline(path_validado)
                self.log.info(f"JSON multiline detected: {multiline}")
                df = self.spark.read.option("multiline", str(multiline).lower()).json(
                    path_validado
                )
            else:
                self.log.info(
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
            if path_validado.startswith(("/Volumes")):

                df = df.withColumn(
                    "source_file",
                    F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1),
                )

            else:

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

            path_validado = path
            self.log.info(f"Table '{path}' exists in the catalog.")

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

        if os.path.isfile(dir_path):
            if dir_path.lower().endswith(".xlsx"):
                self.log.info(f"Single .xlsx file found: {dir_path}")
                return [dir_path]

            self.log.error(f"The specified path is a file but not .xlsx: {dir_path}")
            raise ValueError(f"The specified path is a file but not .xlsx: {dir_path}")

        if os.path.isdir(dir_path):
            paths = [
                os.path.join(dir_path, nome)
                for nome in os.listdir(dir_path)
                if nome.lower().endswith(".xlsx")
            ]
            if not paths:
                self.log.error(f"No .xlsx files found in directory: {dir_path}")
                raise FileNotFoundError(
                    f"No .xlsx files found in directory: {dir_path}"
                )

            self.log.info(f"Found {len(paths)} .xlsx files in directory: {dir_path}")
            return paths

        self.log.error(
            f"The specified path is neither a file nor a directory: {dir_path}"
        )
        raise ValueError(
            f"The specified path is neither a file nor a directory: {dir_path}"
        )

    def saniteze_columns(
        self, header_cells: List, prefer_from_schema: Optional[T.StructType] = None
    ) -> List[str]:

        target_names: List[str] = (
            [f.name for f in prefer_from_schema] if prefer_from_schema else []
        )

        seen = set()
        safe_cols = []

        def is_blank(x):
            if x is None:
                return True
            try:
                if isinstance(x, float) and math.isnan(x):
                    return True
            except Exception:
                pass
            s = str(x).strip()
            return s == "" or s.lower() == "nan"

        for i, h in enumerate(header_cells):
            if is_blank(h):
                if target_names and i < len(target_names):
                    base = target_names[i]
                else:
                    base = f"c_{i+1}"
            else:
                base = str(h).replace("\n", " ").replace("\r", " ").strip()

            base = re.sub(r"\s+", "_", base)
            base = re.sub(r"[^0-9a-zA-Z_]", "", base)

            if base[0].isdigit():
                base = f"c_{base}"

            name = base
            k = 1
            while name in seen:
                name = f"{base}__{k}"
                k += 1
            seen.add(name)
            safe_cols.append(name)

        return safe_cols

    def concat_ps_dfs(self, lista_files: List[str], schema: T.StructType) -> DataFrame:
        """
        Concatena uma lista de DataFrames do Pandas on Spark API.
        """

        if not lista_files:
            self.log.warning("No DataFrames provided for concatenation.")
            raise ValueError("No DataFrames provided for concatenation.")

        if len(lista_files) == 1:
            self.log.info("Only one DataFrame provided, returning it directly.")
            return self.read_xlsx_with_pandas(lista_files[0], schema=schema)

        self.log.info(f"Concatenating {len(lista_files)} Pandas on Spark DataFrames")

        df_final = self.read_xlsx_with_pandas(lista_files[0], schema=schema)
        self.log.info(f"file: {lista_files[0]} read successfully.")

        for file in lista_files[1:]:
            df = self.read_xlsx_with_pandas(file, schema=schema)
            df_final = df_final.unionByName(df, allowMissingColumns=True)
            self.log.info(f"file: {file} read and concatenated successfully.")

        self.log.info("DataFrames concatenated successfully.")
        return df_final

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

    def read_xlsx_with_pandas(self, xlsx_path: str, schema: T.StructType, sheet_name=0):
        """
        Lê um arquivo .xlsx em um DataFrame do Pandas on Spark API.
        """

        self.log.info(f"Reading .xlsx file: {xlsx_path} by pandas")

        try:
            df = pd.read_excel(
                xlsx_path,
                engine="openpyxl",
                header=0,
                sheet_name=sheet_name,
            )

            valid_mask = df.notna().any(axis=1)
            if not bool(valid_mask.all()):
                self.log.warning(f"All rows are empty in .xlsx file: {xlsx_path}")
                raise ValueError(f"All rows are empty in .xlsx file: {xlsx_path}")

            first_valid_pos = int(np.argmax(valid_mask.to_numpy()))

        except Exception as e:
            self.log.warning(f"Error reading .xlsx file {xlsx_path} by pandas: {e}")
            raise

        header_raw = df.iloc[first_valid_pos].tolist()
        safe_cols = self.saniteze_columns(header_raw, prefer_from_schema=schema)

        df = df.dropna(how="all").reset_index(drop=True)

        for c in range(len(safe_cols)):
            col = df.columns[c]
            df[col] = (
                df[col]
                .where(~df[col].isna(), None)
                .map(lambda x: str(x) if x is not None else None)
            )

        df.columns = safe_cols

        self.log.info(
            "Using safe conversion mode to avoid internal Serverless Arrow errors."
        )

        string_schema = T.StructType(
            [T.StructField(c, T.StringType(), True) for c in df.columns]
        )

        try:
            sdf = self.spark.createDataFrame(df, schema=string_schema)

        except Exception as e:
            self.log.error(
                "Spark Serverless failed to create DataFrame even in safe mode."
            )
            self.log.debug(f"Internal Spark error: {e}")
            raise

        sdf = sdf.select([F.col(c).cast("string").alias(c) for c in sdf.columns])

        sdf = sdf.withColumn("source_file", F.lit(os.path.basename(xlsx_path)))

        sdf = self.dataservy.aplicar_schema_df(sdf, schema)

        sdf = self.remove_header_rows(sdf)

        return sdf

    def read_xlsx(self, dir_path: str, schema: T.StructType) -> DataFrame:
        """
        Lê todos os arquivos .xlsx em um diretório e retorna um DataFrame do Spark com o schema especificado.
        """

        self.log.info(f"Reading .xlsx file in directory: {dir_path}")

        paths = self.list_xslx_paths(dir_path)

        df = self.concat_ps_dfs(paths, schema)

        self.log.info(
            f"All .xlsx files read and combined successfully from directory: {dir_path}"
        )

        return df


class dataservy:

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.log = Logger(spark)

        if not hasattr(self, "_dataservy_initialized"):
            self.log.info("Class dataservy initialized")
            self._dataservy_initialized: bool = True

    def aplicar_schema_df(self, df: DataFrame, schema: T.StructType) -> DataFrame:
        """
        Aplica um schema especificado a um DataFrame do Spark.
        """

        self.log.info("Applying schema to DataFrame")

        if df is None:
            self.log.error("Input DataFrame is None")
            raise ValueError("Input DataFrame is None")

        if not schema.fields:
            self.log.warning("Provided schema is empty, returning original DataFrame")
            return df

        try:

            df_temp = df.toDF(*[f.name for f in schema.fields])

            for field in schema.fields:
                df_temp = df_temp.withColumn(
                    field.name, F.col(field.name).cast(field.dataType)
                )

            self.log.info("Schema applied successfully")
            return df_temp

        except Exception as e:
            self.log.error(f"Error applying schema: {e}")
            raise ValueError(f"Error applying schema: {e}") from e
