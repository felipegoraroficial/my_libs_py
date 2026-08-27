from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

from .commons.obter_encoding import obter_encoding
from .csv.analisar_quote_for_path import analisar_quote_for_path
from .csv.detectar_delimitador import detectar_delimitador
from .json.detectar_json_multiline import detectar_json_multiline


def read_by_format(
    spark,
    *,
    file_format: str,
    path_validado: str,
    log,
) -> DataFrame:
    """
    Lê dados conforme o formato e adiciona 'source_file' para formatos baseados em arquivos.
    """
    id_file_based = file_format in {"csv", "txt", "json", "parquet", "delta"}

    if file_format in {"csv", "txt"}:
        if path_validado.startswith(("/Volumes", "dbfs:", "file:")):
            sep = detectar_delimitador(path_validado, log=log)
            encoding = obter_encoding(path_validado, log=log)
            log.info(f"separator detected: {sep} | encoding detected: {encoding}")
        else:
            sep = ","
            encoding = "utf-8"
            log.warning(
                f"Using default separator ',' and encoding 'utf-8' for {path_validado}."
            )

        analise = analisar_quote_for_path(
            path_validado,
            log=log,
        )
        log.info(f"[quote analysis] {analise}")

        multiLine = "true" if analise.get("needs_multiline") else "false"
        quote = analise.get("quote_suggestion") or '"'

        escape_style = analise.get("escape_style")
        if escape_style == "csv_double":
            escape = '"'
        elif escape_style == "backslash":
            escape = "\\"
        else:
            escape = None

        reader = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("samplingRatio", 0.1)
            .option("sep", sep)
            .option("encoding", encoding)
            .option("multiLine", multiLine)
        )

        if quote:
            reader = reader.option("quote", quote)

        if escape:
            reader = reader.option("escape", escape)

        df = reader.csv(path_validado)

    elif file_format == "json":
        if path_validado.startswith(("/Volumes", "dbfs:", "file:")):
            multiline = detectar_json_multiline(path_validado, log=log)
            log.info(f"JSON multiline detected: {multiline}")
            df = spark.read.option("multiline", str(multiline).lower()).json(
                path_validado
            )

            if len(df.columns) == 1:

                coluna = df.columns[0]

                campo = df.schema[coluna]

                if isinstance(campo.dataType, ArrayType) and isinstance(
                    campo.dataType.elementType,
                    StructType,
                ):

                    log.info(f"Flattening array column '{coluna}'.")

                    df = df.withColumn(coluna, F.explode(F.col(coluna))).select(
                        f"{coluna}.*"
                    )

        else:
            log.info(f"Using default JSON multiline 'false' for {path_validado}.")
            df = spark.read.json(path_validado)

    elif file_format == "parquet":
        df = spark.read.parquet(path_validado)

    elif file_format == "delta":
        df = spark.read.format("delta").load(path_validado)

    elif file_format == "table":
        df = spark.table(path_validado)

    else:
        log.error(f"file format '{file_format}' not support.")
        raise ValueError(f"file format '{file_format}' not support.")

    if id_file_based:

        df = df.withColumn(
            "source_file",
            F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1),
        )

    return df
