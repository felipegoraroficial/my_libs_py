from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _validar_estrutura_basica(
    df: DataFrame,
    schema: T.StructType,
    log,
) -> tuple[list[str], list[str]]:
    """Valida presença de DataFrame/schema e compatibilidade da contagem de colunas."""

    if df is None:
        log.error("The dataframe is null.")
        raise ValueError("DataFrame de entrada é nulo.")

    if not schema.fields:
        log.error("The schema is null.")
        raise ValueError("Schema de entrada está vazio.")

    df_columns = list(df.columns)
    schema_names = [field.name for field in schema.fields]

    if len(df_columns) != len(schema_names):
        raise ValueError(
            f"Number os dataframes columns is ({len(df_columns)}) its different from "
            f"schema ({len(schema_names)})."
        )

    return df_columns, schema_names


def _validar_colunas_duplicadas(df_columns: list[str], schema_names: list[str]) -> None:
    """Colunas duplicadas quebram o mapeamento posicional confiável."""

    duplicadas_df = sorted(
        {column for column in df_columns if df_columns.count(column) > 1}
    )

    if duplicadas_df:
        raise ValueError(f"DataFrame possui colunas duplicadas: {duplicadas_df}")

    duplicadas_schema = sorted(
        {nome for nome in schema_names if schema_names.count(nome) > 1}
    )

    if duplicadas_schema:
        raise ValueError(
            f"Schema possui nomes de colunas duplicados: {duplicadas_schema}"
        )


def _comparar_campos_posicionalmente(
    campos_origem: list,
    campos_destino: list,
) -> tuple[list[str], list[str]]:
    """Compara origem (DataFrame) e destino (schema) posição a posição."""

    renomeacoes: list[str] = []
    divergencias_tipo: list[str] = []

    for posicao, (origem, destino) in enumerate(zip(campos_origem, campos_destino)):
        if origem.name != destino.name:
            renomeacoes.append(f"{origem.name} -> {destino.name}")

        if origem.dataType != destino.dataType:
            divergencias_tipo.append(
                f"posição {posicao} ('{origem.name}' -> '{destino.name}'): "
                f"tipo origem '{origem.dataType.simpleString()}' != "
                f"schema '{destino.dataType.simpleString()}'"
            )

    return renomeacoes, divergencias_tipo


def _validar_pre_aplicacao(
    df: DataFrame,
    schema: T.StructType,
    log,
) -> None:

    df_columns, schema_names = _validar_estrutura_basica(df, schema, log)

    _validar_colunas_duplicadas(
        df_columns,
        schema_names,
    )

    # Comparação posicional entre origem (DataFrame) e destino (schema):
    #   - nome diferente  -> apenas WARNING (renomeação posicional é permitida);
    #   - tipo diferente  -> ERRO (tipagem de origem incompatível com o schema).
    renomeacoes, divergencias_tipo = _comparar_campos_posicionalmente(
        df.schema.fields,
        schema.fields,
    )

    if renomeacoes:
        log.warning(f"Colunas serão renomeadas por posição: {renomeacoes}")

    if divergencias_tipo:
        mensagem = "Tipagem incompatível entre DataFrame e schema: " + "; ".join(
            divergencias_tipo
        )

        log.error(mensagem)
        raise TypeError(mensagem)

    log.debug(f"Pré-validação OK. Origem: {df_columns} | Destino: {schema_names}")


def _validar_pos_aplicacao(df_resultante: DataFrame, schema: T.StructType, log) -> None:

    campos_resultantes = df_resultante.schema.fields
    campos_esperados = schema.fields

    if len(campos_resultantes) != len(campos_esperados):
        raise RuntimeError(
            f"Pós-validação falhou: número de colunas resultantes "
            f"({len(campos_resultantes)}) difere do schema ({len(campos_esperados)})"
        )

    divergencia: list[str] = []

    for posicao, (resultante, esperado) in enumerate(
        zip(campos_resultantes, campos_esperados)
    ):
        if resultante.name != esperado.name:
            divergencia.append(
                f"posição {posicao}: nome '{resultante.name}' != "
                f"esperado '{esperado.name}'"
            )

        if resultante.dataType != esperado.dataType:
            divergencia.append(
                f"posição {posicao} ('{esperado.name}'): "
                f"tipo {resultante.dataType.simpleString()} ('{esperado.name}' != "
                f"esperado ('{esperado.dataType.simpleString()}') "
            )

        if divergencia:
            raise RuntimeError(
                "Pós-validação do schema falhou: " + "; ".join(divergencia)
            )

        log.debug("Pós-validação OK: ordem, nomes e tipos conferem com o schema.")


def aplicar_schema_df(
    df: DataFrame,
    schema: T.StructType,
    log,
) -> DataFrame:

    log.debug("Aplicando schema ao DataFrame...")

    _validar_pre_aplicacao(df, schema, log)

    try:
        df_temp = df.select(
            [
                F.col(df.columns[i]).cast(field.dataType).alias(field.name)
                for i, field in enumerate(schema.fields)
            ]
        )

    except Exception as e:
        log.error(f"Erro ao aplicar schema: {e}")
        raise RuntimeError(f"Falha ao aplicar schema: {e}") from e

    _validar_pos_aplicacao(
        df_temp,
        schema,
        log,
    )

    log.info("Schema aplicado com sucesso.")

    return df_temp
