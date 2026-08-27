from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def audit_invalid_int_values(
    df: DataFrame,
    cols: list[str],
    log,
    sample_fraction: float,
) -> None:

    if sample_fraction == 0.0:
        log.debug("Auditoria em amostra desativada (audit_sample_fraction=0).")
        return

    log.debug(
        f"Executando auditoria de valores inválidos em amostra ({sample_fraction:.2%})"
    )

    audit_df = df.sample(
        withReplacement=False,
        fraction=sample_fraction,
        seed=42,
    )

    exprs = []

    for c in cols:
        col_expr = F.col(c)

        invalid_condition = (col_expr.isNotNull()) & (
            col_expr.rlike(r"^-?\d+$").eqNullSafe(False)
        )

        exprs.append(
            F.coalesce(
                F.sum(F.when(invalid_condition, 1).otherwise(0)),
                F.lit(0),
            )
            .cast("long")
            .alias(c)
        )

    audit_row = audit_df.agg(*exprs).first()

    invalid_counts = (
        audit_row.asDict() if audit_row is not None else {c: 0 for c in cols}
    )

    for c, count in invalid_counts.items():
        if count > 0:
            log.warning(
                f"[Amostra] Coluna '{c}' possui {count} valores inválidos "
                f"que serão tratados."
            )
        else:
            log.debug(f"[Amostra] Coluna '{c}' não possui valores inválidos")


def build_int_transformations(cols: list[str], log) -> dict:
    transformations = {}

    for col_name in cols:
        log.debug(f"Convertendo coluna '{col_name}' para INTEGER")

        cleaned = F.regexp_replace(F.col(col_name), "[^0-9-]", "")

        transformations[col_name] = F.coalesce(
            F.when(cleaned != "", cleaned).otherwise(F.lit(None)).cast("bigint"),
            F.lit(0),
        )

    return transformations


def validate_int_dtypes(
    df_int: DataFrame,
    cols: list[str],
    log,
) -> None:

    log.debug("Validando schema das colunas convertidas")

    dtypes = dict(df_int.dtypes)

    for c in cols:
        dtype = dtypes.get(c)

        if dtype not in ("int", "bigint"):
            log.error(f"Falha na conversão da coluna '{c}'. Tipo encontrado: {dtype}")
            raise ValueError(f"Erro na conversão da coluna '{c}'")

        log.info(f"Coluna '{c}' convertida com sucesso para {dtype}")


def tratativa_inttype(df: DataFrame, cols: list[str], Log) -> DataFrame:

    Log.info(f"Iniciando conversão para INTEGER nas colunas: {cols}")

    invalid = [c for c in cols if c not in df.columns]
    if invalid:
        raise ValueError(f"Colunas inexistentes: {invalid}")

    audit_enabled = getattr(Log, "show_logs", True)
    audit_sample_fraction = float(getattr(Log, "audit_sample_fraction", 0.05))
    audit_sample_fraction = min(max(audit_sample_fraction, 0.0), 1.0)

    if audit_enabled:
        audit_invalid_int_values(df, cols, Log, audit_sample_fraction)

    try:
        transformations = build_int_transformations(cols, Log)

    except Exception as e:
        Log.error(
            f"Erro ao converter colunas para INTEGER: {e}",
            exc_info=True,
        )

        raise ValueError(f"Erro ao converter colunas para INTEGER: {e}") from e

    df_int = df.select(*[transformations.get(c, F.col(c)).alias(c) for c in df.columns])

    validate_int_dtypes(df_int, cols, Log)

    return df_int
