from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def string_invalid_expr(col_name: str):
    return (
        (F.col(col_name).isNull())
        | (F.trim(F.col(col_name)) == "")
        | (F.lower(F.col(col_name)) == "nan")
    )


def audit_invalid_string_values(
    df: DataFrame,
    string_cols: list[str],
    log,
    sample_fraction: float,
) -> None:

    if sample_fraction == 0.0:
        log.debug("Auditoria em amostra desativada (audit_sample_fraction=0).")
        return

    log.debug(
        f"Executando auditoria de valores inválidos em amostra "
        f"({sample_fraction:.2%}) das colunas STRING"
    )

    audit_df = df.sample(
        withReplacement=False,
        fraction=sample_fraction,
        seed=42,
    )

    audit_exprs = [
        F.coalesce(
            F.sum(F.when(string_invalid_expr(c), 1).otherwise(0)),
            F.lit(0),
        )
        .cast("long")
        .alias(c)
        for c in string_cols
    ]

    audit_row = audit_df.agg(*audit_exprs).first()

    invalid_counts = (
        audit_row.asDict() if audit_row is not None else {c: 0 for c in string_cols}
    )

    for c, count in invalid_counts.items():
        if count > 0:
            log.warning(
                f"[Amostra] Coluna '{c}' possui {count} valores substituídos "
                f"(null/vazio/'nan')."
            )
        else:
            log.debug(f"[Amostra] Coluna '{c}' sem valores problemáticos")


def validate_string_after_transform(
    df_out: DataFrame,
    string_cols: list[str],
    log,
    sample_fraction: float,
) -> None:

    if sample_fraction == 0.0:
        log.debug("Revalidação em amostra desativada (audit_sample_fraction=0).")
        return

    log.debug(f"Validando resultado da substituição em amostra ({sample_fraction:.2%})")

    validation_df = df_out.sample(
        withReplacement=False,
        fraction=sample_fraction,
        seed=42,
    )

    validation_exprs = [
        F.coalesce(
            F.sum(F.when(string_invalid_expr(c), 1).otherwise(0)),
            F.lit(0),
        )
        .cast("long")
        .alias(c)
        for c in string_cols
    ]

    validation_row = validation_df.agg(*validation_exprs).first()

    remaining_invalids = (
        validation_row.asDict()
        if validation_row is not None
        else {c: 0 for c in string_cols}
    )

    still_invalid = {c: v for c, v in remaining_invalids.items() if v > 0}

    if still_invalid:
        for c, count in still_invalid.items():
            log.error(
                f"[Amostra] Coluna '{c}' ainda possui {count} valores inválidos "
                f"após tratamento."
            )

        raise ValueError(
            f"Falha na tratativa de colunas STRING: {list(still_invalid.keys())}"
        )

    for c in string_cols:
        log.info(f"[Amostra] Coluna '{c}' tratada com sucesso")


def tratativa_stringtype(
    df: DataFrame,
    Log,
    string_cols: list[str] | None = None,
) -> DataFrame:

    Log.info("Iniciando tratativa de colunas do tipo STRING.")

    if string_cols is None:
        string_cols = [
            f.name for f in df.schema.fields if f.dataType.simpleString() == "string"
        ]
    else:
        invalid_cols = [c for c in string_cols if c not in df.columns]
        if invalid_cols:
            raise ValueError(f"Colunas inexistentes: {invalid_cols}")

    Log.debug(f"Colunas do tipo STRING identificadas: {string_cols}")

    if not string_cols:
        Log.info("Nenhuma coluna STRING encontrada. Retornando DataFrame original.")
        return df

    audit_enabled = getattr(Log, "show_logs", True)
    audit_sample_fraction = float(getattr(Log, "audit_sample_fraction", 0.05))
    audit_sample_fraction = min(max(audit_sample_fraction, 0.0), 1.0)

    if audit_enabled:
        audit_invalid_string_values(df, string_cols, Log, audit_sample_fraction)

    Log.debug("Aplicando transformação nas colunas STRING")

    transformations = {
        c: F.when(string_invalid_expr(c), F.lit("-")).otherwise(F.col(c))
        for c in string_cols
    }

    df_out = df.select(*[transformations.get(c, F.col(c)).alias(c) for c in df.columns])

    audit_validate = bool(getattr(Log, "audit_validate_after_transform", False))

    if audit_enabled and audit_validate:
        validate_string_after_transform(df_out, string_cols, Log, audit_sample_fraction)

    return df_out
