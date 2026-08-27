from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def tratativa_floattype(df: DataFrame, cols: list[str], Log) -> DataFrame:

    Log.info(f"Iniciando tratativa de conversão para FLOAT nas colunas: {cols}")

    invalid_cols = [c for c in cols if c not in df.columns]
    if invalid_cols:
        raise ValueError(f"Colunas inexistentes: {invalid_cols}")

    decimal_regex = r"^-?\d+(\.\d+)?$"

    audit_enabled = getattr(Log, "show_logs", True)

    if audit_enabled:
        audit_sample_fraction = float(getattr(Log, "audit_sample_fraction", 0.05))
        audit_sample_fraction = min(max(audit_sample_fraction, 0.0), 1.0)

        if audit_sample_fraction == 0.0:
            Log.debug("Auditoria em amostra desativada (audit_sample_fraction=0).")
        else:
            Log.debug(
                f"Executando auditoria de valores inválidos em amostra "
                f"({audit_sample_fraction:.2%})"
            )

            audit_df = df.sample(
                withReplacement=False, fraction=audit_sample_fraction, seed=42
            )

            exprs = []

            for c in cols:
                col_expr = F.col(c)

                s = col_expr.cast("string")
                s = F.regexp_replace(F.trim(s), r"^\((.*)\)$", r"-\1")
                s = F.regexp_replace(s, r"[^0-9,.\-]", "")

                invalid_condition = (col_expr.isNotNull()) & (
                    s.rlike(decimal_regex).eqNullSafe(False)
                )

                expr = (
                    F.coalesce(
                        F.sum(F.when(invalid_condition, 1).otherwise(0)),
                        F.lit(0),
                    )
                    .cast("long")
                    .alias(c)
                )

                exprs.append(expr)

            audit_row = audit_df.agg(*exprs).first()

            invalid_counts = (
                audit_row.asDict() if audit_row is not None else {c: 0 for c in cols}
            )

            for c, count in invalid_counts.items():
                if count > 0:
                    Log.warning(
                        f"[Amostra] Coluna '{c}' possui " f"{count} valores inválidos."
                    )
                else:
                    Log.debug(f"[Amostra] Coluna '{c}' sem valores inválidos")

    transformations = {}

    for col_name in cols:
        s = F.col(col_name).cast("string")

        s = F.regexp_replace(F.trim(s), r"^\((.*)\)$", r"-\1")
        s = F.regexp_replace(s, r"[^0-9,.\-]", "")

        has_dot = s.contains(".")
        has_comma = s.contains(",")

        strlen = F.length(s)

        last_dot = strlen - F.instr(F.reverse(s), ".") + 1
        last_comma = strlen - F.instr(F.reverse(s), ",") + 1

        digits_after_dot = strlen - last_dot
        digits_after_comma = strlen - last_comma

        decimal_sep = (
            F.when(
                has_dot & has_comma,
                F.when(last_dot > last_comma, F.lit(".")).otherwise(F.lit(",")),
            )
            .when(
                has_dot,
                F.when(digits_after_dot == 3, F.lit(None)).otherwise(F.lit(".")),
            )
            .when(
                has_comma,
                F.when(digits_after_comma == 3, F.lit(None)).otherwise(F.lit(",")),
            )
            .otherwise(F.lit(None))
        )

        s_clean = (
            F.when(decimal_sep == ".", F.regexp_replace(s, ",", ""))
            .when(decimal_sep == ",", F.regexp_replace(s, r"\.", ""))
            .otherwise(F.regexp_replace(s, r"[,.]", ""))
        )

        s_norm = F.when(
            decimal_sep == ",",
            F.regexp_replace(s_clean, ",", "."),
        ).otherwise(s_clean)

        as_double = F.when(
            s_norm.rlike(decimal_regex),
            s_norm.cast("double"),
        ).otherwise(F.lit(None).cast("double"))

        transformations[col_name] = F.round(F.coalesce(as_double, F.lit(0.0)), 4)

    df_out = df.select(*[transformations.get(c, F.col(c)).alias(c) for c in df.columns])

    Log.debug("Validando schema das colunas convertidas")

    dtypes = dict(df_out.dtypes)

    for c in cols:
        dtype = dtypes.get(c)

        if dtype not in ("double", "float"):
            Log.error(f"Falha na conversão da coluna '{c}'. Tipo: {dtype}")
            raise ValueError(f"Erro na conversão da coluna '{c}'")

        Log.info(f"Coluna '{c}' convertida com sucesso para {dtype}")

    return df_out
