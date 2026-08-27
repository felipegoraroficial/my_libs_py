from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MESES_MAP = {
    "jan": "01",
    "fev": "02",
    "mar": "03",
    "abr": "04",
    "mai": "05",
    "jun": "06",
    "jul": "07",
    "ago": "08",
    "set": "09",
    "out": "10",
    "nov": "11",
    "dez": "12",
}


def build_month_map_expr():
    return F.create_map(*[F.lit(x) for kv in MESES_MAP.items() for x in kv])


def parse_mmm_yy(col_name: str, month_map_expr):
    col_norm = F.lower(F.trim(F.col(col_name)))
    mes_expr = month_map_expr[F.substring(col_norm, 1, 3)]
    ano_expr = F.split(col_norm, "/").getItem(1)

    return F.when(
        mes_expr.isNotNull() & ano_expr.isNotNull(),
        F.to_date(
            F.concat_ws(
                "/",
                F.lit("01"),
                mes_expr,
                F.concat(F.lit("20"), ano_expr),
            ),
            "dd/MM/yyyy",
        ),
    )


def replace_month_name_ptbr(col_clean):
    return (
        F.when(
            col_clean.contains("janeiro"),
            F.regexp_replace(col_clean, "janeiro", "01"),
        )
        .when(
            col_clean.contains("fevereiro"),
            F.regexp_replace(col_clean, "fevereiro", "02"),
        )
        .when(
            col_clean.contains("março"),
            F.regexp_replace(col_clean, "março", "03"),
        )
        .when(
            col_clean.contains("abril"),
            F.regexp_replace(col_clean, "abril", "04"),
        )
        .when(
            col_clean.contains("maio"),
            F.regexp_replace(col_clean, "maio", "05"),
        )
        .when(
            col_clean.contains("junho"),
            F.regexp_replace(col_clean, "junho", "06"),
        )
        .when(
            col_clean.contains("julho"),
            F.regexp_replace(col_clean, "julho", "07"),
        )
        .when(
            col_clean.contains("agosto"),
            F.regexp_replace(col_clean, "agosto", "08"),
        )
        .when(
            col_clean.contains("setembro"),
            F.regexp_replace(col_clean, "setembro", "09"),
        )
        .when(
            col_clean.contains("outubro"),
            F.regexp_replace(col_clean, "outubro", "10"),
        )
        .when(
            col_clean.contains("novembro"),
            F.regexp_replace(col_clean, "novembro", "11"),
        )
        .when(
            col_clean.contains("dezembro"),
            F.regexp_replace(col_clean, "dezembro", "12"),
        )
    )


def parse_ptbr_long_date(col_name: str):
    col_clean = F.regexp_replace(F.col(col_name), r"^[^,]+,\s*", "")
    col_replaced = replace_month_name_ptbr(col_clean)
    col_fmt = F.regexp_replace(col_replaced, r" de ", "/")

    col_final = F.concat_ws(
        "/",
        F.lpad(F.split(col_fmt, "/").getItem(0), 2, "0"),
        F.lpad(F.split(col_fmt, "/").getItem(1), 2, "0"),
        F.split(col_fmt, "/").getItem(2),
    )

    return F.to_date(col_final, "dd/MM/yyyy")


def parse_date_expr(col_name: str, formato: str, month_map_expr):
    if formato == "MMM/yy":
        return parse_mmm_yy(col_name, month_map_expr)

    if formato == "EEEE, dd 'de' MMMM 'de' yyyy":
        return parse_ptbr_long_date(col_name)

    return F.to_date(F.col(col_name), formato)


def audit_invalid_dates(
    df: DataFrame,
    cols: list[str],
    formato: str,
    month_map_expr,
    log,
    sample_fraction: float,
) -> None:

    if sample_fraction == 0.0:
        log.debug("Auditoria em amostra desativada (audit_sample_fraction=0).")
        return

    log.debug(
        f"Executando auditoria de datas inválidas em amostra ({sample_fraction:.2%})"
    )

    audit_df = df.sample(
        withReplacement=False,
        fraction=sample_fraction,
        seed=42,
    )

    audit_exprs = []

    for col_name in cols:
        parsed = parse_date_expr(col_name, formato, month_map_expr)

        audit_exprs.append(
            F.coalesce(
                F.sum(
                    F.when(
                        (F.col(col_name).isNotNull()) & parsed.isNull(),
                        1,
                    ).otherwise(0)
                ),
                F.lit(0),
            )
            .cast("long")
            .alias(col_name)
        )

    audit_row = audit_df.agg(*audit_exprs).first()

    invalid_counts = (
        audit_row.asDict() if audit_row is not None else {c: 0 for c in cols}
    )

    for c, count in invalid_counts.items():
        if (count or 0) > 0:
            log.warning(
                f"[Amostra] Coluna '{c}' possui {count} valores inválidos "
                f"para conversão de data."
            )
        else:
            log.debug(f"[Amostra] Coluna '{c}' sem valores inválidos")


def tratativa_datetype(df: DataFrame, cols: list[str], formato: str, Log) -> DataFrame:

    Log.info(
        f"Iniciando tratativa de conversão para DATE nas colunas: {cols} "
        f"com formato: {formato}"
    )

    invalid_cols = [c for c in cols if c not in df.columns]
    if invalid_cols:
        raise ValueError(f"Colunas inexistentes: {invalid_cols}")

    month_map_expr = build_month_map_expr()

    audit_enabled = getattr(Log, "show_logs", True)
    audit_sample_fraction = float(getattr(Log, "audit_sample_fraction", 0.05))
    audit_sample_fraction = min(max(audit_sample_fraction, 0.0), 1.0)

    if audit_enabled:
        audit_invalid_dates(
            df, cols, formato, month_map_expr, Log, audit_sample_fraction
        )

    transformations = {
        col_name: parse_date_expr(col_name, formato, month_map_expr)
        for col_name in cols
    }

    df_result = df.select(
        *[transformations.get(c, F.col(c)).alias(c) for c in df.columns]
    )

    Log.debug("Validando schema das colunas convertidas")

    dtypes = dict(df_result.dtypes)

    invalid_types = {c: dtypes.get(c) for c in cols if dtypes.get(c) != "date"}

    if invalid_types:
        for c, dtype in invalid_types.items():
            Log.error(f"Falha na conversão da coluna '{c}'. Tipo: {dtype}")

        raise ValueError(
            f"Erro na conversão das colunas: " f"{list(invalid_types.keys())}"
        )

    for c in cols:
        Log.info(f"Coluna '{c}' convertida com sucesso para date")

    return df_result
