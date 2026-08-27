from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def tratativa_timestamptype(
    df: DataFrame, cols: list[str], formato: str, Log
) -> DataFrame:

    Log.info(f"Iniciando conversão para TIMESTAMP nas colunas: {cols}")

    invalid_cols = [c for c in cols if c not in df.columns]
    if invalid_cols:
        raise ValueError(f"Colunas inexistentes: {invalid_cols}")

    audit_enabled = getattr(Log, "show_logs", True)

    if audit_enabled:
        audit_sample_fraction = float(getattr(Log, "audit_sample_fraction", 0.05))
        audit_sample_fraction = min(max(audit_sample_fraction, 0.0), 1.0)

        if audit_sample_fraction == 0.0:
            Log.debug("Auditoria em amostra desativada (audit_sample_fraction=0).")
        else:
            Log.debug(
                f"Executando auditoria de timestamps inválidos em amostra "
                f"({audit_sample_fraction:.2%})"
            )

            audit_df = df.sample(
                withReplacement=False, fraction=audit_sample_fraction, seed=42
            )

            audit_exprs = []

            for c in cols:
                parsed = F.to_timestamp(F.col(c), formato)

                audit_exprs.append(
                    F.coalesce(
                        F.sum(
                            F.when(
                                (F.col(c).isNotNull()) & parsed.isNull(), 1
                            ).otherwise(0)
                        ),
                        F.lit(0),
                    )
                    .cast("long")
                    .alias(c)
                )

            audit_row = audit_df.agg(*audit_exprs).first()

            invalid_counts = (
                audit_row.asDict() if audit_row is not None else {c: 0 for c in cols}
            )

            for c, count in invalid_counts.items():
                if count > 0:
                    Log.warning(
                        f"[Amostra] Coluna '{c}' possui {count} valores inválidos "
                        f"para conversão em TIMESTAMP."
                    )
                else:
                    Log.debug(f"[Amostra] Coluna '{c}' sem valores inválidos")

    Log.debug("Aplicando transformação para TIMESTAMP")

    transformations = {c: F.to_timestamp(F.col(c), formato) for c in cols}

    df_out = df.select(*[transformations.get(c, F.col(c)).alias(c) for c in df.columns])

    Log.debug("Validando schema das colunas convertidas")

    dtypes = dict(df_out.dtypes)

    invalid_types = {c: dtypes.get(c) for c in cols if dtypes.get(c) != "timestamp"}

    if invalid_types:
        for c, dtype in invalid_types.items():
            Log.error(f"Falha na conversão da coluna '{c}'. Tipo: {dtype}")

        raise ValueError(f"Erro na conversão das colunas: {list(invalid_types.keys())}")

    for c in cols:
        Log.info(f"Coluna '{c}' convertida com sucesso para timestamp")

    return df_out
