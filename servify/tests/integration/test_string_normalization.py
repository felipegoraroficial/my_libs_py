import pytest

import servify
from servify.src.commons.functions.read import servify_read


def test_string_normalization_replaces_blank_and_nan_values(spark):
    df = spark.sql(
        """
        SELECT * FROM VALUES
            ('joao', 'ativo'),
            ('', CAST(NULL AS STRING)),
            ('NaN', 'inativo')
        AS data(nome, status)
        """
    )

    result = servify.normalization(df, "strings")

    rows = result.orderBy("nome").collect()
    assert [(row["nome"], row["status"]) for row in rows] == [
        ("-", "-"),
        ("-", "inativo"),
        ("joao", "ativo"),
    ]
