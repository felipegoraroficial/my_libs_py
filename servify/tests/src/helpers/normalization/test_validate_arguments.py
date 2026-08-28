import pytest

import servify
from servify.settings.config import flags


def test_normalization_validates_arguments(spark):
    df = spark.sql("SELECT 'x' AS value")
    with pytest.raises(ValueError, match="DataFrame"):
        servify.normalization(None, "strings")
    with pytest.raises(ValueError, match="inválido"):
        servify.normalization(df, "decimal")
    with pytest.raises(ValueError, match="Informe 'columns'"):
        servify.normalization(df, "int")
    with pytest.raises(ValueError, match="Informe 'formato'"):
        servify.normalization(df, "date", columns=["value"])
    with pytest.raises(ValueError, match="Colunas inexistentes"):
        servify.normalization(df, "strings", columns=["missing"])
