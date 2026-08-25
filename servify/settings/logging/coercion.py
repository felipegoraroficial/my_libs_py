from datetime import date, datetime
from typing import Any

__all__ = [
    "coerce_to_string",
    "coerce_to_timestamp",
    "coerce_to_date",
    "coerce_to_int",
    "coerce_to_float",
    "coerce_log_value",
]


def coerce_to_string(value: Any) -> Any:

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def coerce_to_timestamp(value: Any) -> Any:

    if isinstance(value, (datetime)):
        return value
    if isinstance(value, (date)):
        return datetime(value.year, value.month, value.day)
    return None


def coerce_to_date(value: Any) -> Any:
    """Converte o valor para ``date`` ou ``None`` quando não aplicável."""
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return None


def coerce_to_int(value: Any) -> Any:
    """Converte o valor para ``int`` ou ``None`` quando não aplicável."""
    try:
        return int(value)

    except (TypeError, ValueError):
        return None


def coerce_to_float(value: Any) -> Any:
    """Converte o valor para ``float`` ou ``None`` quando não aplicável."""
    try:
        return float(value)

    except (TypeError, ValueError):
        return None


def coerce_log_value(value: Any, dtype: Any) -> Any:

    # pylint: disable=import-outside-toplevel
    from pyspark.sql import types as T

    if value is None:
        return None

    if isinstance(dtype, T.StringType):
        return coerce_to_string(value)

    if isinstance(dtype, T.TimestampType):
        return coerce_to_timestamp(value)

    if isinstance(dtype, T.DateType):
        return coerce_to_date(value)

    if isinstance(
        dtype,
        (
            T.ByteType,
            T.ShortType,
            T.IntegerType,
            T.LongType,
        ),
    ):
        return coerce_to_int(value)

    if isinstance(
        dtype,
        (
            T.FloatType,
            T.DoubleType,
            T.DecimalType,
        ),
    ):
        return coerce_to_float(value)

    # Fallback: representa como string.
    return coerce_to_string(value)
