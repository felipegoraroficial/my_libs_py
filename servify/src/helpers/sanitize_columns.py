import math
import re
from typing import List, Optional

from pyspark.sql import types as T


def sanitize_columns(
    header_cells: List, *, prefer_from_schema: Optional[T.StructType] = None
) -> List[str]:

    target_names: List[str] = (
        [f.name for f in prefer_from_schema] if prefer_from_schema else []
    )

    seen = set()
    safe_cols = []

    def is_blank(x) -> bool:
        if x is None:
            return True
        try:
            if isinstance(x, float) and math.isnan(x):
                return True
        except Exception:
            pass
        s = str(x).strip()
        return s == "" or s.lower() == "nan"

    for i, h in enumerate(header_cells):
        if is_blank(h):
            if target_names and i < len(target_names):
                base = target_names[i]
            else:
                base = f"c_{i+1}"
        else:
            base = str(h).replace("\n", " ").replace("\r", " ").strip()

        base = re.sub(r"\s+", "_", base)
        base = re.sub(r"[^0-9a-zA-Z_]", "", base)

        if base[0].isdigit():
            base = f"c_{base}"

        name = base
        k = 1
        while name in seen:
            name = f"{base}__{k}"
            k += 1
        seen.add(name)
        safe_cols.append(name)

    return safe_cols
