from typing import Any, Dict

from .detect_dominant_quote_char import detect_dominant_quote_char
from .detect_escape_style import detect_escape_style
from .has_quoted_newline_from_text import has_quoted_newline_from_text
from .line_quote_balance_stats import line_quote_balance_stats
from .sample_bytes import sample_bytes


def analisar_quote_for_path(
    path: str,
    *,
    log,
    sample_bytes_size: int = 1_000_000,
    default_quote: str = "",
) -> Dict[str, Any]:

    raw = sample_bytes(
        path,
        sample_bytes=sample_bytes_size,
        log=log,
    )

    text = raw.decode("utf-8", errors="ignore")

    quote = detect_dominant_quote_char(text) or default_quote
    has_qnl = has_quoted_newline_from_text(text, quote)
    escape_style = detect_escape_style(text, quote)
    balance = line_quote_balance_stats(text, quote)

    return {
        "quote_suggestion": quote,
        "escape_style": escape_style,
        "has_quoted_newline": has_qnl,
        "needs_multiline": bool(has_qnl),
        "line_balance": balance,
        "stats": {"len_bytes": len(raw)},
        "reason": ("quoted_newline_detected" if has_qnl else "no_evidence_in_sample"),
    }
