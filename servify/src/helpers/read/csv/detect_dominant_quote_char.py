from typing import Optional


def detect_dominant_quote_char(text: str) -> Optional[str]:

    dq = text.count('"')
    sq = text.count("'")

    if dq >= 2 and dq >= 2 * max(1, sq):
        return '"'

    if sq >= 2 and sq >= 2 * max(1, dq):
        return "'"

    return None
