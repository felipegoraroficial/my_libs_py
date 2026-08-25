from typing import Any, Dict


def line_quote_balance_stats(
    text: str,
    quote: str,
) -> Dict[str, Any]:

    lines = text.splitlines() or [text]
    unbalanced = []

    for idx, line in enumerate(lines[:500]):
        count = 0
        i = 0

        while i < len(line):
            if line[i] == quote and not (i + 1 < len(line) and line[i + 1] == quote):
                count += 1

            i += 1

        if count % 2 != 0:
            unbalanced.append(idx)

    return {
        "lines_checked": min(500, len(lines)),
        "unbalanced_count": len(unbalanced),
        "unbalanced_examples_idx": unbalanced[:10],
    }
