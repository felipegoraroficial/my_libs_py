def has_quoted_newline_from_text(
    text: str,
    quote: str = '"',
) -> bool:

    in_quotes = False
    i = 0

    while i < len(text):
        ch = text[i]

        if ch == quote:
            if i + 1 < len(text) and text[i + 1] == quote:
                i += 2
                continue

            in_quotes = not in_quotes

        elif in_quotes and ch in ("\n", "\r"):
            return True

        i += 1

    return False
