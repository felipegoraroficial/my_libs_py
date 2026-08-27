def detect_escape_style(
    text: str,
    quote: str = '"',
) -> str:

    if quote == '"':
        if '""' in text:
            return "csv_double"

        if '\\"' in text:
            return "backslash"

    else:
        if "''" in text:
            return "csv_double"

        if "\\'" in text:
            return "backslash"

    return "none"
