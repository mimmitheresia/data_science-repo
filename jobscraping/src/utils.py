import re


def slugify_title_for_link(text: str) -> str:
    # Lowercase
    text = text.lower()
    # Replace & with 'and'
    text = text.replace("&", "and")
    # Remove special chars ( ) / , .
    text = re.sub(r"[%()\/,\.]", "", text)
    # Remove åäö
    text = re.sub(r"[åäöÅÄÖ]", "", text)
    # Remove multiple whitespaces
    text = re.sub(r"\s+", " ", text).strip()
    # Replace whitespace with hyphens
    text = re.sub(r"\s+", "-", text)
    # Remove multiple consecutive hyphens
    text = text.replace("–", "-").replace("—", "-")
    text = re.sub(r"-+", "-", text)

    # Strip leading/trailing hyphens
    return text.strip("-")


def return_regex_string_match(search_pattern, html_str):
    search_match = re.search(search_pattern, html_str, re.DOTALL)

    if search_match:
        label = (
            search_match.group(1) if search_match.group(1) else search_match.group(2)
        )
    else:
        label = "-"
    return label
