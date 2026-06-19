from pathlib import Path

STRUCTURED_EXTENSIONS = {
    ".csv",
    ".xlsx",
    ".xls",
    ".json"
}

UNSTRUCTURED_EXTENSIONS = {
    ".pdf",
    ".docx",
    ".txt",
    ".md",
    ".pptx"
}


def classify_file(file_name: str) -> str:
    ext = Path(file_name).suffix.lower()

    if ext in STRUCTURED_EXTENSIONS:
        return "structured"

    if ext in UNSTRUCTURED_EXTENSIONS:
        return "unstructured"

    return "unknown"