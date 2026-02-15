from pathlib import Path


def indent_block(block: str, indent: str) -> str:
    return "\n".join(
        indent + line if i > 0 and line.strip() else line
        for i, line in enumerate(block.split("\n"))
    )


def write_if_changed(path: Path, new_content: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_content = path.read_text(encoding="utf-8") if path.exists() else None
    if existing_content == new_content:
        return False
    path.write_text(new_content, encoding="utf-8")
    path.touch()
    return True
