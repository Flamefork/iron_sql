def indent_block(block: str, indent: str) -> str:
    return "\n".join(
        indent + line if i > 0 and line.strip() else line
        for i, line in enumerate(block.split("\n"))
    )
