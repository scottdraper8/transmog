#!/usr/bin/env python3
"""Fix markdown issues.

- Breaks long lines
- Adds language specifiers to code blocks
"""

import re
from pathlib import Path

MAX_LINE_LENGTH = 120


def is_protected_line(line):
    """Check if a line should be protected from line breaking."""
    # Pattern groups for lines that should not be broken
    protected_patterns = [
        r"^#{1,6}\s+",  # Headers
        r"^-{3,}$",  # Horizontal rule
        r"^\s*[*+-]\s+",  # Unordered list
        r"^\s*\d+\.\s+",  # Ordered list
        r"^```",  # Code block markers
        r"^\s*```",  # Indented code block markers
        r"^\s*>",  # Blockquotes
        r"^\s*$",  # Empty lines
        r".*\|\s*$",  # Table row endings
        r"^\s*\|.*\|\s*$",  # Table rows
        r"^\s*[=-]{3,}\s*$",  # Alternative header
        r"^\[.*\]:.*$",  # Link references
    ]

    return any(re.match(pattern, line) for pattern in protected_patterns)


def break_long_line(line, max_length=MAX_LINE_LENGTH):
    """Break a long line into multiple lines with max_length."""
    if len(line) <= max_length or is_protected_line(line):
        return line

    # Calculate optimal break point location
    ideal_break_point = max_length - 20
    if ideal_break_point < 0:
        ideal_break_point = max_length // 2

    break_point = max_length
    # Search forward from ideal break point
    for i in range(ideal_break_point, min(max_length, len(line))):
        if i < len(line) and line[i] == " ":
            break_point = i
            break

    # Search backward if no break point found
    if break_point == max_length:
        for i in range(
            min(max_length, len(line)) - 1, max(ideal_break_point - 10, 0), -1
        ):
            if i < len(line) and line[i] == " ":
                break_point = i
                break

    # Force break at max_length if no space found
    if break_point == max_length and len(line) > max_length:
        if len(line) <= max_length:
            return line
        return line[:max_length] + "\n" + break_long_line(line[max_length:], max_length)

    if break_point < len(line):
        return (
            line[:break_point]
            + "\n"
            + break_long_line(line[break_point + 1 :], max_length)
        )

    return line


def fix_code_blocks(content):
    """Add 'text' as the language specifier for code blocks that don't have one."""
    lines = content.split("\n")
    in_code_block = False

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("```"):
            if not in_code_block:
                if stripped == "```":
                    # Add 'text' language specifier to unmarked code blocks
                    lines[i] = "```text"
            # Track code block state
            in_code_block = not in_code_block

    return "\n".join(lines)


def fix_markdown_file(file_path):
    """Fix markdown issues in a file."""
    with open(file_path, encoding="utf-8") as f:
        content = f.read()

    # Apply code block fixes before line length fixes
    content = fix_code_blocks(content)

    # Process content line by line
    lines = content.split("\n")
    in_code_block = False
    fixed_lines = []

    for line in lines:
        if line.strip().startswith("```"):
            in_code_block = not in_code_block
            fixed_lines.append(line)
            continue

        # Preserve code block content intact
        if in_code_block:
            fixed_lines.append(line)
            continue

        # Apply line breaking for non-protected lines
        if len(line) > MAX_LINE_LENGTH and not is_protected_line(line):
            broken_lines = break_long_line(line).split("\n")
            fixed_lines.extend(broken_lines)
        else:
            fixed_lines.append(line)

    fixed_content = "\n".join(fixed_lines)

    # Ensure standard file ending with newline
    if not fixed_content.endswith("\n"):
        fixed_content += "\n"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(fixed_content)

    return True


def process_directory(directory):
    """Process all markdown files in a directory recursively."""
    fixed_count = 0
    directory_path = Path(directory)

    for path in directory_path.glob("**/*.md"):
        # Skip markdown files in .env directory
        if ".env" in path.parts:
            continue

        print(f"Processing {path}")
        if fix_markdown_file(path):
            fixed_count += 1

    return fixed_count


def main():
    """Main entry point."""
    # Locate script directory and parent directory
    script_dir = Path(__file__).resolve().parent
    parent_dir = script_dir.parent

    print(f"Processing all markdown files in {parent_dir}")
    fixed_count = process_directory(parent_dir)
    print(f"Fixed {fixed_count} markdown files")


if __name__ == "__main__":
    main()
