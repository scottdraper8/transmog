#!/usr/bin/env python3

with open("src/transmog/process/strategy.py", "r") as f:
    lines = f.readlines()

# Fix first issue around lines 1189-1197 and the extract_arrays code
fixed_lines = []
in_problem_area = False
problem_fixed = False

for i, line in enumerate(lines):
    # Skip the problematic areas and replace with correctly indented code
    if i >= 1189 and i <= 1210 and not problem_fixed:
        if not in_problem_area:
            fixed_lines.append("            # Track extract ID and add to main table\n")
            fixed_lines.append("            if id_field in annotated:\n")
            fixed_lines.append("                main_ids.append(annotated[id_field])\n")
            fixed_lines.append("            else:\n")
            fixed_lines.append("                main_ids.append(None)\n")
            fixed_lines.append("\n")
            fixed_lines.append("            # Add to the result\n")
            fixed_lines.append("            result.add_main_record(annotated)\n")
            fixed_lines.append("\n")
            fixed_lines.append(
                "            # Extract and process arrays for this record\n"
            )
            fixed_lines.append("            if id_field in annotated:\n")
            fixed_lines.append("                extract_id = annotated[id_field]\n")
            fixed_lines.append("                arrays = extract_arrays(\n")
            fixed_lines.append("                    record,\n")
            fixed_lines.append("                    parent_id=extract_id,\n")
            fixed_lines.append("                    entity_name=entity_name,\n")
            fixed_lines.append(
                '                    separator=params.get("separator", "_"),\n'
            )
            fixed_lines.append(
                '                    cast_to_string=params.get("cast_to_string", True),\n'
            )
            fixed_lines.append(
                '                    include_empty=params.get("include_empty", False),\n'
            )
            fixed_lines.append(
                '                    skip_null=params.get("skip_null", True),\n'
            )
            fixed_lines.append("                    extract_time=extract_time,\n")
            fixed_lines.append(
                '                    abbreviate_enabled=params.get("abbreviate_table_names", True),\n'
            )
            fixed_lines.append(
                '                    max_component_length=params.get("max_table_component_length"),\n'
            )
            fixed_lines.append(
                '                    preserve_leaf=params.get("preserve_leaf_component", True),\n'
            )
            fixed_lines.append(
                '                    custom_abbreviations=params.get("custom_abbreviations"),\n'
            )
            fixed_lines.append(
                "                    default_id_field=default_id_field,\n"
            )
            fixed_lines.append(
                "                    id_generation_strategy=id_generation_strategy,\n"
            )
            fixed_lines.append("                )\n")
            fixed_lines.append("\n")
            fixed_lines.append(
                "                # Add arrays to result for this record\n"
            )
            fixed_lines.append(
                "                for table_name, records in arrays.items():\n"
            )
            fixed_lines.append("                    for child in records:\n")
            fixed_lines.append(
                "                        result.add_child_record(table_name, child)\n"
            )
            in_problem_area = True
            problem_fixed = True
    elif i > 1210 or i < 1189:
        fixed_lines.append(line)

# Fix second issue around line 1385 with the CSV reader
second_fixed_lines = []
in_problem_area2 = False
problem_fixed2 = False

for i, line in enumerate(fixed_lines):
    if i >= 1380 and i <= 1400 and "try:" in line and not problem_fixed2:
        second_fixed_lines.append(line)  # Add the try line
        # Add the with statement with correct indentation
        second_fixed_lines.append(
            '                with open(file_path, "r", encoding=encoding) as f:\n'
        )
        # Skip until we find the skip_rows loop
        for j in range(i + 1, len(fixed_lines)):
            if "for _ in range(skip_rows):" in fixed_lines[j]:
                second_fixed_lines.append(
                    "                    # Skip initial rows if needed\n"
                )
                second_fixed_lines.append(
                    "                    for _ in range(skip_rows):\n"
                )
                in_problem_area2 = True
                i = j + 1
                break

    elif in_problem_area2 and "headers =" in line and not problem_fixed2:
        # Fix indentation for headers line
        second_fixed_lines.append("                    # Read headers if present\n")
        second_fixed_lines.append(
            "                    headers = next(csv_reader) if has_header else None\n"
        )
        problem_fixed2 = True
        in_problem_area2 = False
    elif not in_problem_area2:
        second_fixed_lines.append(line)

with open("src/transmog/process/strategy.py", "w") as f:
    f.writelines(second_fixed_lines)

print("Fixed indentation issues in strategy.py")
