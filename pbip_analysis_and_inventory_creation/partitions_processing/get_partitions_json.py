# import re
# import json
# from pathlib import Path
# import yaml


# def extract_partitions(tables_folder):
#     tables_path = Path(tables_folder)
#     all_partitions = {}

#     for tmdl_file in sorted(tables_path.glob("*.tmdl")):
#         with open(tmdl_file, "r", encoding="utf-8") as f:
#             content = f.read()

#         tmdl_name = tmdl_file.stem
#         partitions = re.split(r'(?=^\s*partition )', content, flags=re.MULTILINE)

#         for partition in partitions:
#             if not re.match(r'\s*partition\s+', partition):
#                 continue

#             # Extract partition name and type (m, calculated, etc.)
#             name_type_match = re.match(r'\s*partition\s+(.+?)\s*=\s*(\w+)', partition)
#             partition_name = name_type_match.group(1).strip() if name_type_match else ""
#             partition_type = name_type_match.group(2).strip() if name_type_match else ""

#             # Extract mode
#             mode_match = re.search(r'^\s*mode:\s*(.+)', partition, flags=re.MULTILINE)
#             mode = mode_match.group(1).strip() if mode_match else ""

#             # Extract M query (only for type "m")
#             m_query = ""
#             source_match = re.search(r'source\s*=\s*`*\s*\n?(.*)', partition, flags=re.DOTALL)
#             m_query = source_match.group(1).strip().strip('`') if source_match else ""
#             m_query = re.split(r'\n\s*(annotation|changedProperty)\s+', m_query)[0].strip()

#             # Extract entity name
#             entity_name = ""
#             expression_source = ""
#             if partition_type == "entity":
#                 en_match = re.search(r'^\s*entityName:\s*(.+)', partition, flags=re.MULTILINE)
#                 es_match = re.search(r'^\s*expressionSource:\s*(.+)', partition, flags=re.MULTILINE)
#                 entity_name = en_match.group(1).strip() if en_match else ""
#                 expression_source = es_match.group(1).strip().strip("'") if es_match else ""

#             all_partitions[tmdl_name] = {
#                 "partition_name": partition_name,
#                 "partition_type": partition_type,
#                 "mode": mode,
#                 "m_query": m_query,
#                 "entity_name": entity_name,           # populated for entity type
#                 "expression_source": expression_source  # the AS connection reference
#             }

#     return all_partitions



# def extract_dependencies_from_m_query(current_table_name, m_query, all_known_tables):
#     """
#     Casts a 'Regex Net' over the M query to find potential dependencies,
#     then filters them against the actual known Universe of Tables.
#     """
#     dependencies = set()
    
#     if not m_query:
#         return []

#     # Regex 1: Catches M's quoted table references: #"Table Name with Spaces"
#     # The (?!\s*=) is a safety net: it ensures we don't accidentally grab a local M step declaration
#     quoted_pattern = re.compile(r'#"(.*?)"(?!\s*=)')
    
#     # Regex 2: Catches standard unquoted table references: TableNameWithoutSpaces
#     # Also uses (?!\s*=) to avoid grabbing local step variables like AppendedData = ...
#     word_pattern = re.compile(r'\b([a-zA-Z_]\w*)\b(?!\s*=)')

#     # Cast the net for quoted tables
#     for match in quoted_pattern.findall(m_query):
#         if match in all_known_tables and match != current_table_name:
#             dependencies.add(match)

#     # Cast the net for unquoted standard words
#     for match in word_pattern.findall(m_query):
#         if match in all_known_tables and match != current_table_name:
#             dependencies.add(match)

#     return sorted(list(dependencies))


# def main():
#     with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
#         pbi_variables = yaml.safe_load(f)["power_bi_variables"]
    
#     #### Variables
#     tables_folder = Path(pbi_variables["reports_folder"]) / Path(pbi_variables["table_folder_path"])
#     output_json = Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "all_partitions.json"
#     #### Variables END

#     partitions = extract_partitions(tables_folder)

#     # Step 1: Establish the Universe of Tables
#     all_known_tables = set(partitions.keys())
    
#     print("Mapping M query dependencies...")
#     for tablename, tableinfo in partitions.items():
#         partition_type = tableinfo.get("partition_type", "")
        
#         # Step 2 & 3: Skip Entity and Calculated partitions. Only parse 'm' queries.
#         if partition_type == "m":
#             m_query = tableinfo.get("m_query", "")
#             # Step 4 & 5: Extract, validate, deduplicate, and append
#             dependencies = extract_dependencies_from_m_query(tablename, m_query, all_known_tables)
#             tableinfo["dependency"] = dependencies
#         else:
#             # If it's entity or calculated, just give it an empty dependency list
#             tableinfo["dependency"] = []

#     # Write to JSON

#     with open(output_json, "w", encoding="utf-8") as f:
#         json.dump(partitions, f, indent=4, ensure_ascii=False)

#     print(f"Total partitions extracted: {len(partitions)}")
#     print(f"JSON written to: {output_json}")


#     # ==========================================
#     # OPTIONAL EXCEL EXPORT
#     # (Requires: pip install pandas openpyxl)
#     # ==========================================
#     # import pandas as pd
    
#     # excel_data = []
#     # for table_name, info in partitions.items():
#     #     row = {"Table": table_name}
#     #     row.update(info)
#     #     # Convert the dependency list into a comma-separated string for Excel readability
#     #     row["dependency"] = ", ".join(row.get("dependency", []))
#     #     excel_data.append(row)
    
#     # excel_output_path = output_json.with_suffix('.xlsx')
#     # df = pd.DataFrame(excel_data)
#     # df.to_excel(excel_output_path, index=False)
#     # print(f"Excel written to: {excel_output_path}")
#     # ==========================================


# if __name__ == "__main__":
#     main()

import re
import json
from pathlib import Path
import yaml


def extract_partitions(tables_folder):
    """
    Scans all .tmdl files in the tables folder.
    These are fully materialized model tables in the semantic model.
    source_layer = "model_table"
    """
    tables_path = Path(tables_folder)
    all_partitions = {}

    for tmdl_file in sorted(tables_path.glob("*.tmdl")):
        with open(tmdl_file, "r", encoding="utf-8") as f:
            content = f.read()

        tmdl_name = tmdl_file.stem
        partitions = re.split(r'(?=^\s*partition )', content, flags=re.MULTILINE)

        for partition in partitions:
            if not re.match(r'\s*partition\s+', partition):
                continue

            # Extract partition name and type (m, calculated, entity, etc.)
            name_type_match = re.match(r'\s*partition\s+(.+?)\s*=\s*(\w+)', partition)
            partition_name = name_type_match.group(1).strip() if name_type_match else ""
            partition_type = name_type_match.group(2).strip() if name_type_match else ""

            # Extract mode
            mode_match = re.search(r'^\s*mode:\s*(.+)', partition, flags=re.MULTILINE)
            mode = mode_match.group(1).strip() if mode_match else ""

            # Extract M query (only for type "m")
            m_query = ""
            source_match = re.search(r'source\s*=\s*`*\s*\n?(.*)', partition, flags=re.DOTALL)
            m_query = source_match.group(1).strip().strip('`') if source_match else ""
            m_query = re.split(r'\n\s*(annotation|changedProperty)\s+', m_query)[0].strip()

            # Extract entity fields
            entity_name = ""
            expression_source = ""
            if partition_type == "entity":
                en_match = re.search(r'^\s*entityName:\s*(.+)', partition, flags=re.MULTILINE)
                es_match = re.search(r'^\s*expressionSource:\s*(.+)', partition, flags=re.MULTILINE)
                entity_name = en_match.group(1).strip() if en_match else ""
                expression_source = es_match.group(1).strip().strip("'") if es_match else ""

            all_partitions[tmdl_name] = {
                "source_layer": "model_table",
                "partition_name": partition_name,
                "partition_type": partition_type,
                "mode": mode,
                "m_query": m_query,
                "entity_name": entity_name,
                "expression_source": expression_source
            }

    return all_partitions


def classify_expression_body(body: str) -> str:
    """
    Determines whether an expression body is a table query, a function, or a parameter.

    Rules:
    - Functions start with '(' — M function literal syntax: (arg) => ...
    - Table queries contain a 'let' block — multi-step M query returning a table
    - Everything else is a scalar parameter (a simple value like "PROD_SERVER" or #date(...))
    """
    stripped = body.strip()

    if stripped.startswith("("):
        return "function"

    if re.search(r'\blet\b', stripped, flags=re.IGNORECASE):
        return "expression_query"

    return "parameter"


def _extract_expression_body(block: str) -> str:
    """
    Extracts the M expression body from a single expression block.

    Handles three formats found in expressions.tmdl:
      1. Triple-backtick delimited  (```...```)
      2. Single-backtick delimited  (`...`)
      3. Raw multi-line body        (no backticks — body ends at lineageTag / annotation / changedProperty)
      4. Single-line scalar         (fallback for parameters)
    """
    # ── Format 1 & 2: backtick-delimited body ─────────────────────────────────
    body_match = re.search(r'=\s*`{1,3}\s*\r?\n?(.*?)`{1,3}\s*(?:\r?\n|$)', block, flags=re.DOTALL)
    if body_match:
        body = body_match.group(1)
    else:
        # ── Format 3: raw multi-line body (ends at lineageTag / annotation / changedProperty)
        body_match = re.search(
            r'=\s*\r?\n(.*?)(?=\r?\n[ \t]*(?:lineageTag|annotation|changedProperty)[\s:])', 
            block,
            flags=re.DOTALL
        )
        if body_match:
            body = body_match.group(1)
        else:
            # ── Format 4: single-line scalar (parameter)
            body_match = re.search(r'=\s*(.+)', block)
            body = body_match.group(1).strip() if body_match else ""

    # Strip trailing annotations that may still bleed in
    body = re.split(r'\r?\n\s*(annotation|changedProperty)\s+', body)[0]
    return body.strip()


def extract_expressions(expressions_tmdl_path):
    """
    Parses expressions.tmdl and extracts all shared M expressions.
    Classifies each as: expression_query, parameter, or function.
    Only expression_query entries are returned — parameters and functions
    are written to a separate sidecar JSON for reference.

    Returns:
        expression_queries   (dict) — only table-like M expressions, for main JSON
        other_expressions    (dict) — parameters and functions, for sidecar JSON
    """
    path = Path(expressions_tmdl_path)
    if not path.exists():
        print(f"Warning: expressions.tmdl not found at {path}. Skipping.")
        return {}, {}

    with open(path, "r", encoding="utf-8-sig") as f:
        content = f.read()

    # Split on expression blocks. Each block starts with the keyword 'expression'
    # followed by the name (quoted or unquoted) and an = sign.
    raw_blocks = re.split(r'(?=^\s*expression\s+)', content, flags=re.MULTILINE)

    expression_queries = {}
    other_expressions = {}

    for block in raw_blocks:
        if not re.match(r'\s*expression\s+', block):
            continue

        # Extract expression name — may be quoted: expression 'My Table' or unquoted: expression MyTable
        name_match = re.match(r"\s*expression\s+'?([^'=\n]+?)'?\s*=", block)
        if not name_match:
            continue
        expr_name = name_match.group(1).strip()

        body = _extract_expression_body(block)
        expr_type = classify_expression_body(body)

        if expr_type == "expression_query":
            expression_queries[expr_name] = {
                "source_layer": "expression_query",
                "partition_name": expr_name,
                "partition_type": "m",
                "mode": "",
                "m_query": body,
                "entity_name": "",
                "expression_source": ""
            }
        else:
            # parameter or function — goes to sidecar only
            other_expressions[expr_name] = {
                "expression_type": expr_type,   # "parameter" or "function"
                "body": body
            }

    return expression_queries, other_expressions


def extract_dependencies_from_m_query(current_table_name, m_query, all_known_tables):
    """
    Casts a 'Regex Net' over the M query to find potential dependencies,
    then filters them against the actual known Universe of Tables.
    The universe now includes both model_table and expression_query entries.
    """
    dependencies = set()

    if not m_query:
        return []

    # Regex 1: Catches M's quoted table references: #"Table Name with Spaces"
    quoted_pattern = re.compile(r'#"(.*?)"(?!\s*=)')

    # Regex 2: Catches standard unquoted table references: TableNameWithoutSpaces
    word_pattern = re.compile(r'\b([a-zA-Z_]\w*)\b(?!\s*=)')

    for match in quoted_pattern.findall(m_query):
        if match in all_known_tables and match != current_table_name:
            dependencies.add(match)

    for match in word_pattern.findall(m_query):
        if match in all_known_tables and match != current_table_name:
            dependencies.add(match)

    return sorted(list(dependencies))


def main():
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)["power_bi_variables"]

    report_name = Path(pbi_variables["report_name"])

    #### Variables
    tables_folder         = Path(pbi_variables["reports_folder"]) / Path(pbi_variables["table_folder_path"])
    expressions_tmdl_path = Path(pbi_variables["reports_folder"]) / report_name / f"{report_name}.SemanticModel" / "definition" / "expressions.tmdl"
    output_dir            = Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"])
    output_json           = output_dir / "all_partitions.json"
    sidecar_json          = output_dir / "parameters_and_functions.json"
    #### Variables END

    # ── Step 1: Harvest model tables from .tmdl files ──────────────────────────
    partitions = extract_partitions(tables_folder)
    print(f"Model tables found (.tmdl files): {len(partitions)}")

    # ── Step 2: Harvest expression queries (and sidecar) from expressions.tmdl ─
    expression_queries, other_expressions = extract_expressions(expressions_tmdl_path)
    print(f"Expression queries found (expressions.tmdl): {len(expression_queries)}")
    print(f"Parameters / functions found (sidecar): {len(other_expressions)}")

    # ── Step 3: Merge into one universe ────────────────────────────────────────
    # Model tables take precedence if a name clash somehow occurs (shouldn't happen in practice)
    combined = {**expression_queries, **partitions}
    print(f"Total entries in combined universe: {len(combined)}")

    # ── Step 4: Build dependency map across the full universe ──────────────────
    all_known_tables = set(combined.keys())

    print("Mapping M query dependencies...")
    for tablename, tableinfo in combined.items():
        partition_type = tableinfo.get("partition_type", "")

        if partition_type == "m":
            m_query = tableinfo.get("m_query", "")
            dependencies = extract_dependencies_from_m_query(tablename, m_query, all_known_tables)
            tableinfo["dependency"] = dependencies
        else:
            tableinfo["dependency"] = []

    # ── Step 5: Write main JSON (model_table + expression_query only) ──────────
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=4, ensure_ascii=False)
    print(f"Main JSON written to: {output_json}")

    # ── Step 6: Write sidecar JSON (parameters + functions) ───────────────────
    if other_expressions:
        with open(sidecar_json, "w", encoding="utf-8") as f:
            json.dump(other_expressions, f, indent=4, ensure_ascii=False)
        print(f"Sidecar JSON written to: {sidecar_json}")

    print(f"\nSummary")
    print(f"  model_table entries    : {sum(1 for v in combined.values() if v['source_layer'] == 'model_table')}")
    print(f"  expression_query entries: {sum(1 for v in combined.values() if v['source_layer'] == 'expression_query')}")
    print(f"  parameters + functions : {len(other_expressions)}")


if __name__ == "__main__":
    main()