import re
import json
from pathlib import Path
import yaml


def extract_partitions(tables_folder):
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

            # Extract partition name and type (m, calculated, etc.)
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

            # Extract entity name
            entity_name = ""
            expression_source = ""
            if partition_type == "entity":
                en_match = re.search(r'^\s*entityName:\s*(.+)', partition, flags=re.MULTILINE)
                es_match = re.search(r'^\s*expressionSource:\s*(.+)', partition, flags=re.MULTILINE)
                entity_name = en_match.group(1).strip() if en_match else ""
                expression_source = es_match.group(1).strip().strip("'") if es_match else ""

            all_partitions[tmdl_name] = {
                "partition_name": partition_name,
                "partition_type": partition_type,
                "mode": mode,
                "m_query": m_query,
                "entity_name": entity_name,           # populated for entity type
                "expression_source": expression_source  # the AS connection reference
            }

    return all_partitions



def extract_dependencies_from_m_query(m_query, all_table_names):
    """
    Extract dependency table names from the provided m_query string.
    This uses a simple regex to find plausible references to other tables in the query.
    """
    dependencies = set()
    # Check for table reference patterns
    # Typical PBI table use: TableName[Column] or TableName, 'Table Name', etc.
    # This can be improved, but as a starting point:
    # look for usages like: TableName[xxx], 'Table Name'[xxx], #"Table Name"
    pattern = re.compile(r"""(\b[\w\d_]+)\s*\[ # TableName[Column]
                             |  ['"]([^'"]+)['"]\s*\[  # 'Table Name'[Column]
                             |  #"(.*?)"  # #"Table Name"
                          """, re.VERBOSE)
    
    for match in pattern.finditer(m_query):
        for i in range(1, 4):
            table_candidate = match.group(i)
            if not table_candidate:
                continue
            # Normalize table candidate: remove leading/trailing strange chars, keep as in all_table_names
            candidate = table_candidate.strip(" '#\"[]")
            if candidate in all_table_names:
                dependencies.add(candidate)
    
    # Also catch "let X = Y" alias assignments referencing other table names
    # e.g., acps_Sheet = Source{[Item="acps",Kind="Sheet"]}[Data]
    let_table_pattern = re.compile(r"(?:let|=)\s*([A-Za-z_][\w\d_]*)\s*=")
    # Skipped in this demo, but you can expand if your data model commonly uses such assignments
    
    return sorted(list(dependencies))


def main():
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)["power_bi_variables"]
    
    #### Variables
    tables_folder = Path(pbi_variables["reports_folder"]) / Path(pbi_variables["table_folder_path"])
    output_json = Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "all_partitions.json"
    #### Variables END

    partitions = extract_partitions(tables_folder)

    # Get set of all table names (keys in all_partitions)
    all_table_names = set(partitions.keys())
    for tablename, tableinfo in partitions.items():
        m_query = tableinfo.get("m_query", "")
        dependencies = extract_dependencies_from_m_query(m_query, all_table_names)
        tableinfo["dependency"] = dependencies

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(partitions, f, indent=4, ensure_ascii=False)

    print(f"Total partitions extracted: {len(partitions)}")
    print(f"JSON written to: {output_json}")


if __name__ == "__main__":
    main()