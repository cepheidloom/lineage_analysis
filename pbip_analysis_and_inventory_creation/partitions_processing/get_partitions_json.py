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


def main():
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)["power_bi_variables"]
    
    #### Variables
    tables_folder = Path(pbi_variables["reports_folder"]) / Path(pbi_variables["table_folder_path"])
    output_json = Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "all_partitions.json"
    #### Variables END

    partitions = extract_partitions(tables_folder)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(partitions, f, indent=4, ensure_ascii=False)

    print(f"Total partitions extracted: {len(partitions)}")
    print(f"JSON written to: {output_json}")


if __name__ == "__main__":
    main()