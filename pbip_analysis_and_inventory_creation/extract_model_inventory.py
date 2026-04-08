import os
import re
import json
from pathlib import Path
import yaml

column_detection_pattern = r"^\s*column\s+(?:'([^']+)'|([^\s=]+))\s*$"
calculated_column_detection_pattern = r"^\s*column\s+(?:'([^']+)'|([^\s=]+))\s*=\s*(?:```([\s\S]*?)```|(.+))"
measure_detection_pattern = r"^\s*measure\s+(?:'([^']+)'|([^\s=]+))\s*=\s*([\s\S]*?)(?=\r?\n\s*(?:formatString:|lineageTag:|displayFolder:|description:|isHidden|summarizeBy:|sortByColumn:|dataCategory:|detailRowsDefinition:|annotation|measure\b|column\b|table\b)|\Z)"
table_name_detection_pattern = r"^\s*table\s+(?:'([^']+)'|([^\s=]+))"
heirarchy_detection_pattern = r"^\s*hierarchy\s+(?:'([^']+)'|([^\s=]+))\s*([\s\S]*?)(?=\r?\n\s*(?:hierarchy\b|measure\b|column\b(?!:)|table\b|partition\b|calculationGroup\b)|\Z)"

def generate_model_inventory(folder_path, report_name , output_file ):
    
    # Your exact Regex patterns, compiled with MULTILINE and IGNORECASE
    col_pattern = re.compile(
        r"^\s*column\s+(?:'([^']+)'|([^\s=]+))\s*$", 
        re.MULTILINE | re.IGNORECASE
    )
    
    calc_col_pattern = re.compile(
        r"^\s*column\s+(?:'([^']+)'|([^\s=]+))\s*=\s*(?:```([\s\S]*?)```|(.+))", 
        re.MULTILINE | re.IGNORECASE
    )
    
    measure_pattern = re.compile(
        r"^\s*measure\s+(?:'([^']+)'|([^\s=]+))\s*=\s*([\s\S]*?)(?=\r?\n\s*(?:formatString:|lineageTag:|displayFolder:|description:|isHidden|summarizeBy:|sortByColumn:|dataCategory:|detailRowsDefinition:|annotation|measure\b|column\b|table\b)|\Z)", 
        re.MULTILINE | re.IGNORECASE
    )

    hierarchy_pattern = re.compile(
        r"^\s*hierarchy\s+(?:'([^']+)'|([^\s=]+))\s*([\s\S]*?)(?=\r?\n\s*(?:hierarchy\b|measure\b|column\b(?!:)|table\b|partition\b|calculationGroup\b)|\Z)",
        re.MULTILINE | re.IGNORECASE
    )
    # The master dictionary that will become our JSON
    inventory = {}

    print(f"Scanning directory: {folder_path}...")

    # Ensure the directory exists before attempting to read
    if not os.path.exists(folder_path):
        print(f"Error: Could not find the path '{folder_path}'")
        return

    # Iterate through every .tmdl file in the folder
    for filename in os.listdir(folder_path):
        if not filename.endswith('.tmdl'):
            continue
            
        filepath = os.path.join(folder_path, filename)
        
        # Assume the file name (minus .tmdl) is the table name
        # The Table Regex (compiled at the top of your script)
        table_pattern = re.compile(r"^\s*table\s+(?:'([^']+)'|([^\s=]+))", re.MULTILINE | re.IGNORECASE)

        # ... inside your file reading loop ...
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # Grab the TRUE table name from the file content
        table_match = table_pattern.search(content)
        if table_match:
            raw_table_name = table_match.group(1) or table_match.group(2)
            table_name = raw_table_name.replace("''", "'")
        else:
            # Fallback just in case the file is empty or corrupted
            table_name = filename.replace('.tmdl', '')

        # 1. Extract Raw Columns
        for match in col_pattern.finditer(content):
            raw_name = match.group(1) or match.group(2)
            clean_name = raw_name.replace("''", "'")
            fqn = f"{table_name}[{clean_name}]"
            
            inventory[fqn] = {
                "type": "Raw Column",
                "table": table_name,
                "name": clean_name
            }
            
        # 2. Extract Calculated Columns
        for match in calc_col_pattern.finditer(content):
            raw_name = match.group(1) or match.group(2)
            clean_name = raw_name.replace("''", "'")
            fqn = f"{table_name}[{clean_name}]"
            
            # Group 3 is multiline (inside backticks), Group 4 is single line
            raw_dax = match.group(3) or match.group(4)
            clean_dax = raw_dax.strip() if raw_dax else ""
            
            inventory[fqn] = {
                "type": "Calculated Column",
                "table": table_name,
                "name": clean_name,
                "expression": clean_dax
            }
            
        # 3. Extract Measures
        for match in measure_pattern.finditer(content):
            raw_name = match.group(1) or match.group(2)
            clean_name = raw_name.replace("''", "'")
            fqn = f"{table_name}[{clean_name}]"
            
            raw_dax = match.group(3)
            # Strip out leading/trailing whitespace, and remove the triple backticks if the regex vacuumed them up
            clean_dax = raw_dax.strip().strip("`").strip() if raw_dax else ""
            
            inventory[fqn] = {
                "type": "Measure",
                "table": table_name,
                "name": clean_name,
                "expression": clean_dax
            }

        # 4. Extract Hierarchies
        for match in hierarchy_pattern.finditer(content):
            raw_name = match.group(1) or match.group(2)
            clean_name = raw_name.replace("''", "'")
            
            # Group 3 holds the giant block of levels and lineage tags
            raw_content = match.group(3).strip() if match.group(3) else ""
            
            fqn = f"{table_name}[{clean_name}]"
            
            inventory[fqn] = {
                "type": "Hierarchy",
                "table": table_name,
                "name": clean_name,
                "raw_content": raw_content
            }

    Path(output_file).mkdir(parents=True, exist_ok=True) # Ensure the directory exists 
    # Save the massive dictionary to a JSON file
    with open(output_file / Path(f"{report_name}_model_inventory.json"), 'w', encoding='utf-8') as f:
        json.dump(inventory, f, indent=4)
        
    print(f"Success! Extracted {len(inventory)} total fields.")
    print(f"Inventory saved to {output_file}")

if __name__ == "__main__":
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)

    reports_folder     = Path(pbi_variables["power_bi_variables"]["reports_folder"])
    table_folder_path = Path(pbi_variables["power_bi_variables"]["table_folder_path"])
    
    # folder_path` variable here is the complete relative or absolute path to the "tables" folder that contains all the .tmdl files for a specific Power BI report.
    folder_path = reports_folder / table_folder_path
    report_name = Path(pbi_variables["power_bi_variables"]["report_name"])
    output_file = Path(pbi_variables["power_bi_variables"]["output_file"]) / report_name

    generate_model_inventory(folder_path, report_name, output_file)