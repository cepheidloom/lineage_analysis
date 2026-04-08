import re
import json
from pathlib import Path
import yaml

def sanitize_dax(dax_string):
    if not dax_string:
        return ""
    # 1. Remove String Literals ("text")
    dax_string = re.sub(r'"(?:\\.|[^"\\])*"', '', dax_string)
    # 2. Remove Multi-line Comments (/* comment */)
    dax_string = re.sub(r'/\*[\s\S]*?\*/', '', dax_string)
    # 3. Remove Single-line Comments (// comment OR -- comment)
    dax_string = re.sub(r'(//|--).*', '', dax_string)
    return dax_string

def generate_lineage(inventory_file):
    # inventory_file = "model_inventory.json"
    
    try:
        with open(inventory_file, 'r', encoding='utf-8') as f:
            inventory = json.load(f)
    except FileNotFoundError:
        print(f"Could not find {inventory_file}. Run the extractor first!")
        return

    # THE MAGIC REGEX UPDATE: Notice the `?` after the table groups. 
    # This makes the table name OPTIONAL, allowing us to catch orphans.
    fqn_pattern = re.compile(r"(?:(?:'([^']+)'|([a-zA-Z0-9_]+))\s*)?\[([^\]]+)\]")

    # PRO-MOVE: Build an O(1) Lookup Dictionary for Global Measures
    # This prevents us from having to loop through the JSON thousands of times
    global_measures = {}
    for key, item in inventory.items():
        if item.get("type") == "Measure":
            global_measures[item["name"]] = key

    print("Analyzing DAX expressions and matchmaking orphans...")
    updates_made = 0

    for fqn_key, item in inventory.items():
        if "expression" in item and item["expression"]:
            
            clean_dax = sanitize_dax(item["expression"])
            dependencies = set()
            current_table = item["table"]
            
            for match in fqn_pattern.finditer(clean_dax):
                raw_table = match.group(1) or match.group(2)
                raw_name = match.group(3) # The column or measure name inside brackets
                
                clean_name = raw_name.replace("''", "'")
                
                # --- CASE 1: FULLY QUALIFIED NAME ---
                if raw_table:
                    clean_table = raw_table.replace("''", "'")
                    dependency_key = f"{clean_table}[{clean_name}]"
                    
                    if dependency_key != fqn_key: # Don't depend on yourself
                        # THE GHOST HUNTER VALIDATION
                        if dependency_key in inventory:
                            dependencies.add(dependency_key)
                        else:
                            # If it claims to belong to a table we haven't seen, flag it!
                            dependencies.add(f"MISSING_[{dependency_key}]")
                
                # --- CASE 2: THE ORPHAN MATCHMAKER ---
                else:
                    local_key = f"{current_table}[{clean_name}]"
                    
                    # Priority 1: Check Local Table (Is it a column in the current table?)
                    if local_key in inventory:
                        if local_key != fqn_key:
                            dependencies.add(local_key)
                            
                    # Priority 2: Check Global Measures (Is it a measure in another table?)
                    elif clean_name in global_measures:
                        global_key = global_measures[clean_name]
                        if global_key != fqn_key:
                            dependencies.add(global_key)
                            
                    # Priority 3: Dead End (External Dataset or PBI internal object)
                    else:
                        dependencies.add(f"UNRESOLVED_[{clean_name}]")
            
            if dependencies:
                item["depends_on"] = list(dependencies)
                updates_made += 1

    with open(inventory_file, 'w', encoding='utf-8') as f:
        json.dump(inventory, f, indent=4)
        
    print(f"Success! Resolved lineage and matched orphans for {updates_made} items.")

import json

def validate_lineage(inventory_file):
    
    try:
        with open(inventory_file, 'r', encoding='utf-8') as f:
            inventory = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find '{inventory_file}'.")
        return

    # Create a lightning-fast lookup set of every valid key in the model
    valid_keys = set(inventory.keys())
    
    broken_links = []
    total_checks = 0

    print("Running Data Quality Audit on Lineage...\n")

    # Loop through the inventory
    for item_key, item_data in inventory.items():
        dependencies = item_data.get("depends_on", [])
        
        for dep in dependencies:
            total_checks += 1
            
            # The core test: Does this target actually exist in our JSON?
            if dep not in valid_keys:
                broken_links.append({
                    "source": item_key,
                    "target": dep
                })

    # The Final Report
    print(f"Audit Complete! Checked {total_checks} total dependency links.")
    print("-" * 50)
    
    if not broken_links:
        print("✅ PERFECT SCORE: Every single dependency points to a real, existing object in your JSON.")
    else:
        print(f"❌ WARNING: Found {len(broken_links)} broken, external, or unresolved links.\n")
        
        # Print the first 20 broken links so you don't flood your console
        for issue in broken_links[:20]:
            print(f"  [BROKEN LINK]  {issue['source']}")
            print(f"  └──> Points to: {issue['target']}\n")
            
        if len(broken_links) > 20:
            print(f"... and {len(broken_links) - 20} more.")
            

if __name__ == "__main__":
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
            pbi_variables = yaml.safe_load(f)

    report_name     = Path(pbi_variables["power_bi_variables"]["report_name"])
    output_file = Path(pbi_variables["power_bi_variables"]["output_file"])
    inventory_file = output_file / report_name / f"{report_name}_model_inventory.json"
    generate_lineage(inventory_file)
    validate_lineage(inventory_file)