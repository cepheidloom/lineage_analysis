import json
from pathlib import Path
from typing import Set
import yaml


# ==========================================
# FUNCTIONS
# ==========================================

def build_column_lineage(column_name: str, model_inventory: dict, visited: Set[str] = None) -> list:
    """
    Recursively builds column-to-column lineage within Power BI.
    Returns list of {"source": ..., "target": ...} edges.
    """
    if visited is None:
        visited = set()
    
    if column_name in visited:
        return []
    
    visited.add(column_name)
    lineage_edges = []
    
    col_info = model_inventory.get(column_name)
    
    if not col_info:
        return []
    
    col_type = col_info.get("type")
    
    if col_type == "Raw Column" or col_type == "Hierarchy":
        return []
    
    dependencies = col_info.get("depends_on", [])
    
    for dep in dependencies:
        lineage_edges.append({
            "source": dep,
            "target": column_name
        })
        
        sub_lineage = build_column_lineage(dep, model_inventory, visited.copy())
        lineage_edges.extend(sub_lineage)
    
    return lineage_edges


def get_sql_to_powerbi_lineage(raw_columns: list, sql_pbi_lookup: dict) -> list:
    """
    For each raw column, create SQL table -> PBI column mapping.
    """
    lineage = []
    
    for column in raw_columns:
        if "[" not in column:
            continue
        
        table_name = column.split("[")[0]
        sql_source = sql_pbi_lookup.get(table_name)
        
        if sql_source:
            lineage.append({
                "source": sql_source,
                "target": column
            })
    
    return lineage


# ==========================================
# MAIN PROCESSING
# ==========================================

if __name__ == "__main__":
    # ==========================================
    # CONFIGURATION
    # ==========================================
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
            pbi_variables = yaml.safe_load(f)

    report_name     = Path(pbi_variables["power_bi_variables"]["report_name"])
    output_file = Path(pbi_variables["power_bi_variables"]["output_file"])
    MODEL_INVENTORY_PATH = output_file / report_name / f"{report_name}_model_inventory.json"
    VISUALS_FOLDER       = output_file / report_name / "visuals"
    OUTPUT_FOLDER        = output_file / report_name / "visuals_lineage"
    # Toggle: Include SQL to Power BI lineage
    INCLUDE_SQL_LINEAGE = False  # Set to False to skip SQL mapping
    SQL_TO_POWERBI_MAPPING = Path("sql_to_powerbi.json")  # Optional


    print(f"\n{'='*70}")
    print(f"BUILDING POWER BI VISUAL LINEAGE")
    print(f"{'='*70}\n")
    
    # Load model inventory
    print("Loading model inventory...")
    with open(MODEL_INVENTORY_PATH, 'r', encoding='utf-8') as f:
        model_inventory = json.load(f)
    print(f"Loaded {len(model_inventory)} column/measure definitions")
    
    # Load SQL to Power BI mapping (if enabled)
    sql_pbi_lookup = {}
    if INCLUDE_SQL_LINEAGE:
        if SQL_TO_POWERBI_MAPPING.exists():
            print("Loading SQL to Power BI mapping...")
            with open(SQL_TO_POWERBI_MAPPING, 'r', encoding='utf-8') as f:
                sql_pbi_data = json.load(f)
            
            sql_pbi_mapping_list = sql_pbi_data.get("lineage", [])
            for mapping in sql_pbi_mapping_list:
                src = mapping.get("source")
                tgt = mapping.get("target")
                if src and tgt:
                    sql_pbi_lookup[tgt] = src
            print(f"Loaded {len(sql_pbi_lookup)} SQL to Power BI mappings")
        else:
            print(f"Warning: SQL mapping file not found at {SQL_TO_POWERBI_MAPPING}")
            print("Skipping SQL lineage...")
            INCLUDE_SQL_LINEAGE = False
    else:
        print("Skipping SQL lineage (INCLUDE_SQL_LINEAGE = False)")
    
    print()
    
    # Create output folder
    OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    
    # Process all visual JSON files recursively
    visual_files = list(VISUALS_FOLDER.rglob("*.json"))
    print(f"Found {len(visual_files)} visual JSON files\n")
    
    processed_count = 0
    
    for visual_file in visual_files:
        try:
            with open(visual_file, 'r', encoding='utf-8') as f:
                visual_data = json.load(f)
            
            visual_name = visual_data.get("name", "Unknown")
            
            # Get all fields used (measures + columns)
            measures_used = visual_data.get("measures_used", [])
            columns_used = visual_data.get("columns_used", [])
            all_fields = measures_used + columns_used
            
            # 1. Build visual to field lineage
            visual_to_field = []
            for field in all_fields:
                visual_to_field.append({
                    "source": field,
                    "target": visual_name
                })
            
            # 2. Build column-to-column lineage (Power BI internal)
            all_column_lineage = []
            for field in all_fields:
                lineage_chain = build_column_lineage(field, model_inventory)
                all_column_lineage.extend(lineage_chain)
            
            # Remove duplicates
            seen = set()
            unique_column_lineage = []
            for edge in all_column_lineage:
                edge_key = (edge["source"], edge["target"])
                if edge_key not in seen:
                    seen.add(edge_key)
                    unique_column_lineage.append(edge)
            
            # 3. Find raw columns (leaf nodes)
            all_sources = set(edge["source"] for edge in unique_column_lineage)
            all_targets = set(edge["target"] for edge in unique_column_lineage)
            leaf_columns = all_sources - all_targets
            
            # Also check if any fields used are themselves raw
            for field in all_fields:
                col_info = model_inventory.get(field)
                if col_info and col_info.get("type") == "Raw Column":
                    leaf_columns.add(field)
            
            # 4. Build SQL to Power BI lineage (if enabled)
            sql_to_powerbi = []
            if INCLUDE_SQL_LINEAGE:
                sql_to_powerbi = get_sql_to_powerbi_lineage(list(leaf_columns), sql_pbi_lookup)
            
            # 5. Add lineage fields to visual
            visual_data["visual_to_field_lineage"] = visual_to_field
            visual_data["lineage"] = unique_column_lineage
            if INCLUDE_SQL_LINEAGE:
                visual_data["sql_to_powerbi_lineage"] = sql_to_powerbi
            
            # Save updated visual (preserve folder structure)
            relative_path = visual_file.relative_to(VISUALS_FOLDER)
            output_path = OUTPUT_FOLDER / relative_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(visual_data, f, indent=4, ensure_ascii=False)
            
            processed_count += 1
            
            if processed_count % 10 == 0:
                print(f"Processed {processed_count}/{len(visual_files)} visuals...")
        
        except Exception as e:
            print(f"Error processing {visual_file.name}: {e}")
    
    print(f"\n{'='*70}")
    print(f"Complete!")
    print(f"  Visuals processed: {processed_count}")
    print(f"  Output location: {OUTPUT_FOLDER.absolute()}")
    print(f"{'='*70}\n")