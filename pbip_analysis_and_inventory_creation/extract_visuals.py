import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict
import yaml
import re

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def extract_tables_from_query(query_obj: Any) -> Set[str]:
    """
    Recursively extracts table names from query/dataRole objects.
    Looks for SourceRef.Entity patterns.
    """
    tables = set()
    
    if isinstance(query_obj, dict):
        # Check for SourceRef.Entity pattern
        if "SourceRef" in query_obj and "Entity" in query_obj["SourceRef"]:
            tables.add(query_obj["SourceRef"]["Entity"])
        
        # Recurse through all values
        for value in query_obj.values():
            tables.update(extract_tables_from_query(value))
    
    elif isinstance(query_obj, list):
        for item in query_obj:
            tables.update(extract_tables_from_query(item))
    
    return tables


def extract_columns_from_query(query_obj: Any) -> List[str]:
    """
    Extracts table.column references from query objects.
    Looks for Column expressions with Entity and Property.
    """
    columns = []
    
    if isinstance(query_obj, dict):
        # Check for Column pattern
        if "Column" in query_obj:
            col_obj = query_obj["Column"]
            if "Expression" in col_obj and "Property" in col_obj:
                entity = col_obj["Expression"].get("SourceRef", {}).get("Entity")
                prop = col_obj["Property"]
                if entity and prop:
                    columns.append(f"{entity}[{prop}]")
        
        # Check for Measure pattern
        if "Measure" in query_obj:
            measure_obj = query_obj["Measure"]
            if "Expression" in measure_obj and "Property" in measure_obj:
                entity = measure_obj["Expression"].get("SourceRef", {}).get("Entity")
                prop = measure_obj["Property"]
                if entity and prop:
                    columns.append(f"{entity}[{prop}]")
        
        # Recurse
        for value in query_obj.values():
            columns.extend(extract_columns_from_query(value))
    
    elif isinstance(query_obj, list):
        for item in query_obj:
            columns.extend(extract_columns_from_query(item))
    
    return columns


def extract_aggregations(query_obj: Any) -> Dict[str, str]:
    """
    Extracts aggregation functions (SUM, COUNT, AVERAGE, etc.) from query.
    """
    aggregations = {}
    
    if isinstance(query_obj, dict):
        # Look for Aggregation patterns
        if "Aggregation" in query_obj:
            agg_obj = query_obj["Aggregation"]
            if "Expression" in agg_obj and "Function" in agg_obj:
                # Extract column being aggregated
                cols = extract_columns_from_query(agg_obj.get("Expression", {}))
                agg_func = agg_obj.get("Function")
                if cols and agg_func:
                    for col in cols:
                        aggregations[col] = agg_func
        
        # Recurse
        for value in query_obj.values():
            aggregations.update(extract_aggregations(value))
    
    elif isinstance(query_obj, list):
        for item in query_obj:
            aggregations.update(extract_aggregations(item))
    
    return aggregations


def extract_sort_config(query_obj: Any) -> Optional[Dict]:
    """
    Extracts sort configuration from query.
    """
    if isinstance(query_obj, dict):
        # Look for OrderBy patterns
        if "OrderBy" in query_obj:
            order_by = query_obj["OrderBy"]
            if isinstance(order_by, list) and order_by:
                first_sort = order_by[0]
                direction = first_sort.get("Direction", 1)  # 1=ascending, 2=descending
                
                # Extract column being sorted
                cols = extract_columns_from_query(first_sort.get("Expression", {}))
                if cols:
                    return {
                        "column": cols[0],
                        "order": "ascending" if direction == 1 else "descending"
                    }
        
        # Recurse
        for value in query_obj.values():
            result = extract_sort_config(value)
            if result:
                return result
    
    elif isinstance(query_obj, list):
        for item in query_obj:
            result = extract_sort_config(item)
            if result:
                return result
    
    return None


def categorize_fields(fields: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """
    Categorizes fields into measures, columns, and unknown based on model inventory.
    Returns: (measures, columns, unknown)
    """
    measures = []
    columns = []
    unknown = []
    
    for field in fields:
        field_info = model_inventory.get(field)
        
        if not field_info:
            unknown.append(field)
        elif field_info.get("type") == "Measure":
            measures.append(field)
        elif field_info.get("type") in ["Raw Column", "Calculated Column"]:
            columns.append(field)
        elif field_info.get("type") == "Hierarchy":
            columns.append(field)  # Treat hierarchies as columns
        else:
            unknown.append(field)
    
    return measures, columns, unknown


def map_fields_to_tables(fields: List[str]) -> Dict[str, List[str]]:
    """
    Creates a mapping of tables to their fields.
    Returns: {table_name: [field1, field2, ...]}
    """
    mapping = defaultdict(list)
    
    for field in fields:
        if "[" in field:
            table_name = field.split("[")[0]
            mapping[table_name].append(field)
    
    return dict(mapping)


def parse_data_roles(visual_data: Dict) -> Dict[str, List[str]]:
    """
    Extracts data role mappings (Category, Values, X, Y, Legend, etc.)
    from the visual's query/dataRoles configuration.
    """
    data_roles = {}
    
    # Try to find dataRoles in visual config
    query_data = visual_data.get("query", {})
    
    # Different visual types structure this differently
    for key in ["dataRoles", "DataRoles", "projections"]:
        if key in query_data:
            role_data = query_data[key]
            if isinstance(role_data, list):
                for role in role_data:
                    role_name = role.get("name", "Unknown")
                    items = role.get("items", [])
                    columns = []
                    for item in items:
                        cols = extract_columns_from_query(item)
                        columns.extend(cols)
                    if columns:
                        data_roles[role_name] = columns
    
    return data_roles


def extract_visual_filters(visual_data: Dict) -> List[Dict]:
    """
    Extracts visual-level filters (different from page filters).
    """
    filters = []
    
    # Look in multiple possible locations
    visual_obj = visual_data.get("visual", {})
    
    # Check filters in visual.filters
    if "filters" in visual_obj:
        filter_str = visual_obj.get("filters")
        if filter_str:
            try:
                filter_obj = json.loads(filter_str) if isinstance(filter_str, str) else filter_str
                if isinstance(filter_obj, list):
                    for f in filter_obj:
                        filters.append({
                            "type": f.get("type", "Unknown"),
                            "target": str(f.get("target", "Unknown"))
                        })
            except:
                pass
    
    # Check in query
    query = visual_data.get("query", {})
    if "Where" in query:
        filters.append({
            "type": "query_filter",
            "details": "Has WHERE clause in query"
        })
    
    return filters


def parse_filters(filter_list: List[Dict]) -> List[Dict]:
    """
    Parses filter configurations into readable format.
    """
    parsed_filters = []
    
    for f in filter_list:
        filter_info = {
            "name": f.get("displayName", f.get("name", "Unknown")),
            "type": f.get("type", "Unknown")
        }
        
        # Try to extract target column
        field = f.get("field", {})
        if "Column" in field:
            col_obj = field["Column"]
            entity = col_obj.get("Expression", {}).get("SourceRef", {}).get("Entity")
            prop = col_obj.get("Property")
            if entity and prop:
                filter_info["target"] = f"{entity}[{prop}]"
        
        parsed_filters.append(filter_info)
    
    return parsed_filters


def extract_visual_info(page_folder: Path, page_name: str, page_display_name: str) -> List[Dict]:
    """
    Extracts all visuals from a page folder with enhanced information.
    """
    visuals = []
    visuals_folder = page_folder / "visuals"
    
    if not visuals_folder.exists():
        return visuals
    
    visual_index = 0
    
    for visual_folder in visuals_folder.iterdir():
        if not visual_folder.is_dir():
            continue
        
        visual_json_path = visual_folder / "visual.json"
        if not visual_json_path.exists():
            continue
        
        visual_index += 1
        visual_id = visual_folder.name  # Hash ID like "00ee772102a0e25843dbb"
        
        with open(visual_json_path, 'r', encoding='utf-8') as f:
            visual_data = json.load(f)
        
        # Extract basic info
        visual_obj = visual_data.get("visual", {})
        visual_type = visual_obj.get("visualType", "Unknown")
        
        # Skip decorative visuals
        if visual_type in ['shape', 'textbox', 'image']:
            continue
        
        # Extract title
        visual_title = "Default Title / No Title"
        has_custom_title = False
        
        title_obj = visual_data.get("visual", {}).get("visualContainerObjects", {}).get("title", [])
        if title_obj:
            for title_prop in title_obj:
                props = title_prop.get("properties", {})
                if "text" in props:
                    text_expr = props["text"].get("expr", {}).get("Literal", {}).get("Value", "")
                    if text_expr and text_expr != "''":
                        visual_title = text_expr.strip("'")
                        has_custom_title = True
        
        # Extract position
        position = visual_data.get("position", {})
        position_info = {
            "x": position.get("x"),
            "y": position.get("y"),
            "width": position.get("width"),
            "height": position.get("height"),
            "z_index": position.get("z")
        }
        
        # Check if hidden
        is_hidden = False
        visual_header = visual_data.get("visual", {}).get("visualContainerObjects", {}).get("visualHeader", [])
        if visual_header:
            for header_prop in visual_header:
                show_prop = header_prop.get("properties", {}).get("show", {}).get("expr", {}).get("Literal", {}).get("Value")
                if show_prop == "false":
                    is_hidden = True
        
        # Extract tables and columns from query
        tables_used = extract_tables_from_query(visual_obj)
        columns_measures_raw = extract_columns_from_query(visual_obj)
        
        # Remove duplicates
        columns_measures_raw = sorted(list(set(columns_measures_raw)))
        
        # Categorize fields
        measures, columns, unknown = categorize_fields(columns_measures_raw)
        
        # Map fields to tables
        field_to_table_map = map_fields_to_tables(columns_measures_raw)
        
        # Find tables that are referenced but have no explicit fields
        all_tables_from_fields = set(field_to_table_map.keys())
        tables_without_fields = sorted(list(tables_used - all_tables_from_fields))
        
        # Extract aggregations
        aggregations = extract_aggregations(visual_obj)
        
        # Extract sort configuration
        sort_config = extract_sort_config(visual_obj)
        
        # Extract visual-level filters
        visual_filters = extract_visual_filters(visual_data)
        
        # Extract data roles
        data_roles = parse_data_roles(visual_obj)
        
        # Generate unique name
        unique_name = f"{REPORT_NAME} || {page_display_name} || {visual_title} || {visual_index}"
        
        # Construct visual info
        visual_info = {
            "visual_id": visual_id,
            "name": unique_name,
            "type": "POWER_BI_VISUAL",
            "report_name": REPORT_NAME,
            "page_name": page_name,
            "page_display_name": page_display_name,
            "visual_title": visual_title,
            "visual_type": visual_type,
            "visual_index": visual_index,
            "has_custom_title": has_custom_title,
            "is_hidden": is_hidden,
            "position": position_info,
            
            # Enhanced field categorization
            "tables_used": sorted(list(tables_used)),
            "measures_used": measures,
            "columns_used": columns,
            "unknown_fields": unknown,
            "tables_without_explicit_fields": tables_without_fields,
            
            # Field to table mapping
            "table_field_mapping": field_to_table_map,
            
            # Aggregations
            "aggregations": aggregations if aggregations else None,
            
            # Sort configuration
            "sort_by": sort_config,
            
            # Filters
            "visual_filters": visual_filters if visual_filters else None,
            
            # Data roles
            "data_roles": data_roles if data_roles else None,
        }
        
        visuals.append(visual_info)
    
    return visuals


def extract_all_visuals(FINAL_OUTPUT_FOLDER, PAGE_FOLDER):
    """
    Main function to extract all visuals from all pages.
    """
    FINAL_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)
    
    all_visuals = []
    page_count = 0
    visual_count = 0
    
    print(f"{'='*70}")
    print(f"EXTRACTING VISUALS FROM PBIP")
    print(f"{'='*70}\n")
    
    # Iterate through page folders
    for page_folder in PAGE_FOLDER.iterdir():
        if not page_folder.is_dir():
            continue
        
        page_json_path = page_folder / "page.json"
        if not page_json_path.exists():
            continue
        
        page_count += 1
        
        # Read page metadata
        with open(page_json_path, 'r', encoding='utf-8') as f:
            page_data = json.load(f)
        
        page_name = page_data.get("name", "Unknown")
        page_display_name = page_data.get("displayName", page_name)
        
        print(f">> Processing Page: {page_display_name}")
        
        # Extract page-level filters
        page_filters = []
        filter_config = page_data.get("filterConfig", {})
        if "filters" in filter_config:
            page_filters = parse_filters(filter_config["filters"])
        
        # Extract visuals from this page
        page_visuals = extract_visual_info(page_folder, page_name, page_display_name)
        
        # Add page filters to each visual
        for visual in page_visuals:
            visual["page_filters"] = page_filters
        
        all_visuals.extend(page_visuals)
        visual_count += len(page_visuals)
        
        print(f"   Found {len(page_visuals)} visuals")
    
    # Save individual JSON files
    for visual in all_visuals:
        # Clean filename
        raw_title = visual["visual_title"]
        clean_title = raw_title.replace(" ", "_")
        clean_title = re.sub(r'[<>:"/\\|?*]', '_', clean_title)
        clean_title = clean_title[:60].strip('_')
        filename = f"{clean_title}__{visual["visual_id"]}.json"  # Use visual ID to ensure uniqueness

        safe_page_name = visual["page_display_name"].replace(" ", "_").replace("/", "_")
        (FINAL_OUTPUT_FOLDER / safe_page_name).mkdir(parents=True, exist_ok=True)

        output_path = FINAL_OUTPUT_FOLDER / safe_page_name / filename
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(visual, f, indent=4, ensure_ascii=False)
    
    print(f"\n{'='*70}")
    print(f"Extraction Complete!")
    print(f"  Pages processed: {page_count}")
    print(f"  Visuals extracted: {visual_count}")
    print(f"  JSON files saved to: {FINAL_OUTPUT_FOLDER.absolute()}")
    print(f"{'='*70}\n")


# ==========================================
# EXECUTION
# ==========================================

if __name__ == "__main__":
    
    # ==========================================
    # CONFIGURATION
    # ==========================================
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
            pbi_variables = yaml.safe_load(f)
    PBIP_FOLDER = pbi_variables["power_bi_variables"]["reports_folder"]
    REPORT_NAME = pbi_variables["power_bi_variables"]["report_name"]
    
    PBIP_REPORT_FOLDER = Path(f"{REPORT_NAME}/{REPORT_NAME}.Report")    
    PAGE_FOLDER = PBIP_FOLDER / PBIP_REPORT_FOLDER / "definition" / "pages"
    

    OUTPUT_FOLDER = Path(pbi_variables["power_bi_variables"]["output_file"])
    FINAL_OUTPUT_FOLDER = OUTPUT_FOLDER / REPORT_NAME / "visuals"

    MODEL_INVENTORY_PATH = Path(OUTPUT_FOLDER / REPORT_NAME / (REPORT_NAME + "_model_inventory.json"))

    # ==========================================
    # LOAD MODEL INVENTORY
    # ==========================================

    print("Loading model inventory...")
    with open(MODEL_INVENTORY_PATH, 'r', encoding='utf-8') as f:
        model_inventory = json.load(f)
    print(f"Loaded {len(model_inventory)} column/measure definitions\n")


    extract_all_visuals(FINAL_OUTPUT_FOLDER, PAGE_FOLDER)