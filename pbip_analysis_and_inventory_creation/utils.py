import json
import yaml
from pathlib import Path
import os
import re

def get_specific_page_tables():
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
            pbi_variables = yaml.safe_load(f)
    report_name     = Path(pbi_variables["power_bi_variables"]["report_name"])
    output_file = Path(pbi_variables["power_bi_variables"]["output_file"])
    VISUALS_LINEAGE_FOLDER        = output_file / report_name / "visuals_lineage"

    list_pages = ["SharePoint_Landscape", "Records_Labelling", "File_Plan", "Sensitivity_Labelling", "Deletions_-_Details", "Dispositions", "Ownership_Metrics"]



    set_all_tables = set()
    for folder in list_pages:
        folder_path = VISUALS_LINEAGE_FOLDER / folder
        for json_file in folder_path.glob("*.json"):
            with open(json_file, "r") as f:
                visual_json_data = json.load(f)
            set_all_tables.update(visual_json_data["tables_used"])
            for lin_pairs in visual_json_data["lineage"]:
                    src_table_name = re.match(r"^[^\[]+", lin_pairs["source"]).group(0)
                    tgt_table_name = re.match(r"^[^\[]+", lin_pairs["target"]).group(0)
                    set_all_tables.add(src_table_name)
                    set_all_tables.add(tgt_table_name)

    print("\n".join(set_all_tables))
        

def get_primary_and_secondary_tables():
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)

    reports_folder     = Path(pbi_variables["power_bi_variables"]["reports_folder"])
    table_folder_path = Path(pbi_variables["power_bi_variables"]["table_folder_path"])
        
    TABLES_FOLDER = reports_folder / table_folder_path
    primary_tables = []
    derived_tables = []

    for tmdl_file in TABLES_FOLDER.glob("*.tmdl"):
        with open(tmdl_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check partition type
        if "= m" in content:
            primary_tables.append(tmdl_file.stem)
        elif "= calculated" in content:
            derived_tables.append(tmdl_file.stem)

    print(f"\n�� PRIMARY TABLES ({len(primary_tables)}):")
    for table in sorted(primary_tables):
        print(f"  - {table}")

    print(f"\n�� DERIVED TABLES ({len(derived_tables)}):")
    for table in sorted(derived_tables):
        print(f"  - {table}")

if __name__ == "__main__":
     get_primary_and_secondary_tables()





####################################################################################################################################
################################# OLD CODE - FOR REFERENCE ONLY(Processing visuals from PBIX file) #################################
####################################################################################################################################
# import zipfile
# import json
# from pathlib import Path
# import re

# # ==========================================
# # CONFIGURATION - EDIT THESE VALUES
# # ==========================================

# PBIX_FILE_PATH = Path(r"")

# OUTPUT_FOLDER = Path("powerbi_jsons")

# # ==========================================
# # HELPER FUNCTIONS
# # ==========================================

# def clean_name_for_id(name: str) -> str:
#     """
#     Cleans a visual title or name to make it suitable as a JSON filename.
#     Removes special characters, replaces spaces with underscores.
#     """
#     # Remove special characters, keep alphanumeric, spaces, underscores, hyphens
#     cleaned = re.sub(r'[^\w\s-]', '', name)
#     # Replace spaces with underscores
#     cleaned = cleaned.replace(' ', '_')
#     # Remove multiple consecutive underscores
#     cleaned = re.sub(r'_+', '_', cleaned)
#     # Strip leading/trailing underscores
#     cleaned = cleaned.strip('_')
#     return cleaned


# def extract_pbix_visuals_to_json(pbix_path: Path, output_folder: Path):
#     """
#     Extracts all visuals from a PBIX file and generates individual JSON files
#     for each visual in the specified output folder.
#     """
#     # Create output folder if it doesn't exist
#     output_folder.mkdir(parents=True, exist_ok=True)
    
#     # Extract PBIX name
#     pbix_file = pbix_path.name
#     pbix_name = pbix_path.stem  # filename without extension
    
#     print(f"\n{'='*70}")
#     print(f"Processing PBIX: {pbix_file}")
#     print(f"Output folder: {output_folder}")
#     print(f"{'='*70}\n")
    
#     # Open and read the PBIX file
#     with zipfile.ZipFile(pbix_path, 'r') as z:
#         with z.open('Report/Layout') as f:
#             layout = json.loads(f.read().decode('utf-16-le'))
    
#     total_visuals = 0
    
#     # Iterate through pages
#     for section in layout.get('sections', []):
#         page_name = section.get('displayName', 'Unknown_Page')
#         page_display_name = page_name
        
#         print(f"\n�� Processing Page: {page_name}")
#         print("-" * 70)
        
#         visual_index = 0
        
#         # Iterate through visuals on this page
#         for visual in section.get('visualContainers', []):
#             config_str = visual.get('config')
#             if not config_str:
#                 continue
            
#             config = json.loads(config_str)
#             single_visual = config.get('singleVisual', {})
#             visual_type = single_visual.get('visualType', 'Unknown')
            
#             # Skip shapes, textboxes, and images
#             if visual_type in ['shape', 'textbox', 'image']:
#                 continue
            
#             visual_index += 1
            
#             # Extract visual title
#             visual_title = "Default Title / No Title"
#             has_custom_title = False
#             vc_objects = single_visual.get('vcObjects', {})
            
#             if 'title' in vc_objects:
#                 try:
#                     raw_title = vc_objects['title'][0]['properties']['text']['expr']['Literal']['Value']
#                     visual_title = raw_title.strip("'")
#                     has_custom_title = True
#                 except KeyError:
#                     pass
            
#             # Extract tables used
#             tables_used = []
#             prototype_query = single_visual.get('prototypeQuery', {})
            
#             for item in prototype_query.get('From', []):
#                 if 'Entity' in item:
#                     tables_used.append(item['Entity'])
            
#             # Extract columns and measures used
#             columns_measures_used = []
#             for item in prototype_query.get('Select', []):
#                 col_name = item.get('Name')
#                 if col_name and isinstance(col_name, str):
#                     columns_measures_used.append(col_name)
            
#             # Generate unique name for this visual (keep human-readable)
#             unique_visual_name = f"{pbix_name} || {page_name} || {visual_title} || {visual_index}"
            
#             # Build lineage relationships
#             lineage = []
#             for table in tables_used:
#                 lineage.append({
#                     "source": f"pbi.{pbix_name}.{table}",
#                     "target": unique_visual_name
#                 })
            
#             # Construct the JSON object
#             visual_json = {
#                 "name": unique_visual_name,
#                 "type": "POWER_BI_VISUAL",
#                 "pbix_file": pbix_file,
#                 "pbix_name": pbix_name,
#                 "page_name": page_name,
#                 "page_display_name": page_display_name,
#                 "visual_title": visual_title,
#                 "visual_type": visual_type,
#                 "visual_index": visual_index,
#                 "has_custom_title": has_custom_title,
#                 "tables_used": tables_used,
#                 "columns_measures_used": columns_measures_used,
#                 "lineage": lineage
#             }
            
#             # Generate filename (clean for filesystem)
#             json_filename = f"{clean_name_for_id(unique_visual_name)}.json"
#             json_filepath = output_folder / json_filename
            
#             # Write JSON file
#             with open(json_filepath, 'w', encoding='utf-8') as f:
#                 json.dump(visual_json, f, indent=4, ensure_ascii=False)
            
#             total_visuals += 1
#             print(f"  ✓ {visual_title} ({visual_type}) → {json_filename}")
    
#     print(f"\n{'='*70}")
#     print(f"✓ Extraction Complete!")
#     print(f"  Total visuals extracted: {total_visuals}")
#     print(f"  JSON files saved to: {output_folder.absolute()}")
#     print(f"{'='*70}\n")


# # ==========================================
# # EXECUTION
# # ==========================================

# if __name__ == "__main__":
#     extract_pbix_visuals_to_json(PBIX_FILE_PATH, OUTPUT_FOLDER)