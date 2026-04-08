import re
import hashlib
import os
import argparse
import json
import shutil
from pathlib import Path


def robust_clean_sql(sql_query):
    sql_text = str(sql_query)

    sql_text = sql_text.replace('\\n', '\n').replace('\\t', '\t')

    # Remove single-line comments (-- ...)
    sql_text = re.sub(r'--.*', '', sql_text)
    # Remove multi-line comments (/* ... */)
    sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)

    # Replace multiple newlines with a single newline
    sql_text = re.sub(r'\n\s*\n', '\n', sql_text)
    # Collapse horizontal spaces (tabs/spaces) into one space
    sql_text = re.sub(r'[ \t]+', ' ', sql_text)
    
    return sql_text.strip()



def get_hash_for_object(schema_name, object_name):
    """Generate a hash based on schema and object name."""
    combined = f"{schema_name}::{object_name}"
    return hashlib.md5(combined.encode()).hexdigest()



def build_processed_hashes(OUTPUT_FOLDER):
    """Scan output folder and build a set of already processed hashes."""
    processed = set()
    if not os.path.exists(OUTPUT_FOLDER):
        return processed
    
    for filename in os.listdir(OUTPUT_FOLDER):
        if filename.endswith('.json'):
            # Extract hash from filename pattern: {index}--{schema}--{object}.json
            # We'll regenerate the hash from schema and object
            parts = filename.replace('.json', '').split('--')
            if len(parts) >= 3:
                schema = parts[1]
                object_name = parts[2]
                file_hash = get_hash_for_object(schema, object_name)
                processed.add(file_hash)
    
    return processed



def get_lineage_prompt(sql_text):
    # Define the path to your file
    try:
        with open('prompt.txt', 'r', encoding='utf-8') as file:
            template = file.read()
            
        # Inject the SQL using replace (safest for prompts containing JSON)
        final_prompt = template.replace('__SQL_TEXT__', sql_text)
        return final_prompt
        
    except Exception as e:
        print("Error: Prompt file not found: ", e)
        raise



def get_target_schemas(args=None):
    #------------------------ Parse CLI arguments to take schema name ------------------------
    # Initialize the parser
    parser = argparse.ArgumentParser(description="Extract lineage for a specific schema.")

    # 'required=True' ensures the script fails if user fails to provide schema
    parser.add_argument("--schema", nargs='+', required=True, help="List of database schemas (space separated)")
    # Parse the arguments
    parsed_args = parser.parse_args(args)

    return parsed_args.schema


def json_cleaner_workflow(source_dir : str, dest_dir: str ):
    # DELETE all files and folders from destintation directory
    
    # Verify path exists to avoid errors
    dest_dir_path = Path(dest_dir)
    if Path(dest_dir_path).exists():
        for item in dest_dir_path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)  # Deletes a folder and all its contents
            else:
                item.unlink()  # Deletes a file

    # Copies entire directory tree# Copies entire directory tree
    shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
# Define the folder path relative to the current working directory

def clean_json():
    json_cleaner_workflow(source_dir = "../lineage_outputs/", dest_dir = "../lineage_outputs_cleaned/")
    folder_path = "../lineage_outputs/"
    # Check if folder exists to avoid errors
    if os.path.exists(folder_path):
        files_list = os.listdir(folder_path)

        for filename in files_list:
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                
                # 1. Read the existing JSON file
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # 2. Modify the data in memory
                # Check if 'lineage' key exists to be safe
                if "lineage" in data:
                    for item in data["lineage"]:
                        try:
                            # Clean 'source' if it exists
                            if "source" in item:
                                item["source"] = item["source"].replace("[", "").replace("]", "")
                            
                            # Clean 'target' if it exists
                            if "target" in item:
                                item["target"] = item["target"].replace("[", "").replace("]", "")
                        except Exception as e:
                            print(f"Error in {filename}: ", e)

                # 3. Write the cleaned data back to the same file
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=4)
                    
        print(f"Successfully cleaned brackets from files in '{folder_path}'")
    else:
        print(f"Folder '{folder_path}' not found.")