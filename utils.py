import re
import hashlib
import os
import argparse


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