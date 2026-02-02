import asyncio
import pandas as pd
import json
from tqdm.asyncio import tqdm_asyncio
import ollama
import os
import re

# custom .py files
import json_cleaner
import utils

# -------------------------------
# Configuration
# -------------------------------
EXCEL_FILE = "object_definitions.csv"
OUTPUT_FOLDER = "lineage_outputs"
MODEL_NAME = "qwen2.5-coder:14b"
MAX_CONCURRENT_REQUESTS = 1  # increase gradually if VRAM allows

# -------------------------------
# Setup
# -------------------------------
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

df_definitions = pd.concat([pd.read_csv("object_definitions.csv"), pd.read_csv("UAT_object_definitions.csv")], ignore_index=True)

df_definitions = df_definitions.query("ObjectType == 'SQL_STORED_PROCEDURE'")
# # 1. Sort so that preferred database comes first
df_definitions = df_definitions.sort_values(by="DatabaseName", ascending=False)
df = df_definitions.drop_duplicates(subset=['Schema', 'Object'], keep='first')


target_schemas = utils.get_target_schemas()
print(f"Filtering for schemas: {target_schemas}")

#Select only PARTICULAR SCHEMA
df = df.query("Schema in @target_schemas")



SEM = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Create ONE async client (important)
client = ollama.AsyncClient()

# -------------------------------
# Async per-row processor
# -------------------------------
async def process_row(index, row, processed_hashes):
    schema_name = str(row["Schema"])
    object_name = str(row["Object"])
    
    # Check if already processed
    current_hash = utils.get_hash_for_object(schema_name, object_name)
    if current_hash in processed_hashes:
        return  # Skip this row
    
    async with SEM:
        try:
            # -------------------------------
            # Clean SQL text
            # -------------------------------
            sql_text = utils.robust_clean_sql(row["definition"])

            # -------------------------------
            # Safe filename
            # -------------------------------
            filename_raw = f"{index}--{schema_name}--{object_name}"
            filename = (
                re.sub(r"[^\w\s-]", "", filename_raw)
                .strip()
                .replace(" ", "_")
                + ".json"
            )
            file_path = os.path.join(OUTPUT_FOLDER, filename)

            # -------------------------------
            # LLM call
            # -------------------------------
            result = await client.generate(
                model=MODEL_NAME,
                prompt = utils.get_lineage_prompt(sql_text),
                format="json",
            )

            # -------------------------------
            # Parse JSON safely
            # -------------------------------
            try:
                response_data = json.loads(result["response"])
            except json.JSONDecodeError:
                response_data = {
                    "error": "Invalid JSON from model",
                    "raw_response": result["response"]
                }

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(response_data, f, indent=4)

        except Exception as e:
            print(f"[ERROR] Row {index} ({row.get('Object', 'UNKNOWN')}): {e}")

# -------------------------------
# Orchestrator
# -------------------------------
async def main():
    # Build set of already processed hashes
    processed_hashes = utils.build_processed_hashes(OUTPUT_FOLDER)
    total_rows = len(df)
    skipped = len([1 for _, row in df.iterrows() 
                   if utils.get_hash_for_object(str(row["Schema"]), str(row["Object"])) in processed_hashes])
    
    print(f"Found {len(processed_hashes)} already processed files")
    print(f"Will process {total_rows - skipped} out of {total_rows} rows")
    
    tasks = [
        process_row(index, row, processed_hashes)
        for index, row in df.iterrows()
    ]

    await tqdm_asyncio.gather(
        *tasks,
        desc="Processing SQL Lineage",
    )

# -------------------------------
# Script entry point
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main())

    print("\n" + "-" * 30)
    print(f"Batch processing complete. Files saved to: {OUTPUT_FOLDER}")
    json_cleaner.clean_json()