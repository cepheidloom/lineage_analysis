import asyncio
import pandas as pd
import json
import os
import re
from tqdm.asyncio import tqdm_asyncio
import ollama

# -------------------------------
# Configuration
# -------------------------------
EXCEL_FILE = "object_definitions.csv"
OUTPUT_FOLDER = "lineage_outputs"
MODEL_NAME = "qwen2.5-coder:7b"
MAX_CONCURRENT_REQUESTS = 1  # increase gradually if VRAM allows

# -------------------------------
# Setup
# -------------------------------
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

df = pd.read_csv(EXCEL_FILE)
df = df[df["ObjectType"] == "SQL_STORED_PROCEDURE"]

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

SEM = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Create ONE async client (important)
client = ollama.AsyncClient()

# -------------------------------
# Async per-row processor
# -------------------------------
async def process_row(index, row):
    async with SEM:
        try:
            # -------------------------------
            # Clean SQL text
            # -------------------------------
            sql_text = robust_clean_sql(row["definition"])

            schema_name = str(row["Schema"])
            object_name = str(row["Object"])

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
#                 prompt=f"""
# You are a SQL lineage extractor.
# Return ONLY valid JSON.
# No explanations.

# JSON schema:
# {{
#   "source": [],
#   "target": []
# }}

# SQL:
# {sql_text}
# """
prompt= f"""
You are a SQL data lineage extractor.

TASK:
Extract ALL source-to-target data object mappings from the SQL.

STRICT RULES (MANDATORY):
- Output ONLY valid JSON
- No explanations
- No comments
- No markdown

OBJECT IDENTIFICATION RULES:
- A valid source or target must be a real data object:
  - schema.table
  - database.schema.table
  - [schema].[table]

- DO NOT include SQL table hints:
  - IGNORE and REMOVE: NOLOCK, (NOLOCK), WITH (NOLOCK)

- DO NOT treat SQL keywords or hints as schemas


PAIRING RULES:
- Each source MUST be paired with exactly one target
- DO NOT group sources
- DO NOT group targets
- One JSON object per source → target relationship
- If multiple sources write to the same target, repeat the target

TEMP TABLE RULES:
- Temp tables (#table) are INTERMEDIATE objects
- DO NOT use temp tables as final targets
- If a temp table feeds a permanent table, map source → permanent table
- Use temp tables ONLY if no permanent target exists


STORED PROCEDURE RULES:
- Do NOT treat stored procedure names as source tables
- Extract underlying base tables used inside the procedure
- Final lineage must represent table-to-table movement


REQUIRED OUTPUT FORMAT:

{{
  "lineage": [
    {{
      "source": "string",
      "target": "string"
    }}
  ]
}}

EXAMPLE:
If SQL reads from A, B (WITH NOLOCK) and inserts into C,
output MUST be:
{{
  "lineage": [
    {{ "source": "A", "target": "C" }},
    {{ "source": "B", "target": "C" }}
  ]
}}

SQL:
{sql_text}
""",
format="json",
# options={
#         "num_ctx": 4096,  # Limits memory used by the "memory" of the prompt
#         "num_gpu": 15,    # Forces ~15 layers onto 4GB RTX 3050
#         "temperature": 0  # Best for data extraction (more deterministic)
#     }
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
    tasks = [
        process_row(index, row)
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
