import asyncio
import pandas as pd
import json
import os
import re
import hashlib
from tqdm.asyncio import tqdm_asyncio
import ollama

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

def build_processed_hashes():
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
    current_hash = get_hash_for_object(schema_name, object_name)
    if current_hash in processed_hashes:
        return  # Skip this row
    
    async with SEM:
        try:
            # -------------------------------
            # Clean SQL text
            # -------------------------------
            sql_text = robust_clean_sql(row["definition"])

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
prompt = f"""
You are a SQL data lineage extractor specializing in T-SQL stored procedures.

OBJECTIVE:
Extract DIRECT source-to-target mappings between PERSISTENT database objects ONLY.
Trace data flow through ALL intermediate steps (temp tables, CTEs, subqueries) but report only the FINAL persistent objects.

OBJECT CLASSIFICATION:

PERSISTENT OBJECTS (Report these):
- Tables: schema.table, [schema].[table], database.schema.table
- Views: schema.view, [schema].[view]
- Stored procedures (when used as data sources via EXEC INSERT)

INTERMEDIATE OBJECTS (Trace through, but DO NOT report):
- Temp tables: #temp, ##global_temp
- Table variables: @table
- CTEs: WITH cte_name AS (...)
- Subqueries and derived tables
- Variables: @variable

EXTRACTION RULES:

1. TRACE THROUGH INTERMEDIATES:
   - If temp table #T is populated from table A, then #T is inserted into table B
   - Report: A → B (not A → #T or #T → B)

2. HANDLE MULTI-STEP FLOWS:
   - Step 1: A → #temp1
   - Step 2: #temp1 → #temp2  
   - Step 3: #temp2 → B
   - Report: A → B

3. MULTIPLE SOURCES TO ONE TARGET:
   - Create separate lineage entries for each source
   - Example: A → C, B → C (two separate JSON objects)

4. ONE SOURCE TO MULTIPLE TARGETS:
   - Create separate lineage entries for each target
   - Example: A → X, A → Y (two separate JSON objects)

5. COMPLEX QUERIES:
   - Trace through all JOINs, subqueries, CTEs
   - Extract base tables from nested SELECT statements
   - Follow data flow through UNION, EXCEPT, INTERSECT operations

6. IGNORE:
   - Table hints: (NOLOCK), WITH (NOLOCK), (INDEX=...), etc.
   - System tables/views unless explicitly part of business logic
   - The stored procedure name itself as a source

7. DELETE/TRUNCATE OPERATIONS:
   - These affect targets but have no sources
   - Omit from lineage (or include with "source": null if you need to track modifications)

8. EXEC STORED PROCEDURES:
   - If "INSERT INTO table EXEC stored_proc", treat stored_proc as a source
   - Otherwise, you may need to trace into that procedure separately

OUTPUT FORMAT:

{{
  "lineage": [
    {{
      "source": "schema.table_name",
      "target": "schema.table_name"
    }}
  ]
}}

RULES ENFORCEMENT:
✓ Output ONLY valid JSON
✓ No explanations, comments, or markdown
✓ No temp tables (#temp) in final output
✓ No CTEs or table variables in final output
✓ Each lineage pair must have exactly one source and one target
✓ Use fully qualified names when available (schema.table)
✓ Remove all table hints from object names

EXAMPLE:

Given SQL:
```sql
-- Step 1: Read from A, B into temp
SELECT * INTO #temp FROM A JOIN B ON A.id = B.id

-- Step 2: Read from #temp and C into final table
INSERT INTO Z 
SELECT * FROM #temp JOIN C ON #temp.id = C.id

Correct output:
{{
  "lineage": [
    {{"source": "A", "target": "Z"}},
    {{"source": "B", "target": "Z"}},
    {{"source": "C", "target": "Z"}}
  ]
}}

Incorrect output (DO NOT DO THIS):
{{
  "lineage": [
    {{"source": "A", "target": "#temp"}},
    {{"source": "B", "target": "#temp"}},
    {{"source": "#temp", "target": "Z"}},
    {{"source": "C", "target": "Z"}}
  ]
}}

SQL TO ANALYZE:
{sql_text}
""",
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
    processed_hashes = build_processed_hashes()
    total_rows = len(df)
    skipped = len([1 for _, row in df.iterrows() 
                   if get_hash_for_object(str(row["Schema"]), str(row["Object"])) in processed_hashes])
    
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