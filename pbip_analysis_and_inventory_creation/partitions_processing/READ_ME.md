# Partitions Processing — Subfolder Scripts

This subfolder contains 3 scripts that form a sequential pipeline to extract, classify, and map Power BI data sources from a PBIP report to its visuals.

---

## Pipeline Overview

```
get_partitions_json.py  →  classify_partitions.py  →  generate_visuals_to_source_mapping.py
        ↓                          ↓                               ↓
 all_partitions.json     tmdl_source_extraction.xlsx    combined_visual_lineage.xlsx
```

---

## Scripts

### 1. `get_partitions_json.py`
**Purpose:** Reads raw `.tmdl` files from the Power BI project and extracts partition metadata (M queries, partition type, mode) for every table in the model.

**Input:**
- `.tmdl` files from the report's tables folder (path resolved via `target_object.yaml`)

**Output:**
- `all_partitions.json` — one entry per table, containing `partition_name`, `partition_type`, `mode`, and `m_query`

---

### 2. `classify_partitions.py`
**Purpose:** Reads `all_partitions.json` and classifies each table's data source by parsing its M query. Produces a summary Excel report.

**Input:**
- `all_partitions.json` (output of Step 1)

**Output:**
- `tmdl_source_extraction.xlsx` with two sheets:
  - **All Sources** — every table with its classified source type, server, database, schema, dataflow details, etc.
  - **Summary** — count of tables per source type

**Source types detected:**
- SQL Database
- Dataflow (PowerBI.Dataflows / PowerPlatform.Dataflows)
- SharePoint (Files / Tables)
- Manual Table (Enter Data)
- DAX Calculated Tables (DATATABLE, UNION, GENERATESERIES, SUMMARIZE, Table Constructor)
- Web/SharePoint (Web.Contents)
- Reference (another PBI table)
- Generated (Calendar Function)
- Empty / Calculated Table

**Reusable function:**
`classify_single_partition(tmdl_name, data) -> dict` — classifies a single partition entry. Imported by Step 3.

---

### 3. `generate_visuals_to_source_mapping.py`
**Purpose:** Combines visual-level field usage (columns, measures, filters, lineage) with the source classification from Step 2 to produce an end-to-end mapping from report visuals back to their upstream data sources.

**Input:**
- `all_partitions.json` (output of Step 1)
- `{report_name}_model_inventory.json` — model inventory with field/measure metadata
- `visuals_lineage/` folder — one subfolder per page, each containing `.json` files describing visual field usage

**Output:**
- `combined_visual_lineage.xlsx` with three sheets:
  - **Combined Lineage** — every visual/field combination with full source details
  - **Tables Summary** — per-table aggregation with source info and usage counts
  - **Source Type Summary** — high-level breakdown by source type (tables, fields, visuals, pages)

> **Note:** A table appearing in the model but not in this report means it exists in the data model but is not referenced by any visual, measure, or filter — i.e. it is an unused/orphaned table.

---

## Configuration

All three scripts read paths from a shared YAML config file:

```
_DATA_AND_OUTPUTS/local_files/target_object.yaml
```

Expected structure:

```yaml
power_bi_variables:
  reports_folder: "_DATA_AND_OUTPUTS/local_files/power_bi_inventory/power_bi_pbip_files"
  output_file: "_DATA_AND_OUTPUTS/local_files/power_bi_inventory/reports_generated_outputs"
  ## Above 2 fields are fixed and should remain so for the sake of this project. 
  ## Change below 2 fields. 
  report_name: "Your Report Name" #Example:- Business Management Dashboards
  table_folder_path: "reportname/reportname.SemanticModel/tables" #Example:- Business Management Dashboards/Business Management Dashboards.SemanticModel/definition/tables
```

NOTE:- Kindly keep the name of the folder in which .pbip files are saved exactly the same as the name of the report. This prevents errors when resolving filepaths in multiple code files.
---

## Dependencies

```
pandas
openpyxl
pyyaml
```