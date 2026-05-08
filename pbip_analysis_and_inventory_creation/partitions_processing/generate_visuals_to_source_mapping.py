import json
import re
import yaml
import pandas as pd
from pathlib import Path

# Import classify_partition directly from classify_partitions.py
from classify_partitions import classify_single_partition


def load_model_inventory(model_inventory_path):
    with open(model_inventory_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_partitions(partitions_json_path):
    with open(partitions_json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_source_lookup(partitions):
    return {tmdl_name: classify_single_partition(tmdl_name, data) for tmdl_name, data in partitions.items()}



def extract_fields_from_visuals(visuals, page_name):
    rows = []
    for visual in visuals:
        visual_title = visual.get("visual_title", visual.get("name", "Unknown"))
        visual_id = visual.get("visual_id", "")

        for field in visual.get("columns_used", []):
            rows.append({"page_name": page_name, "visual_id": visual_id, "visual": visual_title, "field": field, "source": "direct (column)"})

        for field in visual.get("measures_used", []):
            rows.append({"page_name": page_name, "visual_id": visual_id, "visual": visual_title, "field": field, "source": "direct (measure)"})

        for entry in visual.get("lineage", []):
            rows.append({"page_name": page_name, "visual_id": visual_id, "visual": visual_title, "field": entry["source"], "source": "lineage (indirect)"})
            rows.append({"page_name": page_name, "visual_id": visual_id, "visual": visual_title, "field": entry["target"], "source": "lineage (indirect)"})

        for pf in visual.get("page_filters", []):
            target = pf.get("target")
            if target:
                rows.append({"page_name": page_name, "visual_id": visual_id, "visual": visual_title, "field": target, "source": "page_filter"})

    return rows


def enrich_rows(rows, model_inventory, source_lookup):
    enriched = []
    for row in rows:
        field = row["field"]
        inv = model_inventory.get(field, {})

        table = inv.get("table", field.split("[")[0] if "[" in field else "")
        name = inv.get("name", field.split("[")[1].rstrip("]") if "[" in field else field)
        field_type = inv.get("type", "Unknown")
        expression = inv.get("expression", "")

        src = source_lookup.get(table, {})

        enriched.append({
            "Page Name": row["page_name"],
            "Visual ID": row["visual_id"],
            "Visual": row["visual"],
            "Table": table,
            "Field": name,
            "Full Reference": field,
            "Field Type": field_type,
            "DAX Expression": expression,
            "Usage": row["source"],
            "Source Type": src.get("Source Type", ""),
            "Server / URL": src.get("Server / URL", ""),
            "Database": src.get("Database", ""),
            "Schema": src.get("Schema", ""),
            "Source Table Name": src.get("Source Table Name", ""),
            "Dataflow Entity": src.get("Dataflow Entity", ""),
            "Workspace ID": src.get("Workspace ID", ""),
            "Dataflow ID": src.get("Dataflow ID", ""),
        })

    return enriched


def main():

    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)["power_bi_variables"]


    report_name = Path(pbi_variables["report_name"])
    output_file = Path(pbi_variables["output_file"])

    #### Variables
        #Input variables
    model_inventory_path   = output_file / report_name / f"{report_name}_model_inventory.json"
    partitions_json_path   = output_file / report_name / "all_partitions.json"
    VISUALS_LINEAGE_FOLDER = output_file / report_name / "visuals_lineage"
        
        #Output variables
    output_excel           = output_file / report_name / "combined_visual_lineage.xlsx"
    #### Variables END

    model_inventory = load_model_inventory(model_inventory_path)
    partitions = load_partitions(partitions_json_path)
    source_lookup = build_source_lookup(partitions)

    list_pages = [f.name for f in VISUALS_LINEAGE_FOLDER.iterdir() if f.is_dir()]

    all_rows = []
    for page_name in sorted(list_pages):
        folder_path = VISUALS_LINEAGE_FOLDER / page_name
        visuals = []
        for json_file in folder_path.glob("*.json"):
            with open(json_file, "r", encoding="utf-8") as f:
                visuals.append(json.load(f))

        page_rows = extract_fields_from_visuals(visuals, page_name)
        all_rows.extend(page_rows)

    enriched = enrich_rows(all_rows, model_inventory, source_lookup)

    df = pd.DataFrame(enriched).drop_duplicates()

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Combined Lineage", index=False)

        tables_summary = df.groupby(["Table", "Source Type", "Server / URL", "Database", "Schema", "Source Table Name"]).agg(
            Fields_Count=("Field", "nunique"),
            Pages_Using=("Page Name", "nunique"),
            Visuals_Using=("Visual ID", "nunique")
        ).reset_index()
        tables_summary.to_excel(writer, sheet_name="Tables Summary", index=False)

        source_summary = df.groupby("Source Type").agg(
            Tables=("Table", "nunique"),
            Fields=("Field", "nunique"),
            Visuals=("Visual ID", "nunique"),
            Pages=("Page Name", "nunique")
        ).reset_index().sort_values("Tables", ascending=False)
        source_summary.to_excel(writer, sheet_name="Source Type Summary", index=False)

    print(f"✅ Total rows: {len(df)}")
    print(f"📄 Total pages: {len(list_pages)}")
    print(f"💾 Excel written to: {output_excel}")


if __name__ == "__main__":
    main()