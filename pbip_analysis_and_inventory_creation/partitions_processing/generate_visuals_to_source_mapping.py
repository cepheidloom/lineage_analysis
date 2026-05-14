import json
import yaml
import pandas as pd
from pathlib import Path


def load_model_inventory(model_inventory_path):
    with open(model_inventory_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_partitions(partitions_json_path):
    with open(partitions_json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_source_lookup(partitions):
    from classify_partitions import classify_single_partition
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

    return rows


def extract_page_filters(visuals, page_name):
    seen = set()
    rows = []
    for visual in visuals:
        for pf in visual.get("page_filters", []):
            target = pf.get("target")
            if target and target not in seen:
                seen.add(target)
                rows.append({
                    "page_name": page_name,
                    "visual_id": "PAGE_FILTER",
                    "visual": "Page Filter",
                    "field": target,
                    "source": "page_filter"
                })
    return rows


def enrich_rows(rows, model_inventory, source_lookup=None):
    enriched = []
    for row in rows:
        field = row["field"]
        inv = model_inventory.get(field, {})

        table = inv.get("table", field.split("[")[0] if "[" in field else "")
        name = inv.get("name", field.split("[")[1].rstrip("]") if "[" in field else field)
        field_type = inv.get("type", "Unknown")
        expression = inv.get("expression", "")

        entry = {
            "Page Name": row["page_name"],
            "Visual ID": row["visual_id"],
            "Visual": row["visual"],
            "Table": table,
            "Field": name,
            "Full Reference": field,
            "Field Type": field_type,
            "DAX Expression": expression,
            "Usage": row["source"],
        }

        if source_lookup is not None:
            src = source_lookup.get(table, {})
            entry.update({
                "Source Type": src.get("Source Type", ""),
                "Server / URL": src.get("Server / URL", ""),
                "Database": src.get("Database", ""),
                "Schema": src.get("Schema", ""),
                "Source Table Name": src.get("Source Table Name", ""),
                "Dataflow Entity": src.get("Dataflow Entity", ""),
                "Workspace ID": src.get("Workspace ID", ""),
                "Dataflow ID": src.get("Dataflow ID", ""),
            })

        enriched.append(entry)

    return enriched


def main():

    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)["power_bi_variables"]

    report_name = Path(pbi_variables["report_name"])
    output_file = Path(pbi_variables["output_file"])

    #### Variables
    include_source_mapping = True
    model_inventory_path = output_file / report_name / f"{report_name}_model_inventory.json"
    partitions_json_path = output_file / report_name / "all_partitions.json"
    VISUALS_LINEAGE_FOLDER = output_file / report_name / "visuals_lineage"
    output_excel = output_file / report_name / "combined_visual_lineage.xlsx"
    #### Variables END

    model_inventory = load_model_inventory(model_inventory_path)

    source_lookup = None
    if include_source_mapping:
        source_lookup = build_source_lookup(load_partitions(partitions_json_path))

    list_pages = [f.name for f in VISUALS_LINEAGE_FOLDER.iterdir() if f.is_dir()]

    all_visual_rows = []
    all_filter_rows = []

    for page_name in sorted(list_pages):
        folder_path = VISUALS_LINEAGE_FOLDER / page_name
        visuals = []
        for json_file in folder_path.glob("*.json"):
            with open(json_file, "r", encoding="utf-8") as f:
                visuals.append(json.load(f))

        all_visual_rows.extend(extract_fields_from_visuals(visuals, page_name))
        all_filter_rows.extend(extract_page_filters(visuals, page_name))

    df_visuals = pd.DataFrame(enrich_rows(all_visual_rows, model_inventory, source_lookup)).drop_duplicates()
    df_filters = pd.DataFrame(enrich_rows(all_filter_rows, model_inventory, source_lookup)).drop_duplicates()

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        df_visuals.to_excel(writer, sheet_name="Combined Lineage", index=False)

        df_filters.to_excel(writer, sheet_name="Page Filters", index=False)

        tables_summary = df_visuals.groupby(["Table"] + (["Source Type", "Server / URL", "Database", "Schema", "Source Table Name"] if include_source_mapping else [])).agg(
            Fields_Count=("Field", "nunique"),
            Pages_Using=("Page Name", "nunique"),
            Visuals_Using=("Visual ID", "nunique")
        ).reset_index()
        tables_summary.to_excel(writer, sheet_name="Tables Summary", index=False)

        if include_source_mapping:
            source_summary = df_visuals.groupby("Source Type").agg(
                Tables=("Table", "nunique"),
                Fields=("Field", "nunique"),
                Visuals=("Visual ID", "nunique"),
                Pages=("Page Name", "nunique")
            ).reset_index().sort_values("Tables", ascending=False)
            source_summary.to_excel(writer, sheet_name="Source Type Summary", index=False)

    print(f"✅ Total visual rows: {len(df_visuals)}")
    print(f"🔎 Total page filter rows: {len(df_filters)}")
    print(f"📄 Total pages: {len(list_pages)}")
    print(f"💾 Excel written to: {output_excel}")


if __name__ == "__main__":
    main()