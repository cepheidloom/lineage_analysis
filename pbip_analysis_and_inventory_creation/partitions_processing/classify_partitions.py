import json
import re
import pandas as pd
from pathlib import Path
import yaml


def classify_single_partition(tmdl_name, data):
    m_query = data.get("m_query", "")
    partition_name = data.get("partition_name", "")

    sql_db_match = re.search(
        r'Sql\.Database\(\s*(?:#"([^"]+)"|"([^"]+)"|(\w+))\s*,\s*(?:#"([^"]+)"|"([^"]+)"|(\w+))\s*\)',
        m_query
    )

    if sql_db_match:
        source_type = "SQL Database"
        server = sql_db_match.group(1) or sql_db_match.group(2) or sql_db_match.group(3) or ""
        database = sql_db_match.group(4) or sql_db_match.group(5) or sql_db_match.group(6) or ""
    elif re.search(r'PowerBI\.Dataflows\(', m_query):
        source_type = "Dataflow (PowerBI.Dataflows)"; server = ""; database = ""
    elif re.search(r'PowerPlatform\.Dataflows\(', m_query):
        source_type = "Dataflow (PowerPlatform.Dataflows)"; server = ""; database = ""
    elif sp_match := re.search(r'SharePoint\.Files\(\s*"([^"]+)"', m_query):
        source_type = "SharePoint (Files)"; server = sp_match.group(1); database = ""
    elif sp_match := re.search(r'SharePoint\.Tables\(\s*"([^"]+)"', m_query):
        source_type = "SharePoint (Tables/List)"; server = sp_match.group(1); database = ""
    elif re.search(r'Json\.Document\(\s*Binary\.Decompress\(', m_query):
        source_type = "Manual Table (Enter Data)"; server = ""; database = ""
    elif re.search(r'DATATABLE\s*\(', m_query, re.IGNORECASE):
        source_type = "DAX Calculated Table (DATATABLE)"; server = ""; database = ""
    elif re.search(r'UNION\s*\(', m_query, re.IGNORECASE):
        source_type = "DAX Calculated Table (UNION)"; server = ""; database = ""
    elif re.search(r'GENERATESERIES\s*\(', m_query, re.IGNORECASE):
        source_type = "DAX Calculated Table (GENERATESERIES)"; server = ""; database = ""
    elif re.search(r'SUMMARIZE\s*\(', m_query, re.IGNORECASE):
        source_type = "DAX Calculated Table (SUMMARIZE)"; server = ""; database = ""
    elif web_match := re.search(r'Web\.Contents\(\s*"([^"]+)"', m_query):
        source_type = "Web/SharePoint (Web.Contents)"; server = web_match.group(1); database = ""
    elif re.match(r'\s*\{', m_query):
        source_type = "DAX Calculated Table (Table Constructor)"; server = ""; database = ""
    elif re.search(r'Cal\s*\(', m_query):
        source_type = "Generated (Calendar Function)"; server = ""; database = ""
    elif m_query.strip() == "":
        source_type = "Empty / Calculated Table (no M query)"; server = ""; database = ""
    elif ref_match := re.search(r'^\s+Source\s*=\s*(?:#"([^"]+)"|#\'([^\']+)\'|([a-zA-Z_]\w*))', m_query, re.MULTILINE):
        ref_table = re.sub(r'[,\s\(\)]+$', '', ref_match.group(1) or ref_match.group(2) or ref_match.group(3))
        if not re.match(r'^(Table\.|Sql\.|SharePoint\.|PowerBI\.|PowerPlatform\.|Web\.|Excel\.|Csv\.|Date\.|List\.|Text\.)', ref_table):
            source_type = "Reference (another PBI table)"; server = ref_table; database = ""
        else:
            source_type = "Unknown"; server = ""; database = ""
    else:
        source_type = "Unknown"; server = ""; database = ""

    schema_item_match = re.search(r'\[Schema="([^"]+)",\s*Item="([^"]+)"\]', m_query)
    schema = schema_item_match.group(1) if schema_item_match else ""
    table_name = schema_item_match.group(2) if schema_item_match else ""

    entity_name = workspace_id = dataflow_id = ""
    if "Dataflow" in source_type:
        entity_name = (re.search(r'\[entity="([^"]+)"', m_query) or type('', (), {'group': lambda s, x: ""})()).group(1)
        ws_match = re.search(r'workspaceId="([^"]+)"', m_query)
        df_match = re.search(r'dataflowId="([^"]+)"', m_query)
        workspace_id = ws_match.group(1) if ws_match else ""
        dataflow_id = df_match.group(1) if df_match else ""

    return {
        "PBI Table Name": tmdl_name,
        "Partition Name": partition_name,
        "Source Type": source_type,
        "Server / URL": server,
        "Database": database,
        "Schema": schema,
        "Source Table Name": table_name,
        "Dataflow Entity": entity_name,
        "Workspace ID": workspace_id,
        "Dataflow ID": dataflow_id,
    }


def classify_partitions(partitions_json_path, output_excel):
    with open(partitions_json_path, "r", encoding="utf-8") as f:
        partitions = json.load(f)

    results = [classify_single_partition(tmdl_name, data) for tmdl_name, data in partitions.items()]
    df = pd.DataFrame(results)

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All Sources", index=False)
        summary = df.groupby("Source Type").agg(Count=("PBI Table Name", "count")).reset_index().sort_values("Count", ascending=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"✅ Total tables: {len(df)}")
    print(f"\n📊 Breakdown:")
    for _, row in summary.iterrows():
        print(f"  {row['Source Type']}: {row['Count']}")
    print(f"\n💾 Excel written to: {output_excel}")


if __name__ == "__main__":
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)["power_bi_variables"]

    classify_partitions(
        Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "all_partitions.json",
        Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "tmdl_source_extraction.xlsx"
    )