import json
import re
import pandas as pd
from pathlib import Path
import yaml


def classify_single_partition(tmdl_name, data):
    m_query = data.get("m_query", "")
    partition_name = data.get("partition_name", "")
    partition_type = data.get("partition_type", "")

    # ------------------------------------------------------------------ #
    # 1. ENTITY partitions (DirectQuery to Analysis Services)
    # ------------------------------------------------------------------ #
    if partition_type == "entity":
        return {
            "PBI Table Name": tmdl_name,
            "Partition Name": partition_name,
            "Source Type": "DirectQuery (Analysis Services / Entity)",
            "Server / URL": data.get("expression_source", ""),
            "Database": "",
            "Schema": "",
            "Source Table Name": data.get("entity_name", ""),
            "Dataflow Entity": "",
            "Workspace ID": "",
            "Dataflow ID": "",
            "Excel Sheet / Table Name": "",
            "Inline SQL Query": "",
        }

    # ------------------------------------------------------------------ #
    # 2. CALCULATED partitions (DAX — no M query)
    # ------------------------------------------------------------------ #
    if partition_type == "calculated":
        mq_upper = m_query.upper().strip()
        if re.match(r'SUMMARIZE\s*\(', mq_upper):
            calc_type = "DAX Calculated Table (SUMMARIZE)"
        elif re.match(r'UNION\s*\(', mq_upper):
            calc_type = "DAX Calculated Table (UNION)"
        elif re.match(r'GENERATESERIES\s*\(', mq_upper):
            calc_type = "DAX Calculated Table (GENERATESERIES)"
        elif re.match(r'DATATABLE\s*\(', mq_upper):
            calc_type = "DAX Calculated Table (DATATABLE)"
        elif re.match(r'CALENDAR\s*\(', mq_upper):
            calc_type = "DAX Calculated Table (CALENDAR)"
        elif re.match(r'ROW\s*\(', mq_upper):
            calc_type = "DAX Calculated Table (ROW)"
        else:
            calc_type = "DAX Calculated Table"
        return _empty_result(tmdl_name, partition_name, calc_type)

    # ------------------------------------------------------------------ #
    # 3. M partitions — classify by connector
    # ------------------------------------------------------------------ #

    # --- SAP HANA ---
    sap_match = re.search(
        r'SapHana\.Database\(\s*"([^"]+)"',
        m_query
    )
    if sap_match:
        server = sap_match.group(1)
        # SAP schema: second {[Name="..."]} lookup after Source
        sap_schema_match = re.search(
            r'Contents\s*=\s*Source\{?\[Name="([^"]+)"\]\}?',
            m_query
        )
        # If not found try any second-level Name lookup
        if not sap_schema_match:
            name_hits = re.findall(r'\[Name="([^"]+)"\]', m_query)
            sap_schema = name_hits[0] if name_hits else ""
            sap_table = name_hits[1] if len(name_hits) > 1 else ""
        else:
            sap_schema = sap_schema_match.group(1)
            # SAP table / view: third-level Name lookup
            remaining = m_query[sap_schema_match.end():]
            sap_table_match = re.search(r'\[Name="([^"]+)"\]', remaining)
            sap_table = sap_table_match.group(1) if sap_table_match else ""
        return {
            "PBI Table Name": tmdl_name,
            "Partition Name": partition_name,
            "Source Type": "SAP HANA",
            "Server / URL": server,
            "Database": "",
            "Schema": sap_schema,
            "Source Table Name": sap_table,
            "Dataflow Entity": "",
            "Workspace ID": "",
            "Dataflow ID": "",
            "Excel Sheet / Table Name": "",
            "Inline SQL Query": "",
        }

    # --- SQL Database ---
    sql_db_match = re.search(
        r'Sql\.Database\(\s*(?:#"([^"]+)"|"([^"]+)"|(\w+))\s*,\s*(?:#"([^"]+)"|"([^"]+)"|(\w+))\s*',
        m_query
    )
    if sql_db_match:
        server  = sql_db_match.group(1) or sql_db_match.group(2) or sql_db_match.group(3) or ""
        database = sql_db_match.group(4) or sql_db_match.group(5) or sql_db_match.group(6) or ""

        # Schema + table from [Schema="..", Item=".."] style
        schema_item_match = re.search(r'\[Schema="([^"]+)",\s*Item="([^"]+)"\]', m_query)
        schema = schema_item_match.group(1) if schema_item_match else ""
        table_name = schema_item_match.group(2) if schema_item_match else ""

        # If no Schema/Item, try to extract from inline SQL: FROM [schema].[table]
        inline_sql = ""
        sql_query_match = re.search(r'\[Query\s*=\s*"(.*?)(?<!\\)"\s*[,\]]', m_query, re.DOTALL)
        if sql_query_match:
            inline_sql = sql_query_match.group(1).replace("#(lf)", "\n").strip()
            if not schema and not table_name:
                from_match = re.search(r'FROM\s+\[?(\w+)\]?\.\[?(\w+)\]?', inline_sql, re.IGNORECASE)
                if from_match:
                    schema = from_match.group(1)
                    table_name = from_match.group(2)

        return {
            "PBI Table Name": tmdl_name,
            "Partition Name": partition_name,
            "Source Type": "SQL Database",
            "Server / URL": server,
            "Database": database,
            "Schema": schema,
            "Source Table Name": table_name,
            "Dataflow Entity": "",
            "Workspace ID": "",
            "Dataflow ID": "",
            "Excel Sheet / Table Name": "",
            "Inline SQL Query": inline_sql,
        }

    # --- Excel Workbook via Web.Contents / SharePoint ---
    excel_match = re.search(r'Excel\.Workbook\(Web\.Contents\(\s*"([^"]+)"', m_query)
    if excel_match:
        url = excel_match.group(1)
        # Collect all Item/Kind references (Sheet or Table)
        items = re.findall(r'\[Item="([^"]+)",\s*Kind="([^"]+)"\]', m_query)
        sheet_table_names = ", ".join(
            f'{name} ({kind})' for name, kind in items
        ) if items else ""
        return {
            "PBI Table Name": tmdl_name,
            "Partition Name": partition_name,
            "Source Type": "Excel Workbook (SharePoint)",
            "Server / URL": url,
            "Database": "",
            "Schema": "",
            "Source Table Name": "",
            "Dataflow Entity": "",
            "Workspace ID": "",
            "Dataflow ID": "",
            "Excel Sheet / Table Name": sheet_table_names,
            "Inline SQL Query": "",
        }

    # --- Dataflows ---
    if re.search(r'PowerBI\.Dataflows\(', m_query):
        source_type = "Dataflow (PowerBI.Dataflows)"
    elif re.search(r'PowerPlatform\.Dataflows\(', m_query):
        source_type = "Dataflow (PowerPlatform.Dataflows)"
    else:
        source_type = None

    if source_type:
        entity_name = ""
        workspace_id = ""
        dataflow_id = ""
        en_match = re.search(r'\[entity="([^"]+)"', m_query)
        ws_match = re.search(r'workspaceId="([^"]+)"', m_query)
        df_match = re.search(r'dataflowId="([^"]+)"', m_query)
        entity_name  = en_match.group(1)  if en_match  else ""
        workspace_id = ws_match.group(1)  if ws_match  else ""
        dataflow_id  = df_match.group(1)  if df_match  else ""
        return {
            "PBI Table Name": tmdl_name,
            "Partition Name": partition_name,
            "Source Type": source_type,
            "Server / URL": "",
            "Database": "",
            "Schema": "",
            "Source Table Name": "",
            "Dataflow Entity": entity_name,
            "Workspace ID": workspace_id,
            "Dataflow ID": dataflow_id,
            "Excel Sheet / Table Name": "",
            "Inline SQL Query": "",
        }

    # --- SharePoint Files / Tables (without Excel.Workbook wrapper) ---
    sp_files_match  = re.search(r'SharePoint\.Files\(\s*"([^"]+)"', m_query)
    sp_tables_match = re.search(r'SharePoint\.Tables\(\s*"([^"]+)"', m_query)
    if sp_files_match:
        return _simple_result(tmdl_name, partition_name, "SharePoint (Files)", sp_files_match.group(1))
    if sp_tables_match:
        return _simple_result(tmdl_name, partition_name, "SharePoint (Tables/List)", sp_tables_match.group(1))

    # --- Web.Contents (not Excel, not SharePoint connector) ---
    web_match = re.search(r'Web\.Contents\(\s*"([^"]+)"', m_query)
    if web_match:
        return _simple_result(tmdl_name, partition_name, "Web/SharePoint (Web.Contents)", web_match.group(1))

    # --- Manual / Enter Data ---
    if re.search(r'Json\.Document\(\s*Binary\.Decompress\(', m_query):
        return _empty_result(tmdl_name, partition_name, "Manual Table (Enter Data)")

    # --- DAX-style patterns inside M (edge cases in m-typed partitions) ---
    if re.search(r'DATATABLE\s*\(',      m_query, re.IGNORECASE):
        return _empty_result(tmdl_name, partition_name, "DAX Calculated Table (DATATABLE)")
    if re.search(r'UNION\s*\(',          m_query, re.IGNORECASE):
        return _empty_result(tmdl_name, partition_name, "DAX Calculated Table (UNION)")
    if re.search(r'GENERATESERIES\s*\(', m_query, re.IGNORECASE):
        return _empty_result(tmdl_name, partition_name, "DAX Calculated Table (GENERATESERIES)")
    if re.search(r'SUMMARIZE\s*\(',      m_query, re.IGNORECASE):
        return _empty_result(tmdl_name, partition_name, "DAX Calculated Table (SUMMARIZE)")
    if re.match(r'\s*\{',                m_query):
        return _empty_result(tmdl_name, partition_name, "DAX Calculated Table (Table Constructor)")
    if re.search(r'Cal\s*\(',            m_query):
        return _empty_result(tmdl_name, partition_name, "Generated (Calendar Function)")

    # --- Empty M query ---
    if m_query.strip() == "":
        return _empty_result(tmdl_name, partition_name, "Empty / Calculated Table (no M query)")

    # --- Reference to another PBI table ---
    ref_match = re.search(
        r'^\s+Source\s*=\s*(?:#"([^"]+)"|#\'([^\']+)\'|([a-zA-Z_]\w*))',
        m_query, re.MULTILINE
    )
    if ref_match:
        ref_table = re.sub(
            r'[,\s\(\)]+$', '',
            ref_match.group(1) or ref_match.group(2) or ref_match.group(3)
        )
        is_connector = re.match(
            r'^(Table\.|Sql\.|SapHana\.|SharePoint\.|PowerBI\.|PowerPlatform\.|'
            r'Web\.|Excel\.|Csv\.|Date\.|List\.|Text\.|Binary\.)',
            ref_table
        )
        if not is_connector:
            return _simple_result(tmdl_name, partition_name, "Reference (another PBI table)", ref_table)

    return _empty_result(tmdl_name, partition_name, "Unknown")


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _base(tmdl_name, partition_name, source_type):
    return {
        "PBI Table Name": tmdl_name,
        "Partition Name": partition_name,
        "Source Type": source_type,
        "Server / URL": "",
        "Database": "",
        "Schema": "",
        "Source Table Name": "",
        "Dataflow Entity": "",
        "Workspace ID": "",
        "Dataflow ID": "",
        "Excel Sheet / Table Name": "",
        "Inline SQL Query": "",
    }

def _empty_result(tmdl_name, partition_name, source_type):
    return _base(tmdl_name, partition_name, source_type)

def _simple_result(tmdl_name, partition_name, source_type, server):
    r = _base(tmdl_name, partition_name, source_type)
    r["Server / URL"] = server
    return r


# ------------------------------------------------------------------ #
# Main
# ------------------------------------------------------------------ #

def classify_partitions(partitions_json_path, output_excel):
    with open(partitions_json_path, "r", encoding="utf-8") as f:
        partitions = json.load(f)

    results = [classify_single_partition(tmdl_name, data) for tmdl_name, data in partitions.items()]
    df = pd.DataFrame(results)

    with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="All Sources", index=False)
        summary = (
            df.groupby("Source Type")
            .agg(Count=("PBI Table Name", "count"))
            .reset_index()
            .sort_values("Count", ascending=False)
        )
        summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"✅ Total tables: {len(df)}")
    print(f"\n�� Breakdown:")
    for _, row in summary.iterrows():
        print(f"  {row['Source Type']}: {row['Count']}")
    print(f"\n�� Excel written to: {output_excel}")


if __name__ == "__main__":
    with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
        pbi_variables = yaml.safe_load(f)["power_bi_variables"]

    classify_partitions(
        Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "all_partitions.json",
        Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "tmdl_source_extraction.xlsx"
    )



# import json
# import re
# import pandas as pd
# from pathlib import Path
# import yaml


# def classify_single_partition(tmdl_name, data):
#     m_query = data.get("m_query", "")
#     partition_name = data.get("partition_name", "")

#     # Handle entity partitions (DirectQuery to Analysis Services / composite models)
#     if data.get("partition_type") == "entity":
#         return {
#             "PBI Table Name": tmdl_name,
#             "Partition Name": partition_name,
#             "Source Type": "DirectQuery (Analysis Services / Entity)",
#             "Server / URL": data.get("expression_source", ""),
#             "Database": "",
#             "Schema": "",
#             "Source Table Name": data.get("entity_name", ""),
#             "Dataflow Entity": "",
#             "Workspace ID": "",
#             "Dataflow ID": "",
#         }
    
#     sql_db_match = re.search(
#         r'Sql\.Database\(\s*(?:#"([^"]+)"|"([^"]+)"|(\w+))\s*,\s*(?:#"([^"]+)"|"([^"]+)"|(\w+))\s*\)',
#         m_query
#     )

#     if sql_db_match:
#         source_type = "SQL Database"
#         server = sql_db_match.group(1) or sql_db_match.group(2) or sql_db_match.group(3) or ""
#         database = sql_db_match.group(4) or sql_db_match.group(5) or sql_db_match.group(6) or ""
#     elif re.search(r'PowerBI\.Dataflows\(', m_query):
#         source_type = "Dataflow (PowerBI.Dataflows)"; server = ""; database = ""
#     elif re.search(r'PowerPlatform\.Dataflows\(', m_query):
#         source_type = "Dataflow (PowerPlatform.Dataflows)"; server = ""; database = ""
#     elif sp_match := re.search(r'SharePoint\.Files\(\s*"([^"]+)"', m_query):
#         source_type = "SharePoint (Files)"; server = sp_match.group(1); database = ""
#     elif sp_match := re.search(r'SharePoint\.Tables\(\s*"([^"]+)"', m_query):
#         source_type = "SharePoint (Tables/List)"; server = sp_match.group(1); database = ""
#     elif re.search(r'Json\.Document\(\s*Binary\.Decompress\(', m_query):
#         source_type = "Manual Table (Enter Data)"; server = ""; database = ""
#     elif re.search(r'DATATABLE\s*\(', m_query, re.IGNORECASE):
#         source_type = "DAX Calculated Table (DATATABLE)"; server = ""; database = ""
#     elif re.search(r'UNION\s*\(', m_query, re.IGNORECASE):
#         source_type = "DAX Calculated Table (UNION)"; server = ""; database = ""
#     elif re.search(r'GENERATESERIES\s*\(', m_query, re.IGNORECASE):
#         source_type = "DAX Calculated Table (GENERATESERIES)"; server = ""; database = ""
#     elif re.search(r'SUMMARIZE\s*\(', m_query, re.IGNORECASE):
#         source_type = "DAX Calculated Table (SUMMARIZE)"; server = ""; database = ""
#     elif web_match := re.search(r'Web\.Contents\(\s*"([^"]+)"', m_query):
#         source_type = "Web/SharePoint (Web.Contents)"; server = web_match.group(1); database = ""
#     elif re.match(r'\s*\{', m_query):
#         source_type = "DAX Calculated Table (Table Constructor)"; server = ""; database = ""
#     elif re.search(r'Cal\s*\(', m_query):
#         source_type = "Generated (Calendar Function)"; server = ""; database = ""
#     elif m_query.strip() == "":
#         source_type = "Empty / Calculated Table (no M query)"; server = ""; database = ""
#     elif ref_match := re.search(r'^\s+Source\s*=\s*(?:#"([^"]+)"|#\'([^\']+)\'|([a-zA-Z_]\w*))', m_query, re.MULTILINE):
#         ref_table = re.sub(r'[,\s\(\)]+$', '', ref_match.group(1) or ref_match.group(2) or ref_match.group(3))
#         if not re.match(r'^(Table\.|Sql\.|SharePoint\.|PowerBI\.|PowerPlatform\.|Web\.|Excel\.|Csv\.|Date\.|List\.|Text\.)', ref_table):
#             source_type = "Reference (another PBI table)"; server = ref_table; database = ""
#         else:
#             source_type = "Unknown"; server = ""; database = ""
#     else:
#         source_type = "Unknown"; server = ""; database = ""

#     schema_item_match = re.search(r'\[Schema="([^"]+)",\s*Item="([^"]+)"\]', m_query)
#     schema = schema_item_match.group(1) if schema_item_match else ""
#     table_name = schema_item_match.group(2) if schema_item_match else ""

#     entity_name = workspace_id = dataflow_id = ""
#     if "Dataflow" in source_type:
#         entity_name = (re.search(r'\[entity="([^"]+)"', m_query) or type('', (), {'group': lambda s, x: ""})()).group(1)
#         ws_match = re.search(r'workspaceId="([^"]+)"', m_query)
#         df_match = re.search(r'dataflowId="([^"]+)"', m_query)
#         workspace_id = ws_match.group(1) if ws_match else ""
#         dataflow_id = df_match.group(1) if df_match else ""

#     return {
#         "PBI Table Name": tmdl_name,
#         "Partition Name": partition_name,
#         "Source Type": source_type,
#         "Server / URL": server,
#         "Database": database,
#         "Schema": schema,
#         "Source Table Name": table_name,
#         "Dataflow Entity": entity_name,
#         "Workspace ID": workspace_id,
#         "Dataflow ID": dataflow_id,
#     }


# def classify_partitions(partitions_json_path, output_excel):
#     with open(partitions_json_path, "r", encoding="utf-8") as f:
#         partitions = json.load(f)

#     results = [classify_single_partition(tmdl_name, data) for tmdl_name, data in partitions.items()]
#     df = pd.DataFrame(results)

#     with pd.ExcelWriter(output_excel, engine="openpyxl") as writer:
#         df.to_excel(writer, sheet_name="All Sources", index=False)
#         summary = df.groupby("Source Type").agg(Count=("PBI Table Name", "count")).reset_index().sort_values("Count", ascending=False)
#         summary.to_excel(writer, sheet_name="Summary", index=False)

#     print(f"✅ Total tables: {len(df)}")
#     print(f"\n📊 Breakdown:")
#     for _, row in summary.iterrows():
#         print(f"  {row['Source Type']}: {row['Count']}")
#     print(f"\n💾 Excel written to: {output_excel}")


# if __name__ == "__main__":
#     with open("_DATA_AND_OUTPUTS/local_files/target_object.yaml", "r") as f:
#         pbi_variables = yaml.safe_load(f)["power_bi_variables"]

#     classify_partitions(
#         Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "all_partitions.json",
#         Path(pbi_variables["output_file"]) / Path(pbi_variables["report_name"]) / "tmdl_source_extraction.xlsx"
#     )