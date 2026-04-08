import pandas as pd
import json
import os
import re
# Load data
df_lineage_extracted = pd.read_json("_DATA_AND_OUTPUTS/local_files/database_lineage_extracted.json")
df_lineage_extracted = df_lineage_extracted[df_lineage_extracted["Dependent_Object_Type"] == 'VIEW']

subset_cols = ['Dependent_Schema', 'Dependent_Object_Name', 'Dependent_Object_Type',
               'Depends_On_Schema', 'Depends_On_Object_Name', 'Depends_On_Object_Type']
df = df_lineage_extracted.drop_duplicates(subset=subset_cols)

# 1. Setup the dedicated folder
folder_name = "_DATA_AND_OUTPUTS/lineage_outputs"
if not os.path.exists(folder_name):
    os.makedirs(folder_name)

for db in df['Database'].unique():
    db_folder = os.path.join(folder_name, db)
    if not os.path.exists(db_folder):
        os.makedirs(db_folder)


# 2. Create concatenated full names for JSON content only (Schema.ObjectName)
df.loc[:, 'Target_Full'] = df['Dependent_Schema'] + "." + df['Dependent_Object_Name']
df.loc[:, 'Source_Full'] = df['Depends_On_Schema'] + "." + df['Depends_On_Object_Name']

# 3. Group and Export
# Enumerate starts at 1 for the index
for idx, (target_full, group) in enumerate(df.groupby('Target_Full'), 1):
    dependencies = []
    # Get schema and object name from the first row of the group for naming
    # We grab these directly to avoid parsing the '.' later
    schema_name = group['Dependent_Schema'].iloc[0]
    object_name = group['Dependent_Object_Name'].iloc[0]

    for _, row in group.iterrows():
        dependencies.append({
            "source": row['Source_Full'],
            "target": row['Target_Full']
        })
    
    # SANITIZATION:
    # Remove illegal characters from schema and object name individually
    safe_schema = re.sub(r'[<>:"/\\|?*]', '_', str(schema_name))
    safe_object = re.sub(r'[<>:"/\\|?*]', '_', str(object_name))
    
    separator = "--" 
    
    filename = f"{idx}{separator}{safe_schema}{separator}{safe_object}.json"
    
    file_path = os.path.join(folder_name,group['Database'].iloc[0], filename)
    
    # Wrap dependencies in a "lineage" key
    output_data = {
        "name": f"{schema_name}.{object_name}",
        "type": "VIEW",
        "database": group['Database'].iloc[0],
        "object_schema": schema_name,
        "object_name": object_name,
        "lineage": dependencies
    }
    with open(file_path, 'w') as f:
        json.dump(output_data, f, indent=4)
        
    # print(f"Generated: {file_path}")