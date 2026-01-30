import os
import json

# Define the folder path relative to the current working directory
folder_path = "../lineage_outputs/"

# Check if folder exists to avoid errors
if os.path.exists(folder_path):
    files_list = os.listdir(folder_path)

    for filename in files_list:
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            
            # 1. Read the existing JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # 2. Modify the data in memory
            # Check if 'lineage' key exists to be safe
            if "lineage" in data:
                for item in data["lineage"]:
                    try:
                        # Clean 'source' if it exists
                        if "source" in item:
                            item["source"] = item["source"].replace("[", "").replace("]", "")
                        
                        # Clean 'target' if it exists
                        if "target" in item:
                            item["target"] = item["target"].replace("[", "").replace("]", "")
                    except Exception as e:
                        print(f"Error in {filename}: ", e)

            # 3. Write the cleaned data back to the same file
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
                
    print(f"Successfully cleaned brackets from files in '{folder_path}'")
else:
    print(f"Folder '{folder_path}' not found.")