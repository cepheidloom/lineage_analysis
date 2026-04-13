import os
import json
import pandas as pd
import re

def create_sets_from_jsons() -> list[str] :
    # pd.set_option("display.max_rows", None)

    folder = "_DATA_AND_OUTPUTS/lineage_outputs/shell-31-eun-sqdb-gfhihcxnwbvsohwwoqdp/"
    json_list = []
    set_creation_error = dict()

    for file in os.listdir(folder):
        with open(folder+file, "r") as f:
            try:
                json_file = json.load(f)
            except Exception as e:
                print("Failed to load file: ",f)
                print("\nError: ", e)
                raise
            json_file["file_name"] = file.replace(".json", "").split("--")[1] + "." + file.replace(".json", "").split("--")[2]

            try:
                src_trg_set = set()
                for src_trg_pairs in json_file["lineage"]:
                    src_trg_set.add(src_trg_pairs["source"])
                    src_trg_set.add(src_trg_pairs["target"])
                src_trg_set.remove(json_file["file_name"])
            except Exception as e:
                #if the name of error is simply the name of file then pass
                if str(e).replace("'","") == json_file["file_name"]:
                    pass
                else:
                    set_creation_error[json_file["file_name"]] = e
                
            src_trg_set = {element.lower() if element is not None else element for element in src_trg_set}

            json_file["content_set"] = src_trg_set
            json_list.append(json_file)

    print("Errors encountered while creating set for each json file: ", len(set_creation_error))

    # print("Key template of each dictionary in list: ",json_list[0].keys(), "\n")
    return json_list


def create_lineage_list() -> dict :
    # Load data
    df_dependency_mapping = pd.read_json("_DATA_AND_OUTPUTS/local_files/database_lineage_extracted.json")


    subset_cols = ['Dependent_Schema', 'Dependent_Object_Name', 'Dependent_Object_Type',
                'Depends_On_Schema', 'Depends_On_Object_Name', 'Depends_On_Object_Type']
    df_dependency_mapping = df_dependency_mapping.drop_duplicates(subset=subset_cols)

    lineage_list = {}
    for (dependent_schema, dependent_object_name), group in df_dependency_mapping.groupby(
        ["Dependent_Schema", "Dependent_Object_Name"]
    ):
        dependent_object_name = re.sub(r"[^\w\s-]", "", dependent_object_name).replace(
            " ", "_"
        )
        lineage_list[dependent_schema + "." + dependent_object_name] = set()
        for row in group.itertuples():
            if pd.isna(row[7]):
                continue
            content = row[7] + "." + row[8]
            lineage_list[dependent_schema + "." + dependent_object_name].add(
                content.lower()
            )
    return lineage_list


### Analyze both data structure and find out unequal sets containing lineage objects
def find_unequal_sets(json_list, lineage_list):
    correct = []
    incorrect = []
    errors = []
    for json_item in json_list:
        try:
            json_item["content_set"].discard("changing") #CHANGING
            json_item["content_set"].discard("referenced") #REFERENCED
            if json_item["content_set"] == lineage_list[json_item["file_name"]]:
                correct.append(json_item["file_name"])
            else:
                incorrect.append(json_item["file_name"])
        except Exception as e:
            errors.append(e)

    incorrect.sort()
    correct.sort()

    #---------------------------------Create dict using above code variables and print in json format-----------------

    stats_dict = {
        "correct": len(correct),
        "incorrect": len(incorrect),
        "total_jsons_processed": len(correct) + len(incorrect),
        "errors": len(errors),
        "names": {
            "correct_file_names": correct,
            "incorrect_file_names": incorrect,
            "errors_file_names": [str(e) for e in errors],
        },
    }
    print(json.dumps(stats_dict,indent=4))

if __name__ == "__main__":
    json_list = create_sets_from_jsons()
    lineage_list = create_lineage_list()
    find_unequal_sets(json_list, lineage_list)
    # print(lineage_list)
