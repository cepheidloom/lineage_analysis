import subprocess
import sys
import time
def run_pipeline():
    scripts = [
        "pbip_analysis_and_inventory_creation/extract_model_inventory.py",
        "pbip_analysis_and_inventory_creation/extract_visuals.py",
        "pbip_analysis_and_inventory_creation/generate_depends_on_columns.py",
        "pbip_analysis_and_inventory_creation/generate_lineage_in_visuals.py"
    ]

    print(f"{'='*50}\nSTARTING POWER BI ANALYSIS PIPELINE\n{'='*50}")
    start_time = time.time()
    
    for script in scripts:
        print(f"\n>> Executing: {script}...")

        result = subprocess.run([sys.executable, "-u", script])
        
        if result.returncode != 0:
            print(f"\n[!] PIPELINE FAILED AT: {script}")
            return #Abort the rest of the pipeline
    
    end_time = time.time()
    print(f"\n{'='*50}")

if __name__ == "__main__":
    run_pipeline()