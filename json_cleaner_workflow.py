# Delete all files/folders from a directory without deleting the directory itself.
import shutil
from pathlib import Path


def json_cleaner_workflow(source_dir : str, dest_dir: str ):
    # DELETE all files and folders from destintation directory
    
    # Verify path exists to avoid errors
    dest_dir_path = Path(dest_dir)
    if Path(dest_dir_path).exists():
        for item in dest_dir_path.iterdir():
            if item.is_dir():
                shutil.rmtree(item)  # Deletes a folder and all its contents
            else:
                item.unlink()  # Deletes a file

    # Copies entire directory tree# Copies entire directory tree
    shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)