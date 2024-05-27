from prefect import task, flow
import shutil
 
@task
def copy_file(src_path, dest_path):
    try:
        shutil.copy(src_path, dest_path)
        print(f"File copied from {src_path} to {dest_path}")
    except Exception as e:
        print(f"Error copying file: {e}")
 
@flow
def file_movement_flow():
    src_path = r"C:\Users\MK2307\OneDrive - Modak Analytics LLP\MY MODAK WORK\AZURE DATABRICKS\AZURE DATABRICKS NOTES.docx"  # Using raw string for source path
    dest_path = r"C:\Users\MK2307\OneDrive - Modak Analytics LLP\MY MODAK WORK\HUMANA PROJECT\PREFECT"  # Using raw string for destination path
    copy_file(src_path, dest_path)
 
if __name__ == "__main__":
    file_movement_flow()
    
    