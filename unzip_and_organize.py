import zipfile
import os

ZIPFILES_PATH = "./tables/zip/"
TABLES_PATH = "./tables_cnes_database/"

zipfiles = os.listdir(ZIPFILES_PATH)

def unzip_file(zipfile_name: list[str], zipfiles_path: str, tables_path: str) -> None:
    """Extract zip file to tables_path creating folder to organize per year and month"""
    with zipfile.ZipFile(zipfiles_path + zipfile_name, "r") as zf:
        digits = zipfile_name.split(".")[0][-6:] # getting digits for year and month
        full_path = tables_path + digits[:4] + "/" + digits[-2:] + "/"
                                # year              # month 

        if not os.path.exists(full_path): # if folder doesn't exist, create the folder
            os.makedirs(full_path)
            print(f"Extracting from {zipfile_name}...")
            zf.extractall(full_path)
            print(f"Files from {zipfile_name} extracted.\n")
        else:
            if not os.listdir(full_path): # if directory is empty
                print(f"Extracting from {zipfile_name}...")
                zf.extractall(full_path)
                print(f"Files from {zipfile_name} extracted.\n")
            else:
                pass
        return 1

count = 0

for z in zipfiles:
    try:
        count += unzip_file(z, ZIPFILES_PATH, TABLES_PATH)
    except zipfile.BadZipFile: # case the zipfile isn't opening
        print(f"{z} is corrupted, the file will be deleted and downloaded again...\n")
        os.remove(ZIPFILES_PATH + z)
        with open("download_zipfiles.py", "r") as download_zipfiles:
            exec(download_zipfiles.read())
        count += unzip_file(z, ZIPFILES_PATH, TABLES_PATH)

if count == len(zipfiles):
    print("All files unziped.")