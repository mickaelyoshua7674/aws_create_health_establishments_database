from ftplib import FTP
import traceback
import sys
import os

ZIP_FILES_PATH = "./tables/zip/"
BASE_FILES_NAME = "BASE_DE_DADOS_CNES"
SITE = "ftp.datasus.gov.br"
FTP_FOLDER = "cnes"

def print_error() -> None:
    """Print the error message and exit script."""
    traceback.print_exc()
    print("Closing...")
    sys.exit()

def download_zipfiles(site: str, ftp_folder: str, base_files_name: str, zip_files_path: str, zipfiles_names_dir: str, zipfiles_names_ftp: str) -> None:
    """Access the ftp connection, go to folder and download files with the base name passed as an argument."""
    with FTP(site) as ftp:
        try:
            # LOGIN
            if ftp.login().startswith("230"): # ftp.login() enter the connection and return a string with the response
                print("Logged in.\n")
            else:
                print("Failed to log in.")
                sys.exit()

            # GO TO 'cnes' DIRECTORY
            if ftp.cwd(ftp_folder).startswith("250"): # ftp.cwd() 'change working diretory' to cnes and return a string with the response
                print("Directory changed to 'cnes'.\n")
            else:
                print("Failed to change directory.")
                sys.exit()
        except:
            print_error()

        print("Downloading zipfiles:")
        for file in zipfiles_names_ftp:
            if file.startswith(base_files_name) and file not in zipfiles_names_dir: # if file isn't already on local folder
                with open(zip_files_path + f"{file}", "wb") as f:
                    print(f"Downloading {file}...")
                    retCode = ftp.retrbinary(f"RETR {file}", f.write) # download the file and return a string with the response
                    if retCode.startswith("226"):
                        print(f"{file} downloaded.")
                    else:
                        print(f"Error downloading file: {retCode}")

# GET ZIPFILES NAMES FROM DIR
zipfiles_names_dir = os.listdir(ZIP_FILES_PATH)

# GET ZIPFILES NAMES FROM FTP
with FTP("ftp.datasus.gov.br") as ftp:
    try:
        # LOGIN
        if ftp.login().startswith("230"): # ftp.login() enter the connection and return a string with the response
            print("Logged in.\n")
        else:
            print("Failed to log in.")
            sys.exit()

        # GO TO 'cnes' DIRECTORY
        if ftp.cwd("cnes").startswith("250"): # ftp.cwd() 'change working diretory' to cnes and return a string with the response
            print("Directory changed to 'cnes'.\n")
        else:
            print("Failed to change directory.")
            sys.exit()
    except:
        print_error()
    try:
        zipfiles_names_ftp = []
        for file in ftp.nlst(): # ftp.nlst() return a list with all files name
            if file.startswith(BASE_FILES_NAME): # if file isn't already on folder
                zipfiles_names_ftp.append(file)
        print("All zipfiles names collected from ftp server.\n")
    except:
        print_error()

while len(zipfiles_names_ftp) != len(zipfiles_names_dir):
    try:
        download_zipfiles(SITE, FTP_FOLDER, BASE_FILES_NAME, ZIP_FILES_PATH, zipfiles_names_dir, zipfiles_names_ftp)
    except EOFError: # server constantly is disconnected and give this error
        print("EOFError, reconnecting...\n")
        pass
    except:
        print_error()
    zipfiles_names_dir = os.listdir(ZIP_FILES_PATH)
