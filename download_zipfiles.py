from ftplib import FTP
import traceback
import sys
import os
import boto3

BUCKET_NAME = "files-cnes-datasus"
BASE_FILES_NAME = "BASE_DE_DADOS_CNES"
SITE = "ftp.datasus.gov.br"
FTP_FOLDER = "cnes"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
#-----------------------------------------------------------------FUNCTIONS-----------------------------------------------------------------#
def print_error() -> None:
    """Print the error message and exit script."""
    traceback.print_exc()
    print("Closing...")
    sys.exit()

def get_list_names_zipfiles_bucket(s3_client: boto3.client, bucket: str) -> list[str]:
    try:
        print("Getting list of zipfiles in S3 Bucket...")
        response = s3_client.list_objects(Bucket=bucket, Prefix="zipfiles/")["Contents"]
        content_zipfiles = [k["Key"] for k in response]
        if len(content_zipfiles) == 1:
            print("No zip files in Bucket folder 'zipfiles/'.\n")
            return []
        else:
            print("Names collected.\n")
            return [item[len("zipfiles/"):] for item in content_zipfiles][1:]
    except:
        print("Error getting names of zipfiles in Bucket.")
        print_error()
    
def download_zipfile(site: str, ftp_folder: str, zip_files_path: str, file: str) -> None:
    """Access the ftp connection, go to folder and download files with the base name passed as an argument."""
    with FTP(site) as ftp:
        try:
            # LOGIN
            if ftp.login().startswith("230"): # ftp.login() enter the connection and return a string with the response
                pass
            else:
                print("Failed to log in.")
                sys.exit()

            # GO TO 'cnes' DIRECTORY
            if ftp.cwd(ftp_folder).startswith("250"): # ftp.cwd() 'change working diretory' to cnes and return a string with the response
                pass
            else:
                print("Failed to change directory.")
                sys.exit()
        except:
            print_error()
        with open(zip_files_path + f"{file}", "wb") as f:
            print(f"Downloading {file}...")
            retCode = ftp.retrbinary(f"RETR {file}", f.write) # download the file and return a string with the response
            if retCode.startswith("226"):
                print(f"{file} downloaded.\n")
            else:
                print(f"Error downloading {file}: {retCode}")

def upload_zipfile(s3_resource: boto3.resource, filename: str, bucket: str, key: str) -> None:
    """Save local zipfile on S3 Bucket folder zipfiles/"""
    try:
        print(f"Uploading {filename}...")
        s3_resource.meta.client.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key="zipfiles/" + key
        )
        print(f"{filename} uploaded.\n")
    except:
        print("Error uploading files.")
        print_error()
#-----------------------------------------------------------------SCRIPT-----------------------------------------------------------------#
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

    # GET ZIPFILES NAMES FROM S3 BUCKET
zipfiles_names_bucket = get_list_names_zipfiles_bucket(s3_client, BUCKET_NAME)

if sorted(zipfiles_names_ftp) == sorted(zipfiles_names_bucket):
    print("All zipfiles are uploaded into S3 Bucket.")
else:
    # DOWNLOADING AND UPLOADING ZIPFILES
    for z in zipfiles_names_ftp:
        if z not in zipfiles_names_bucket:
            try:
                download_zipfile(SITE, FTP_FOLDER, "./", z)
                upload_zipfile(s3_resource, z, BUCKET_NAME, z)
                os.remove(z)
            except EOFError: # server constantly is disconnected and give this error
                print("EOFError, reconnecting...")
                pass
            except:
                print_error()