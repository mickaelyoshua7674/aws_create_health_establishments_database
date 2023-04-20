from ftplib import FTP
import traceback
import sys
import os
import boto3

BUCKET_NAME = "files-cnes-datasus"
SITE = "ftp.datasus.gov.br"
FTP_FOLDER_SIASUS = "/dissemin/publicos/SIASUS/200801_/Dados"
FTP_FOLDER_SIHSUS = "/dissemin/publicos/SIHSUS/200801_/Dados"
BASE_FILES_NAME_SIASUS = "PAPB"
BASE_FILES_NAME_SIHSUS = "RDPB"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

def print_error() -> None:
    """Print the error message and exit script."""
    traceback.print_exc()
    print("Closing...")
    sys.exit()

def get_ftp_files_names(site: str, ftp_folder: str, base_file_name: str) -> list[str]:
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
                print(f"Directory changed to {ftp_folder}.\n")
            else:
                print("Failed to change directory.")
                sys.exit()
        except:
            print_error()
        try:
            f = [f for f in ftp.nlst() if f.startswith(base_file_name)] # ftp.nlst() return a list with all files name
            print("All zipfiles names collected from ftp server.\n")
            return f
        except:
            print_error()

def get_list_names_dbcfiles_bucket(s3_client: boto3.client, bucket: str, prefrix: str) -> list[str]:
    try:
        print("Getting list of zipfiles in S3 Bucket...")
        try:
            response = s3_client.list_objects(Bucket=bucket, Prefix="dbcfiles/"+prefrix)["Contents"]
            content_dbcfiles = [k["Key"] for k in response]
            if len(content_dbcfiles) == 1:
                print(f"No dbc files in Bucket folder 'dbcfiles/'+{prefrix}.\n")
                return []
            else:
                print("Names collected.\n")
                return [item[len("dbcfiles/"+prefrix):] for item in content_dbcfiles]
        except KeyError: # if folder doesn't exist will be no 'Contents' key so is returned a KeyError
            return []
    except:
        print("Error getting names of zipfiles in Bucket.")
        print_error()

def upload_dbcfile(s3_resource: boto3.resource, filename: str, bucket: str, key: str, prefrix: str) -> None:
    """Save local dbcfile on S3 Bucket folder dbcfiles/"""
    try:
        print(f"Uploading {filename}...")
        s3_resource.meta.client.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key="dbcfiles/" + prefrix + key
        )
        print(f"{filename} uploaded.\n")
    except:
        print("Error uploading files.")
        print_error()

def download_dbcfile(site: str, ftp_folder: str, dbc_files_path: str, file: str) -> None:
    """Access the ftp connection, go to folder and download files with the base name passed as an argument."""
    with FTP(site) as ftp:
        try:
            # LOGIN
            if ftp.login().startswith("230"): # ftp.login() enter the connection and return a string with the response
                pass
            else:
                print("Failed to log in.")
                sys.exit()

            # GO TO ftp_folder DIRECTORY
            if ftp.cwd(ftp_folder).startswith("250"): # ftp.cwd() 'change working diretory' to cnes and return a string with the response
                pass
            else:
                print("Failed to change directory.")
                sys.exit()
        except:
            print_error()
        with open(dbc_files_path + f"{file}", "wb") as f:
            print(f"Downloading {file}...")
            retCode = ftp.retrbinary(f"RETR {file}", f.write) # download the file and return a string with the response
            if retCode.startswith("226"):
                print(f"{file} downloaded.\n")
            else:
                print(f"Error downloading {file}: {retCode}")


# GET DBCFILES NAMES FROM FTP
dbcfiles_names_ftp_siasus = get_ftp_files_names(SITE, FTP_FOLDER_SIASUS, BASE_FILES_NAME_SIASUS)

# GET DBCFILES NAMES FROM S3 BUCKET
dbcfiles_names_bucket_siasus = get_list_names_dbcfiles_bucket(s3_client, BUCKET_NAME, "siasus/")

if sorted(dbcfiles_names_ftp_siasus) == sorted(dbcfiles_names_bucket_siasus):
    print("All SIASUS dbc files are uploaded into S3 Bucket.")
else:
    # DOWNLOADING AND UPLOADING ZIPFILES
    for z in dbcfiles_names_ftp_siasus:
        if z not in dbcfiles_names_bucket_siasus:
            try:
                download_dbcfile(SITE, FTP_FOLDER_SIASUS, "./", z)
                upload_dbcfile(s3_resource, z, BUCKET_NAME, z, "siasus/")
                os.remove(z)
            except EOFError: # server constantly is disconnected and give this error
                print("EOFError, reconnecting...")
                pass
            except:
                print_error()


# GET DBCFILES NAMES FROM FTP
dbcfiles_names_ftp_sihsus = get_ftp_files_names(SITE, FTP_FOLDER_SIHSUS, BASE_FILES_NAME_SIHSUS)

# GET DBCFILES NAMES FROM S3 BUCKET
dbcfiles_names_bucket_sihsus = get_list_names_dbcfiles_bucket(s3_client, BUCKET_NAME, "sihsus/")

if sorted(dbcfiles_names_ftp_sihsus) == sorted(dbcfiles_names_bucket_sihsus):
    print("All SIHSUS dbc files are uploaded into S3 Bucket.")
else:
    # DOWNLOADING AND UPLOADING ZIPFILES
    for z in dbcfiles_names_ftp_sihsus:
        if z not in dbcfiles_names_bucket_sihsus:
            try:
                download_dbcfile(SITE, FTP_FOLDER_SIHSUS, "./", z)
                upload_dbcfile(s3_resource, z, BUCKET_NAME, z, "sihsus/")
                os.remove(z)
            except EOFError: # server constantly is disconnected and give this error
                print("EOFError, reconnecting...")
                pass
            except:
                print_error()