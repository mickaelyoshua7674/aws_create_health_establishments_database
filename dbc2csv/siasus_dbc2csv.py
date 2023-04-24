from helper_functions import *
import os

BUCKET_NAME = "laos-datasus"
BUCKET_FOLDER_DBCFILES = "siasus/dbcfiles/"
BUCKET_FOLDER_RAW_TABLES = "siasus/raw_tables/"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# GET DBCFILES NAMES FROM BUCKET
dbcfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_DBCFILES)

for file in dbcfiles_names_bucket:
    download_file_bucket(s3_client, BUCKET_NAME, file, BUCKET_FOLDER_DBCFILES)

    print(f"Converting {file} to csv...")
    os.system(f"DBC_FILE_PATH=./{file} CSV_FILE_PATH=./{file.split('.')[0]}.csv docker-compose up")
    print(f"{file} converted.\n")

    upload_file_to_bucket(s3_resource, f"{file.split('.')[0]}.csv", BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES)

    os.remove(file)
    os.remove(f"{file.split('.')[0]}.csv")

    os.system("docker-compose down")