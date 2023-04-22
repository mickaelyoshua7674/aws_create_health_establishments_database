from helper_functions import *
import os

BUCKET_NAME = "laos-datasus"
SITE = "ftp.datasus.gov.br"

BUCKET_FOLDER_ZIPFILES = "cnes/zipfiles/"

BUCKET_FOLDER_RAW_TABLES = "cnes/raw_tables/"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# GET ZIPFILES NAMES FROM S3 BUCKET
zipfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_ZIPFILES)

for z in zipfiles_names_bucket:
    year_month = z.split(".")[0][-6:]
    folder = year_month[:4] + "/" + year_month[-2:] + "/" # Getting path to organized tables
    raw_tables_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES + folder)

    base_name_raw_tables_names_bucket = [t.split(".")[0][:-6] for t in raw_tables_names_bucket]

    if sorted(base_name_raw_tables_names_bucket) == sorted(target_tables): # if all target files are already on Bucket folder
        print(f"All files in '{BUCKET_FOLDER_RAW_TABLES + folder}' are updated.")
    else:
        download_file_bucket(s3_client, BUCKET_NAME, z, BUCKET_FOLDER_ZIPFILES)
        unzip_and_organize(s3_client, BUCKET_NAME, z, BUCKET_FOLDER_RAW_TABLES + folder, base_name_raw_tables_names_bucket)
        os.remove(z)