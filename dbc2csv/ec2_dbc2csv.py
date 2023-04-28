from helper_functions import *
import os
import gc

BUCKET_NAME = os.getenv("BUCKET_NAME")
BUCKET_FOLDER_DBCFILES = os.getenv("BUCKET_FOLDER_DBCFILES")
BUCKET_FOLDER_RAW_TABLES = os.getenv("BUCKET_FOLDER_RAW_TABLES")
BASE_FILE_NAME = os.getenv("BASE_FILE_NAME")

dbc_file_name = BASE_FILE_NAME + ".dbc"
csv_file_name = BASE_FILE_NAME + ".csv"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

download_file_bucket(s3_client, BUCKET_NAME, dbc_file_name, BUCKET_FOLDER_DBCFILES)

print(f"Converting {dbc_file_name} to csv...")
#os.system(f"DBC_FILE_PATH=./{dbc_file_name} CSV_FILE_PATH=./{csv_file_name} Rscript dbc2csv.r")
os.system(f"DBC_FILE_PATH=$(pwd)/{dbc_file_name} CSV_FILE_PATH=$(pwd)/{csv_file_name} Rscript dbc2csv.r")
print(f"{dbc_file_name} converted.\n")

upload_file_to_bucket(s3_resource, csv_file_name, BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES)

os.remove(dbc_file_name)
os.remove(csv_file_name)

gc.collect()