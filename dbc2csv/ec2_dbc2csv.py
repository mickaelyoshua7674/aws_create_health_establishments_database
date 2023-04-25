from helper_functions import *
import os
import gc

BUCKET_NAME = "laos-datasus"
BUCKET_FOLDER_DBCFILES = "siasus/dbcfiles/"
BUCKET_FOLDER_RAW_TABLES = "siasus/raw_tables/"

base_file_name = os.getenv("BASE_FILE_NAME")
dbc_file_name = base_file_name + ".dbc"
csv_file_name = base_file_name + ".csv"
print(base_file_name)

# s3_client = boto3.client("s3")
# s3_resource = boto3.resource("s3")

# download_file_bucket(s3_client, BUCKET_NAME, dbc_file_name, BUCKET_FOLDER_DBCFILES)

# print(f"Converting {dbc_file_name} to csv...")
# os.system(f"DBC_FILE_PATH=./{dbc_file_name} CSV_FILE_PATH=./{csv_file_name} docker-compose up")
# print(f"{dbc_file_name} converted.\n")

# upload_file_to_bucket(s3_resource, csv_file_name, BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES)

# os.remove(dbc_file_name)
# os.remove(csv_file_name)

# os.system(f"DBC_FILE_PATH=./{dbc_file_name} CSV_FILE_PATH=./{csv_file_name} docker-compose down")

# gc.collect()