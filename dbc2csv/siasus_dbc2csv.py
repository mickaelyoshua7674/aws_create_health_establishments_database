from helper_functions import *

BUCKET_NAME = "laos-datasus"
BUCKET_FOLDER_DBCFILES = "siasus/dbcfiles/"
BUCKET_FOLDER_RAW_TABLES = "siasus/raw_tables/"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# GET DBCFILES NAMES FROM BUCKET
dbcfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_DBCFILES)

# GET CSVFILES NAMES FROM BUCKET
csvfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES)

for dbc_file_name in dbcfiles_names_bucket:
    csv_file_name = dbc_file_name.split(".")[0] + ".csv"

    if csv_file_name not in csvfiles_names_bucket:
        print(f"Converting {dbc_file_name} to csv and uploading the csv...")
        # SEND COMMAND TO EC2
        check = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES + csv_file_name)
        if len(check) == 1:
            print(f"{dbc_file_name} converted and uploaded.\n")
        elif len(check) == 0:
            print(f"Failed to convert and upload {dbc_file_name}.\n")
        else:
            print("When checking if csv is on Bucket returned more then one file.")