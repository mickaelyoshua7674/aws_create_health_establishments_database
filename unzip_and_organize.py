import io
import csv
import zipfile
import traceback
import sys
import os
import boto3
import shutil

BUCKET_NAME = "files-cnes-datasus"

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
        try:
            response = s3_client.list_objects(Bucket=bucket, Prefix="zipfiles/")["Contents"]
            content_zipfiles = [k["Key"] for k in response]
            if len(content_zipfiles) == 1:
                print("No zip files in Bucket folder 'zipfiles/'.\n")
                return []
            else:
                print("Names collected.\n")
                return [item[len("zipfiles/"):] for item in content_zipfiles][1:]
        except KeyError: # if folder doesn't exist will be no 'Contents' key so is returned a KeyError
            return []
    except:
        print("Error getting names of zipfiles in Bucket.")
        print_error()

def download_zipfile_bucket(s3_client: boto3.client, bucket: str, file: str) -> None:
    """Download zipfile from S3 Bucket in the same folder that the script is located."""
    print(f"Downloading {file}...")
    s3_client.download_file(
        Bucket=bucket,
        Key="zipfiles/" + file,
        Filename="./" + file
    )
    print(f"{file} downloaded.\n")

def unzip_and_organize(s3_client: boto3.client, bucket: str, zip: str, folder: str) -> None:
    """"""
    print(f"\nUnziping {zip}...")
    with zipfile.ZipFile(zip, "r") as zf: # openning zipfile
        print(f"{zip} unziped.\n")
        for f in zf.namelist():
            print(f"Writing {f}...")
            with zf.open(f, "r") as table: # opening target file
                r = csv.reader(io.TextIOWrapper(table, "utf-8"), delimiter=";") # decoding and reading file
                rows = [row for row in r]

                buff = io.StringIO()
                csv.writer(buff).writerows(rows)

                s3_client.put_object(Bucket=bucket, Key="raw_tables/" + folder + f, Body=buff.getvalue().encode("utf-8", "replace"))
                print(f"{f} written.")

def get_list_names_raw_tables_bucket(s3_client: boto3.client, bucket: str, folder: str) -> list[str]:
    """Get list of all content in 'raw_tables/' plus the folder from the input"""
    try:
        response = s3_client.list_objects(Bucket=bucket, Prefix="raw_tables/"+folder)["Contents"]
        return [k["Key"] for k in response if k["Key"]]
    except KeyError: # if folder doesn't exist will be no 'Contents' key so is returned a KeyError
        return []

def upload_tables(s3_resource: boto3.resource, bucket: str, z: str, folder) -> None:
    print(f"Extracting {z}...")
    with zipfile.ZipFile(z, "r") as zf: # openning zipfile
        zf.extractall(folder)
        print(f"{z} extracted.\n")
    os.remove(z)

    print(f"Uploading tables from {z}...")
    for table in os.listdir(folder):
        s3_resource.meta.client.upload_file(
            Filename=folder + table,
            Bucket=bucket,
            Key="raw_tables/" + folder + table
        )
    print("Tables uploaded.\n")
    shutil.rmtree(folder[:4])
#-----------------------------------------------------------------SCRIPT-----------------------------------------------------------------#
count = 0
# GET ZIPFILES NAMES FROM 'zipfiles/'
names_zipfiles_bucket = get_list_names_zipfiles_bucket(s3_client, BUCKET_NAME)
# DOWNLOAD ZIPFILES, UNZIP AND WRTIE CSV INTO 'raw_tables/'
for z in names_zipfiles_bucket:
    year_month = z.split(".")[0][-6:]
    folder = year_month[:4] + "/" + year_month[-2:] + "/"
    names_raw_tables = get_list_names_raw_tables_bucket(s3_client, BUCKET_NAME, folder)
    if len(names_raw_tables) == 0:
        download_zipfile_bucket(s3_client, BUCKET_NAME, z)
        os.makedirs(folder)
        upload_tables(s3_resource, BUCKET_NAME, z, folder)
    else:
        count += 1

if len(names_zipfiles_bucket) == count:
    print("All zip files extracted and tables organized.")