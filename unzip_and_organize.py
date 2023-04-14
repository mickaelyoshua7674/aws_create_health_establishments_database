import io
import csv
import zipfile
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
    """Get list of all zipfiles in folder 'zipfiles/'"""
    try:
        print("Getting list of zipfiles in S3 Bucket...")
        response = s3_client.list_objects(Bucket=bucket)["Contents"]
        content_zipfiles = [k["Key"] for k in response if k["Key"].startswith("zipfiles/")]
        if len(content_zipfiles) == 1:
            print("No zip files in Bucket folder 'zipfiles/'.\n")
            return []
        else:
            print("Names collected.\n")
            return [item[len("zipfiles/"):] for item in content_zipfiles][1:]
    except:
        print("Error getting names of zipfiles in Bucket.")
        print_error()

def download_zipfile_bucket(s3_client: boto3.client, bucket: str, file: str) -> None:
    """Download zipfile from S3 Bucket in the same folder that the script is located."""
    print(f"\nDownloading {file}...")
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
    response = s3_client.list_objects(Bucket=bucket)["Contents"]
    return [k["Key"] for k in response if k["Key"].startswith("raw_tables/" + folder)]
#-----------------------------------------------------------------SCRIPT-----------------------------------------------------------------#
# GET ZIPFILES NAMES FROM 'zipfiles/'
names_zipfiles_bucket = get_list_names_zipfiles_bucket(s3_client, BUCKET_NAME)

# DOWNLOAD ZIPFILES, UNZIP AND WRTIE CSV INTO 'raw_tables/'
for z in names_zipfiles_bucket:
    year_month = z.split(".")[0][-6:]
    folder = year_month[:4] + "/" + year_month[-2:] + "/"
    if len(get_list_names_raw_tables_bucket(s3_client, BUCKET_NAME, folder)) == 0:
        download_zipfile_bucket(s3_client, BUCKET_NAME, z)
        unzip_and_organize(s3_client, BUCKET_NAME, z, folder)
        os.remove(z)