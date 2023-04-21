from helper_functions import *
import os

BUCKET_NAME = "laos-datasus"
SITE = "ftp.datasus.gov.br"
FTP_FOLDER_CNES = "cnes"
BASE_FILES_NAME_CNES = "BASE_DE_DADOS_CNES"
BUCKET_FOLDER_ZIPFILES = "cnes/zipfiles/"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# GET ZIPFILES NAMES FROM FTP
zipfiles_names_ftp = get_files_names_ftp(SITE, FTP_FOLDER_CNES, BASE_FILES_NAME_CNES)

# GET ZIPFILES NAMES FROM S3 BUCKET
zipfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_ZIPFILES)

if sorted(zipfiles_names_ftp) == sorted(zipfiles_names_bucket):
    print("All zipfiles are uploaded into S3 Bucket folder.")
else:
    # DOWNLOADING AND UPLOADING ZIPFILES
    for z in zipfiles_names_ftp:
        if z not in zipfiles_names_bucket:
            try:
                download_file_from_ftp(SITE, FTP_FOLDER_CNES, z)
                upload_file_to_bucket(s3_resource, z, BUCKET_NAME, BUCKET_FOLDER_ZIPFILES)
                os.remove(z)
            except EOFError: # server constantly is disconnected and give this error
                print("EOFError, reconnecting...")
                pass
            except:
                print_error()