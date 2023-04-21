from helper_functions import *
import os

BUCKET_NAME = "laos-datasus"
SITE = "ftp.datasus.gov.br"

FTP_FOLDER_SIHSUS = "/dissemin/publicos/SIHSUS/200801_/Dados"
BUCKET_FOLDER_SIHSUS_DBCFILES = "sihsus/dbcfiles/"
BASE_FILES_NAME_SIHSUS = "RDPB"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# GET SIHSUS DBCFILES NAMES FROM FTP
sihsus_dbcfiles_names_ftp = get_files_names_ftp(SITE, FTP_FOLDER_SIHSUS, BASE_FILES_NAME_SIHSUS)

# GET SIHSUS DBCFILES NAMES FROM S3 BUCKET
sihsus_dbcfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_SIHSUS_DBCFILES)

if sorted(sihsus_dbcfiles_names_ftp) == sorted(sihsus_dbcfiles_names_bucket):
    print("All SIHSUS dbcfiles are uploaded into S3 Bucket folder.")
else:
    # DOWNLOADING AND UPLOADING SIHSUS DBCFILES
    for d in sihsus_dbcfiles_names_ftp:
        if d not in sihsus_dbcfiles_names_bucket and int(d[4:6]) > 17: # 2018 or above
            try:
                download_file_from_ftp(SITE, FTP_FOLDER_SIHSUS, d)
                upload_file_to_bucket(s3_resource, d, BUCKET_NAME, BUCKET_FOLDER_SIHSUS_DBCFILES)
                os.remove(d)
            except EOFError: # server constantly is disconnected and give this error
                print("EOFError, reconnecting...")
                pass
            except:
                print_error()