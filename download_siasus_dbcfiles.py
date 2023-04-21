from helper_functions import *
import os

BUCKET_NAME = "laos-datasus"
SITE = "ftp.datasus.gov.br"

FTP_FOLDER_SIASUS = "/dissemin/publicos/SIASUS/200801_/Dados"
BUCKET_FOLDER_SIASUS_DBCFILES = "siasus/dbcfiles/"
BASE_FILES_NAME_SIASUS = "PAPB"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# GET SIASUS DBCFILES NAMES FROM FTP
siasus_dbcfiles_names_ftp = get_files_names_ftp(SITE, FTP_FOLDER_SIASUS, BASE_FILES_NAME_SIASUS)

# GET SIASUS DBCFILES NAMES FROM S3 BUCKET
siasus_dbcfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_SIASUS_DBCFILES)

if sorted(siasus_dbcfiles_names_ftp) == sorted(siasus_dbcfiles_names_bucket):
    print("All SIASUS dbcfiles are uploaded into S3 Bucket folder.")
else:
    # DOWNLOADING AND UPLOADING SIASUS DBCFILES
    for d in siasus_dbcfiles_names_ftp:
        if d not in siasus_dbcfiles_names_bucket and int(d[4:6]) > 17: # 2018 or above
            try:
                download_file_from_ftp(SITE, FTP_FOLDER_SIASUS, d)
                upload_file_to_bucket(s3_resource, d, BUCKET_NAME, BUCKET_FOLDER_SIASUS_DBCFILES)
                os.remove(d)
            except EOFError: # server constantly is disconnected and give this error
                print("EOFError, reconnecting...")
                pass
            except:
                print_error()