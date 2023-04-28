from helper_functions import *
import time

BUCKET_NAME = "laos-datasus"
BUCKET_FOLDER_DBCFILES = "siasus/dbcfiles/"
BUCKET_FOLDER_RAW_TABLES = "siasus/raw_tables/"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

ec2_client = boto3.client("ec2")
ssm_client = boto3.client("ssm")

# GET THE EC2 INSTANCE ID
ec2_dbc2csv = ec2_client.describe_instances(Filters = [{"Name": "tag:Name", "Values": ["r_ubuntu"]}])
ec2_dbc2csv_id = ec2_dbc2csv["Reservations"][0]["Instances"][0]["InstanceId"]

# GET DBCFILES NAMES FROM BUCKET
dbcfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_DBCFILES)

# GET CSVFILES NAMES FROM BUCKET
csvfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES)

print("Rebooting EC2 instance...")
# REBOOT EC2 INSTANCE
ec2_client.reboot_instances(InstanceIds=[ec2_dbc2csv_id])
time.sleep(5)
print("EC2 instance rebooted.\n")

count = 0
for dbc_file_name in dbcfiles_names_bucket:
    base_file_name = dbc_file_name.split(".")[0]
    csv_file_name = base_file_name + ".csv"

    if csv_file_name not in csvfiles_names_bucket:
        dbc2csv(ssm_client, ec2_client, dbc_file_name, BUCKET_NAME, BUCKET_FOLDER_DBCFILES, BUCKET_FOLDER_RAW_TABLES, ec2_dbc2csv_id)
    else:
        count += 1

if count == len(csvfiles_names_bucket):
    print("All files are up to date")