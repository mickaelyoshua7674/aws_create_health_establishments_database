from helper_functions import *
import time

BUCKET_NAME = "laos-datasus"
BUCKET_FOLDER_DBCFILES = "sihsus/dbcfiles/"
BUCKET_FOLDER_RAW_TABLES = "sihsus/raw_tables/"

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

ec2_client = boto3.client("ec2")
ssm_client = boto3.client("ssm")

# GET THE EC2 INSTANCE ID
ec2_dbc2csv = ec2_client.describe_instances(Filters = [{"Name": "tag:Name", "Values": ["dbc2csv"]}])
ec2_dbc2csv_id = ec2_dbc2csv["Reservations"][0]["Instances"][0]["InstanceId"]

# GET DBCFILES NAMES FROM BUCKET
dbcfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_DBCFILES)

# GET CSVFILES NAMES FROM BUCKET
csvfiles_names_bucket = get_files_names_bucket(s3_client, BUCKET_NAME, BUCKET_FOLDER_RAW_TABLES)

for dbc_file_name in dbcfiles_names_bucket:
    base_file_name = dbc_file_name.split(".")[0]
    csv_file_name = base_file_name + ".csv"

    if csv_file_name not in csvfiles_names_bucket:
        print(f"Converting {dbc_file_name} to csv and uploading the csv...")
        
        # SEND COMMAND TO EC2
        script = \
            "cd aws_create_health_establishments_database/dbc2csv/ && " + \
            "git pull && " + \
            f"BUCKET_NAME={BUCKET_NAME} BUCKET_FOLDER_DBCFILES={BUCKET_FOLDER_DBCFILES} " + \
            f"BUCKET_FOLDER_RAW_TABLES={BUCKET_FOLDER_RAW_TABLES} BASE_FILE_NAME={base_file_name} python3 ec2_dbc2csv.py"
        
        print("Executing command in EC2 Instance...")
        # RUN SHELL SCRIPT
        response_send = ssm_client.send_command(
            DocumentName ="AWS-RunShellScript",
            Parameters = {"commands": [script]},
            InstanceIds = [ec2_dbc2csv_id]
        )

        # CHECK STATUS OF COMMAND
        time.sleep(1)
        response_status = ssm_client.get_command_invocation(
            CommandId=response_send["Command"]["CommandId"],
            InstanceId=ec2_dbc2csv_id
        )
        status = response_status["Status"]
        print(status + "...")

        while True:
            response_status = ssm_client.get_command_invocation(
                CommandId=response_send["Command"]["CommandId"],
                InstanceId=ec2_dbc2csv_id
            )
            new_status = response_status["Status"]

            if new_status in ["Success", "Cancelled", "Failed", "TimedOut"]: # if command ends
                status = new_status
                print(f"Final status of command is {new_status}.")
                break
            elif status == new_status: # if is still the same status
                pass
            else: # if change the status but is not finished
                status = new_status
                print(status + "...")
        
        if status != "Success":
            print("Somethig went wrong on runing script on EC2 Instance.")
            sys.exit()
        else:
            print(f"{dbc_file_name} converted and uploaded.\n")