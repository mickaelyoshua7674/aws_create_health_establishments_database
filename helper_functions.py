from ftplib import FTP
import traceback
import sys
import boto3
import zipfile
import io
import csv
import time

target_tables = ["tbGrupoAtividade", "tbAtividade", "tbGrupoEquipe", "tbTipoEquipe", "tbEquipe",
                 "tbAtividadeProfissional", "tbConselhoClasse", "tbDadosProfissionalSus", "tbCargaHorariaSus",
                 "tbConvenio", "tbAtendimentoPrestado", "rlEstabAtendPrestConv", "tbMunicipio",
                 "tbLeito", "rlEstabComplementar", "tbNaturezaJuridica", "tbTipoUnidade",
                 "tbTipoEstabelecimento", "tbEstabelecimento"]

def print_error() -> None:
    """Print the error message and exit script."""
    traceback.print_exc()
    print("Closing...")
    sys.exit()

def get_files_names_ftp(site: str, ftp_folder: str, base_file_name: str) -> list[str]:
    """Get from ftp connections a list of files names from the given folder starting with the base_file_name"""
    with FTP(site) as ftp:
        try:
            # LOGIN
            if ftp.login().startswith("230"): # ftp.login() enter the connection and return a string with the response
                print("Logged in.\n")
            else:
                print("Failed to log in.")
                sys.exit()

            # GO TO DIRECTORY
            if ftp.cwd(ftp_folder).startswith("250"): # ftp.cwd() 'change working diretory' to cnes and return a string with the response
                print(f"Directory changed to {ftp_folder}.\n")
            else:
                print("Failed to change directory.")
                sys.exit()
        except:
            print_error()
        try:
            f = [f for f in ftp.nlst() if f.startswith(base_file_name)] # ftp.nlst() return a list with all files name
            print("All files names collected from ftp server.\n")
            return f
        except:
            print_error()

def get_files_names_bucket(s3_client: boto3.client, bucket: str, prefrix: str) -> list[str]:
    """Get list of all files names if Se Bucket folder. If folders doesn't exist or is empty return an empty list."""
    try:
        print(f"Getting list of files in S3 Bucket folder '{prefrix}'...")
        try:
            response = s3_client.list_objects(Bucket=bucket, Prefix=prefrix)["Contents"]
            content_dbcfiles = [k["Key"] for k in response]
            if len(content_dbcfiles) == 1:
                print(f"No files in Bucket folder '{prefrix}'.\n")
                return []
            else:
                print("Names collected.\n")
                return [item[len(prefrix):] for item in content_dbcfiles]
        except KeyError: # if folder doesn't exist will be no 'Contents' key so is returned a KeyError
            print(f"'{prefrix}' doesn't exist in Bucket.\n")
            return []
    except:
        print(f"Error getting names of files in Bucket folder '{prefrix}'.")
        print_error()

def upload_file_to_bucket(s3_resource: boto3.resource, filename: str, bucket: str, prefrix: str) -> None:
    """Save local file to S3 Bucket on folder inserted on prefix"""
    try:
        print(f"Uploading {filename}...")
        s3_resource.meta.client.upload_file(
            Filename=filename,
            Bucket=bucket,
            Key=prefrix + filename
        )
        print(f"{filename} uploaded.\n")
    except:
        print("Error uploading files.")
        print_error()

def download_file_from_ftp(site: str, ftp_folder: str, file: str) -> None:
    """Access the ftp connection, go to folder and download files with the base name passed as an argument."""
    with FTP(site) as ftp:
        try:
            # LOGIN
            if ftp.login().startswith("230"): # ftp.login() enter the connection and return a string with the response
                pass
            else:
                print("Failed to log in.")
                sys.exit()

            # GO TO ftp_folder DIRECTORY
            if ftp.cwd(ftp_folder).startswith("250"): # ftp.cwd() 'change working diretory' to cnes and return a string with the response
                pass
            else:
                print("Failed to change directory.")
                sys.exit()
        except:
            print_error()
        with open("./" + file, "wb") as f:
            print(f"Downloading {file}...")
            retCode = ftp.retrbinary(f"RETR {file}", f.write) # download the file and return a string with the response
            if retCode.startswith("226"):
                print(f"{file} downloaded.\n")
            else:
                print(f"Error downloading {file}: {retCode}")

def download_file_bucket(s3_client: boto3.client, bucket: str, file: str, prefix: str) -> None:
    """Download file from S3 Bucket in the same folder that the script is located."""
    print(f"Downloading {file}...")
    s3_client.download_file(
        Bucket=bucket,
        Key=prefix + file,
        Filename="./" + file
    )
    print(f"{file} downloaded.\n")

def unzip_and_organize(s3_client: boto3.client, bucket: str, zip: str, prefix: str, tables_uploaded: list[str]) -> None:
    """Get files from folder and unzip target tables in organized folders."""
    print(f"\nOpening {zip}...")
    with zipfile.ZipFile(zip, "r") as zf: # openning zipfile
        for table_name in zf.namelist():
            base_table_name = table_name.split(".")[0][:-6]

            if base_table_name in target_tables and base_table_name not in tables_uploaded:
                print(f"Writing {table_name} into S3 Bucket...")
                with zf.open(table_name, "r") as table: # opening target file
                    try:
                        r = csv.reader(io.TextIOWrapper(table, "utf-8"), delimiter=";") # decoding and reading file
                        rows = [row for row in r]
                    except UnicodeDecodeError:
                        print("'utf-8' couldn't decode, trying 'ISO-8859-1'...")
                        r = csv.reader(io.TextIOWrapper(table, "ISO-8859-1"), delimiter=";") # decoding and reading file
                        rows = [row for row in r]

                    buff = io.StringIO()
                    csv.writer(buff).writerows(rows)

                    s3_client.put_object(Bucket=bucket, Key=prefix + table_name, Body=buff.getvalue().encode("utf-8", "replace"))
                    print(f"{table_name} written.\n")

def dbc2csv(ssm_client: boto3.client, ec2_client: boto3.client, dbc_file_name: str,
            bucket: str, bucket_folder_dbcfiles: str, bucket_folder_csvfiles: str, instance_id: str) -> None:
    """Run script in EC2 instance converting dbc to csv and uploading the csv."""
    base_file_name = dbc_file_name.split(".")[0]
    print(f"Converting {dbc_file_name} to csv and uploading the csv...")

    # SEND COMMAND TO EC2
    script = \
        "cd aws_create_health_establishments_database/dbc2csv/ && " + \
        "git pull && " + \
        f"BUCKET_NAME={bucket} BUCKET_FOLDER_DBCFILES={bucket_folder_dbcfiles} " + \
        f"BUCKET_FOLDER_RAW_TABLES={bucket_folder_csvfiles} BASE_FILE_NAME={base_file_name} python3 ec2_dbc2csv.py"
    
    print("Executing command in EC2 Instance...")
    # RUN SHELL SCRIPT
    response_send = ssm_client.send_command(
        DocumentName ="AWS-RunShellScript",
        Parameters = {"commands": [script]},
        InstanceIds = [instance_id]
    )

    # CHECK STATUS OF COMMAND
    time.sleep(1)
    response_status = ssm_client.get_command_invocation(
        CommandId=response_send["Command"]["CommandId"],
        InstanceId=instance_id
    )
    status = response_status["Status"]
    print(status + "...")

    start_time = time.time()
    while True:
        response_status = ssm_client.get_command_invocation(
            CommandId=response_send["Command"]["CommandId"],
            InstanceId=instance_id
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

        if time.time() - start_time > 5*60: # if time on loop is bigger then 5min
            ec2_client.reboot_instances(InstanceIds=[instance_id]) # reboot ec2 instance
            time.sleep(5)
    
    if status != "Success":
        print("Somethig went wrong on runing script on EC2 Instance.")
        print(f"Status: {status}")
        sys.exit()
    else:
        print(f"{dbc_file_name} converted and uploaded.\n")