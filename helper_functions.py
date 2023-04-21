from ftplib import FTP
import traceback
import sys
import boto3

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

            # GO TO 'cnes' DIRECTORY
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
        print("Getting list of files in S3 Bucket...")
        try:
            response = s3_client.list_objects(Bucket=bucket, Prefix=prefrix)["Contents"]
            content_dbcfiles = [k["Key"] for k in response]
            if len(content_dbcfiles) == 1:
                print(f"No dbc files in Bucket folder {prefrix}.\n")
                return []
            else:
                print("Names collected.\n")
                return [item[len(prefrix):] for item in content_dbcfiles]
        except KeyError: # if folder doesn't exist will be no 'Contents' key so is returned a KeyError
            print("Folder doesn't exist in Bucket.")
            return []
    except:
        print("Error getting names of files in Bucket.")
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