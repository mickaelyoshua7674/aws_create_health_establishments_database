install_config_docker = "sudo yum update -y && " + \
    "sudo yum install docker -y && " + \
    "sudo service docker start && " + \
    "sudo usermod -a -G docker ec2-user"

import subprocess
import os
#p = subprocess.run("ls", shell=True)
#print("The exit code was: %d" % list_files.returncode)

os.system("DBC_FILE_PATH=dbc_files/PAPB2201.dbc CSV_FILE_PATH=csv_files/PAPB2201.csv docker compose up")