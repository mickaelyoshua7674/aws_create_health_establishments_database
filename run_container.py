install_config_docker = "sudo yum update -y && " + \
    "sudo yum install docker -y && " + \
    "sudo service docker start && " + \
    "sudo usermod -a -G docker ec2-user && " + \
    "yum install curl && " + \
    "curl -L 'https://github.com/docker/compose/releases/download/1.23.2/docker-compose-$(uname -s)-$(uname -m)' -o /usr/local/bin/docker-compose && " + \
    "chmod +x /usr/local/bin/docker-compose && " + \
    "ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose"

import os

os.system(install_config_docker)

os.system("DBC_FILE_PATH=dbc_files/PAPB2201.dbc CSV_FILE_PATH=csv_files/PAPB2201.csv docker compose up")
#os.system("set DBC_FILE_PATH=./dbc2csv/dbc_files/PAPB2201.dbc&& set CSV_FILE_PATH=./dbc2csv/csv_files/PAPB2201.csv&& docker compose up")