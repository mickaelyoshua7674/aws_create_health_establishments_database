install_config_docker = "yum update && " + \
    "yum -y install docker && " + \
    "service docker start && " + \
    "usermod -a -G docker ec2-user && " + \
    "chkconfig docker on && " + \
    "pip3 install docker-compose && " + \
    "reboot"
    
import os

os.system(install_config_docker)

os.system("DBC_FILE_PATH=dbc_files/PAPB2201.dbc CSV_FILE_PATH=csv_files/PAPB2201.csv docker compose up")
#os.system("set DBC_FILE_PATH=./dbc2csv/dbc_files/PAPB2201.dbc&& set CSV_FILE_PATH=./dbc2csv/csv_files/PAPB2201.csv&& docker compose up")