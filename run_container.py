# install_config_docker = "sudo yum update -y && " + \
#     "sudo yum install docker containerd git screen -y && " + \
#     "wget https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) && " + \
#     "sudo mv docker-compose-$(uname -s)-$(uname -m) /usr/libexec/docker/cli-plugins/docker-compose && " + \
#     "chmod +x /usr/libexec/docker/cli-plugins/docker-compose"

install_config_docker = "sudo yum update -y && " + \
    "sudo yum install docker -y && " + \
    "sudo usermod -a -G docker ec2-user && " + \
    "chkconfig docker on && " + \
    "sudo service docker start && " + \
    "curl -O https://bootstrap.pypa.io/get-pip.py && " + \
    "python3 get-pip.py --user && " + \
    "pip3 install docker-compose && " + \
    "sudo reboot"
    

import os

os.system(install_config_docker)

os.system("DBC_FILE_PATH=dbc_files/PAPB2201.dbc CSV_FILE_PATH=csv_files/PAPB2201.csv docker compose up")
#os.system("set DBC_FILE_PATH=./dbc2csv/dbc_files/PAPB2201.dbc&& set CSV_FILE_PATH=./dbc2csv/csv_files/PAPB2201.csv&& docker compose up")