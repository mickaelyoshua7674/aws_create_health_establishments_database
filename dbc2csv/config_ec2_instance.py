script = \
    "sudo yum update -y && " + \
    "sudo yum install git -y && " + \
    "sudo yum install R -y && " + \
    "sudo yum install python3-pip -y && " + \
    "pip install boto3 && " + \
    "git clone https://github.com/mickaelyoshua7674/aws_create_health_establishments_database.git"
"""
sudo yum update -y
sudo yum install git -y
sudo yum install R -y
sudo yum install python3-pip -y
pip install boto3
git clone https://github.com/mickaelyoshua7674/aws_create_health_establishments_database.git
"""

"""
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install r-base -y
sudo apt-get install python3-pip -y
sudo apt-get install awscli -y
pip install boto3
pip install awscli
"""

# config awscli