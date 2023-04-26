script = \
    "sudo yum update -y && " + \
    "sudo yum install git -y && " + \
    "sudo yum install docker -y && " + \
    "sudo usermod -a -G docker ec2-user && " + \
    "sudo service docker start && " + \
    "sudo chkconfig docker on && " + \
    "sudo curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose && " + \
    "sudo chmod +x /usr/local/bin/docker-compose && " + \
    "sudo yum install python3-pip -y && " + \
    "pip install boto3 && " + \
    "git clone https://github.com/mickaelyoshua7674/aws_create_health_establishments_database.git && " + \
    "newgrp docker"

# config awscli