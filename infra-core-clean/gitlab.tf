##################
## DATA SOURCES ##
##################

# Gitlab CE 14.7.0 AMI, from the public gitlab account
data "aws_ami" "gitlab" {
    owners = [782774275127]
    filter {
        name = "name"
        values = ["GitLab CE 14.8.2"]
    }
}

# The user's local PUBLIC ssh pem key.
data "local_file" "pem_key" {
    filename = "${path.module}/${var.public_key_file}"
}

# AWS VPC for the project
data "aws_vpc" "PRD" {
    id = var.vpc_id
}

# Internet gateway for the VPC. Allows internet acess for the EC2 instance.
data "aws_internet_gateway" "PRD-ig" {
  filter {
    name   = "attachment.vpc-id"
    values = [data.aws_vpc.PRD.id]
  }
}

# Elastic IP Address to be used by the EC2 instance.
# Guarantees that we keep the same IP for gitlab if the instance is rebuilt.
data "aws_eip" "gitlab-public-ip" {
  id = var.eip_id
}



###############
## RESOURCES ##
###############

# AWS SSH key pair for the gitlab EC2 instance.
resource "aws_key_pair" "deployer" {
    key_name = "devkey-emiller"
    public_key = data.local_file.pem_key.content
}

# Subnet for the gitlab EC2 instance.
resource "aws_subnet" "gitlab-subnet" {
    vpc_id = data.aws_vpc.PRD.id
    cidr_block = "10.50.10.0/24"
    tags = {
        Name = "gitlab-subnet"
    }
}

# Route table for the gitlab subnet. Provides internet access.
resource "aws_route_table" "gitlab-router" {
    vpc_id = data.aws_vpc.PRD.id
    route {
        cidr_block = "0.0.0.0/0"
        gateway_id = data.aws_internet_gateway.PRD-ig.id
    }

    tags = {
        Name = "gitlab-route-table"
    }
}

# Associates the above route table to its subnet.
resource "aws_route_table_association" "gitlab-router-assoc" {
    subnet_id = aws_subnet.gitlab-subnet.id
    route_table_id = aws_route_table.gitlab-router.id
}

# Network interface for the EC2 instance.
resource "aws_network_interface" "gitlab-network-interface" {
    subnet_id = aws_subnet.gitlab-subnet.id
    private_ips = ["10.50.10.100"]
    security_groups = [aws_security_group.prefect-sec-group.id]

    tags = {
        Name = "gitlab_network_interface"
    }
}

# Associate the network interface to the Elastic IP Address.
# Gives the network interface the reserved public IP address.
resource "aws_eip_association" "eip_assoc" {
  network_interface_id = aws_network_interface.gitlab-network-interface.id
  allocation_id = data.aws_eip.gitlab-public-ip.id
}

# The Gitlab EC2 instance itself.
# Runs the GitLab AMI on a c5.xlarge instance.
# Adds the user data shell script to restore the instance from the s3 bucket.
resource "aws_instance" "gitlab-ec2" {
    ami = data.aws_ami.gitlab.id
    instance_type = "t3a.xlarge"
    key_name = aws_key_pair.deployer.key_name
    network_interface {
        network_interface_id = aws_network_interface.gitlab-network-interface.id
        device_index = 0
    }
    root_block_device {
        volume_type = "gp2"
        volume_size = 50
    }
    tags = {
        Name = "GitLab"
    }
    user_data = templatefile(
        "${path.module}/gl-startup.tftpl",
        {
            aws_access_key = var.aws_access_key,
            aws_secret_key = var.aws_secret_key,
            prefect_private_ip = aws_instance.prefect-ec2.private_ip
        }
    )

    lifecycle {
        ignore_changes = [user_data, ami]
    }
}