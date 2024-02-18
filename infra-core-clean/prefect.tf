data "aws_ami" "vendor3-2" {
  most_recent = true

  filter {
    name = "name"
    values = ["amzn2-ami-hvm-2.0.20220218.3-x86_64-ebs"]#["amzn2-ami-hvm-*-x86_64-ebs"]
  }
  owners = ["vendor3"]
}

###############
## RESOURCES ##
###############

# Security group for the prefect instance.
# Allows SSH and HTTP traffic to the instance ONLY from the CLIENTNAME IP.
# Expose 8000 to connect to Airbyte webapp
# Remote users must VPN into the CLIENTNAME network if they want to connect to Airbyte.
resource "aws_security_group" "prefect-sec-group" {
    name = "prefect-sec-group"
    vpc_id = data.aws_vpc.prd_vpc.id
    ingress {
        from_port = 22
        protocol = "tcp"
        to_port = 22
        cidr_blocks = [var.CLIENTNAME_public_ip]
    }
    ingress {
        from_port = 0
        protocol = "tcp"
        to_port = 65535
        cidr_blocks = [var.CLIENTNAME_public_ip]
    }
    ingress {
        from_port = 0
        protocol = "tcp"
        to_port = 65535
        self = true
    }
    egress {
        from_port        = 0
        to_port          = 0
        protocol         = "-1"
        cidr_blocks      = ["0.0.0.0/0"]
        ipv6_cidr_blocks = ["::/0"]
    }
}


#EC2 instance for prefect
resource "aws_instance" "prefect-ec2" {
    ami = data.aws_ami.vendor3-2.id
    associate_public_ip_address = true
    private_ip = "XX.XX.XX.XX"
    subnet_id = aws_subnet.gitlab-subnet.id
    vpc_security_group_ids = [aws_security_group.prefect-sec-group.id]
    instance_type = "t3a.large"
    key_name = aws_key_pair.deployer.key_name
    root_block_device {
        volume_type = "gp2"
        volume_size = 32
    }
    tags = merge(local.default_tags, {Name = "Prefect Server"})
    user_data = templatefile(
        "${path.module}/prefect-startup.tftpl",
        {
            aws_access_key = var.aws_access_key,
            aws_secret_key = var.aws_secret_key
        }
    )

    lifecycle {
        ignore_changes = [user_data]
    }
}

data "aws_iam_role" "ecs_task_role" {
    name = "CLIENTNAME-ds-ecs-role"
}
data "aws_iam_role" "ecsTaskExecutionRole" {
    name = "CLIENTNAME-ds-ecs-task-execution-role"
}

resource "aws_ecs_task_definition" "prefect_agent_task" {
    family = "prefect-agent"
    requires_compatibilities = ["FARGATE"]
    network_mode = "awsvpc"
    cpu = 512
    memory = 1024
    runtime_platform {
        operating_system_family = "LINUX"
        cpu_architecture = "X86_64"
    }
    container_definitions = <<TASK_DEFINITION
[{
    "name": "prefect-agent",
    "image": "prefecthq/prefect:0.14.13-python3.8",
    "essential": true,
    "command": ["prefect","agent","ecs","start","--cluster","${aws_ecs_cluster.prefect_ecs_cluster.arn}"],
    "environment": [
        {
            "name": "PREFECT__BACKEND",
            "value": "server"
        },
        {
            "name": "PREFECT__SERVER__AGENT__LEVEL",
            "value": "INFO"
        },
        {
            "name": "PREFECT__SERVER__HOST",
            "value": "http://${aws_instance.prefect-ec2.private_ip}"
        },
        {
            "name": "PREFECT__SERVER__PORT",
            "value": "4200"
        }
    ],
    "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
            "awslogs-group": "${aws_cloudwatch_log_group.prefect_ecs_log_group.name}",
            "awslogs-region": "us-east-1",
            "awslogs-stream-prefix": "ecs",
            "awslogs-create-group": "true"
        }
    }
}]
TASK_DEFINITION
    task_role_arn = data.aws_iam_role.ecs_task_role.arn
    execution_role_arn = data.aws_iam_role.ecsTaskExecutionRole.arn
}

resource "aws_ecs_service" "prefect_agent" {
    name = "prefect-agent-service"
    cluster = aws_ecs_cluster.prefect_ecs_cluster.id
    task_definition = aws_ecs_task_definition.prefect_agent_task.arn
    desired_count = 1
    network_configuration {
        subnets = [aws_subnet.gitlab-subnet.id]
        security_groups = [aws_security_group.prefect-sec-group.id]
        assign_public_ip = true
    }
    lifecycle {
        ignore_changes = [capacity_provider_strategy]
    }
}
