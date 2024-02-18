# This security group is to allow inbound connections to Aurora 
# instance using the security group used by Aurora instance
resource "aws_security_group" "CLIENTNAME_aurora_sec_group" {
  name        = "CLIENTNAME_aurora_sec_group"
  vpc_id      = var.vpc_id

#TODO: Add ingress rule with the new Sec group from peer VPC
#   ingress {
#     description      = "MySQL connection"
#     from_port        = 3306
#     to_port          = 3306
#     protocol         = "tcp"
#     security_groups      = [?]
#   }

  tags = merge(local.default_tags, 
    {
        Name = "Aurora connection"
    }
  )
}