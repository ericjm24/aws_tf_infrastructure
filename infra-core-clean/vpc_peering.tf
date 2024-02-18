# VPC used by data platform in us-east-1
data aws_vpc "prod" {
    id = var.vpc_id
}

# VPC default VPC used by CLIENTNAME-Aurora
data aws_vpc "default_vpc" {
    provider = aws.peer
    id = var.aurora_vpc_id
}

#VPC pairing
resource "aws_vpc_peering_connection" "peer" {
  peer_vpc_id   = data.aws_vpc.default_vpc.id
  vpc_id        = data.aws_vpc.prod.id
  peer_region   = "us-east-2"
  auto_accept   = false
  tags = merge(local.default_tags, 
  { Name = "VPC peering between data platform and Aurora"
    Side = "Requester"
  })
}


resource "aws_vpc_peering_connection_accepter" "peer" {
  provider                  = aws.peer
  vpc_peering_connection_id = aws_vpc_peering_connection.peer.id
  auto_accept               = true

  tags = merge(local.default_tags, 
  { Name = "VPC peering between data platrform and Aurora"
    Side = "Accepter"
  })
}