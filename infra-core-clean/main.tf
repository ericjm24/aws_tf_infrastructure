terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "~> 3.0"
        }
    }
    backend "s3" {
        bucket = "CLIENTNAME-ds-infra-core"
        key = "terraform/gitlab.tfstate"
        region = "us-east-1"
    }
}

provider "aws" {
    region = "us-east-1"
    profile = "default"
}

provider "aws" {
  alias  = "peer"
  region = "us-east-2"
}

locals {
    pipeline_envs = ["dev", "test", "prod"]
}