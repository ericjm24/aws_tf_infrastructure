terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "~> 3.0"
        }
    }
    backend "s3" {
        bucket = "CLIENTNAME-ds-infra-core"
        key = "data-pipelines.tfstate"
        region = "us-east-1"
        workspace_key_prefix = "terraform/data-pipelines"
    }
}

provider "aws" {
    region = "us-east-1"
}

provider "archive" {}

data "terraform_remote_state" "infra_core" {
    backend = "s3"
    workspace = "default"
    config = {
        bucket = "CLIENTNAME-ds-infra-core"
        key = "terraform/gitlab.tfstate"
        region = "us-east-1"
    }
}

locals {
    VALID_ENVS = ["dev"]
    current_env = contains(local.VALID_ENVS, terraform.workspace) ? terraform.workspace : "NULL"
}