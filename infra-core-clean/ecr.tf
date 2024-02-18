resource "aws_ecr_repository" "CLIENTNAME_dbt" {
  name                 = "CLIENTNAME_dbt"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

locals {
  repo_namespace = "CLIENTNAME_pipelines"
}

resource "aws_ecr_repository" "CLIENTNAME_pipeline_repo" {
  for_each = toset(local.pipeline_envs)
  name = "${local.repo_namespace}/${each.value}"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
}