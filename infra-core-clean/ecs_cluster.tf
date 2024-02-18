resource "aws_cloudwatch_log_group" "prefect_ecs_log_group" {
  name = "/ecs/prefect"
}

resource "aws_ecs_cluster" "prefect_ecs_cluster" {
  name = "CLIENTNAME-ds-prefect-ecs-cluster"
  configuration {
    execute_command_configuration {
      logging    = "OVERRIDE"

      log_configuration {
        cloud_watch_encryption_enabled = false
        cloud_watch_log_group_name     = aws_cloudwatch_log_group.prefect_ecs_log_group.name
        s3_bucket_name = aws_s3_bucket.data_logging.id
        s3_key_prefix = "cloudwatch/ecs/"
      }
    }
  }
  lifecycle {
      ignore_changes = [name]
  }
}

resource "aws_ecs_cluster_capacity_providers" "ecs_fargate" {
  cluster_name = aws_ecs_cluster.prefect_ecs_cluster.name

  capacity_providers = ["FARGATE"]

  default_capacity_provider_strategy {
    base              = 1
    weight            = 100
    capacity_provider = "FARGATE"
  }
}