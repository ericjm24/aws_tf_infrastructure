output "data_infrastructure_bucket" {
  value = aws_s3_bucket.data_infrastructure.bucket
}

output "data_landing_bucket" {
  value = aws_s3_bucket.data_landing.bucket
}

output "data_staging_bucket" {
  value = aws_s3_bucket.data_staging.bucket
}

output "data_logging_bucket" {
  value = aws_s3_bucket.data_logging.bucket
}

output "data_archive_bucket" {
  value = aws_s3_bucket.data_archive.bucket
}

output "prefect_server_ip" {
  value = aws_instance.prefect-ec2.public_ip
}

output "prefect_task_role_arn" {
  value = data.aws_iam_role.ecs_task_role.arn
}

output "prefect_execution_role_arn" {
  value = data.aws_iam_role.ecsTaskExecutionRole.arn
}

output "prefect_subnet_id" {
  value = aws_subnet.gitlab-subnet.id
}

output "prefect_security_group_id" {
  value = aws_security_group.prefect-sec-group.id
}

output "gitlab_public_ip" {
  value = data.aws_eip.gitlab-public-ip.public_ip
}

output "gitlab_private_ip" {
  value = aws_instance.gitlab-ec2.private_ip
}

output "prefect_image_repo_url" {
  value = regex("(.*)/[^/]*$", aws_ecr_repository.CLIENTNAME_pipeline_repo[local.pipeline_envs[0]].repository_url)[0]
}