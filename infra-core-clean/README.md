# CLIENTNAME-core-arch

This directory was originally the repository handling the core infrastructure. This includes:

* The GitLab instance hosting all code for the project
* The Prefect server instance that orchestrates and executes data pipelines
* S3 Buckets
* Other AWS services such as ECR, ECS
* Various network connectivity requirements such as VPC peering, subnets, and access policies

## CI/CD

Because this is the core infrastructure, it is not automatically executed by GitLab and instead must be manually deployed by a developer with proper IaM credentials and a private ssh key.

Prefect server startup script (prefect-startup.tftpl) deploys the instance and properly connects it to the ECS backend. Access keys for the IaM role used by the instance must be provided by the user at the time of deployment.

GitLab server startup script (gl-startup.tftpl) establishes the server based on the GitLab Community Edition AWS image and enables a daily backup to be dumped to S3. The startup script will automatically reload the most recent backup of the gitlab instance when it runs. Management of the backups is handled via S3 rules so as not to keep on to too many copies of the backup.

## Outputs

Outputs are included so that the Prefect pipelines can read the names of resources such as S3 buckets and IaM role arns without them needing to be hard-coded into the pipelines. Changes to these objects are then automatically picked up by data pipelines.