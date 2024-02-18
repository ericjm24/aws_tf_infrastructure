# CLIENTNAME-data-pipelines

This directory was originally the repository containing all data pipelines and the infrastructure needed to run them.

Data pipelines were executed in a local Prefect instance using AWS ECS as the runtime. Deployment and orchestration of Prefect flows is also handled in this codebase.

AWS Lambda was used to execute small data retrieval operations such as hitting REST API's, email, etc.

Data-intensive calculations were performed using pyspark and AWS Glue.

Excluding the Prefect instance and the GitLab instance hosting the code, all calculations are performed in a serverless, on-demand compute environment to minimize cost while maintaining scalability.

## Terraform

AWS Lambdas are maintained using Hachicorp's Terraform. Extending the Lambda runtime with additional python modules is very difficult if the python package includes non-python code (Numpy is a great example of this, it's mostly written in C). The AWS runtime layer has to be built locally on a dockerized copy of the Lambda runtime and then all code and dependencies must be packaged together in a way that preserves relative links.

Because this project requires some of these python modules, the Terraform script includes all the instructions necessary to:

1. Build and deploy a copy of the AWS Lambda runtime
2. Install all python packages listed in the function's requirements.txt into the Lambda runtime
3. Zip and export packages with dependencies
4. Deploy the Lambda layer
5. Deploy the Lambda function with the custom layer attached

## Security

All access credentials are handled using a combination of AWS IaM rules and AWS Secrets. The dev, test, and prod environments for this particular project were separated into different VPC environments.

## CI/CD

Pipelines are deployed automatically to the dev,test,and prod environments when PRs are merged in to their repspective branches. This is handled by .gitlab-ci.yml. and GitLab CI/CD. Branch protection rules established on the repo require all PRs to these three branches be approved by an administrator following a code review cycle and testing.