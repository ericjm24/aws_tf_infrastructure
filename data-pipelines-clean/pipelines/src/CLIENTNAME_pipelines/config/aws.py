########################################
## Buckets
landing_bucket = "CLIENTNAME-ds-data-landing"
staging_bucket = "CLIENTNAME-ds-data-staging"
infra_bucket = "CLIENTNAME-ds-data-infrastructure"
logging_bucket = "CLIENTNAME-ds-data-logging"
archive_bucket = "CLIENTNAME-ds-data-archive"

glue_service_role = (
    "arn:aws:iam::AWSACCOUNTID:role/service-role/AWSGlueServiceRole-Main"
)
