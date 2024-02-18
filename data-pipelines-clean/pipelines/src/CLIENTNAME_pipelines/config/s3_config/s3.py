from ._s3_config import S3File, EncryptedS3File

# test_file = S3File("s3://CLIENTNAME-ds-misc/${ENVIRONMENT}/dev_test.py")

vendor1_creds = EncryptedS3File(
    s3_path="s3://CLIENTNAME-ds-data-infrastructure/config/${ENVIRONMENT}/v1_creds.json.bin",
    key_secret_name="vendor1_creds_encryption_key",
)

pipelines_tf_state = S3File(
    "s3://CLIENTNAME-ds-infra-core/terraform/data-pipelines/${ENVIRONMENT}/data-pipelines.tfstate"
)
core_tf_state = S3File("s3://CLIENTNAME-ds-infra-core/terraform/gitlab.tfstate")
