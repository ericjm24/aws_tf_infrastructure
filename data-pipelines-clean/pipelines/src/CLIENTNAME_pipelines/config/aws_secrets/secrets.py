from .._base_config import SwitchedData

snowflake_creds = SwitchedData(
    switch="ENVIRONMENT",
    default="arn:aws:secretsmanager:us-east-1:AWSACCOUNTID:secret:snowflake_creds",
    dev="arn:aws:secretsmanager:us-east-1:AWSACCOUNTID:secret:snowflake_creds",
)

data_source1_creds = "data_source1_aws_creds"
