from ._base_config import SwitchedData

snowflake_staging_s3 = "${ENVIRONMENT}_s3_staging"
snowflake_database = "${ENVIRONMENT}"
snowflake_staging_schema = SwitchedData(
    switch="ENVIRONMENT", default="dev", dev="STAGING"
)
snowflake_metrics_schema = "METRICS"
########################################
## DBT
dbt_repo_name = "CLIENTNAME_dbt"
dbt_branch_name = "${ENVIRONMENT}"
dbt_profile_name = "CLIENTNAME_dbt"
