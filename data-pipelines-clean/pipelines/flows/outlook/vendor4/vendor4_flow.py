from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import date_range, key_from_s3_path, filter_files
from CLIENTNAME_pipelines.config import Config
from prefect import task, unmapped, Parameter
from CLIENTNAME_pipelines.tasks.aws_lambda.outlook_connector import OutlookToS3Connector
from CLIENTNAME_pipelines.tasks.parsers import XlsToParquet
from CLIENTNAME_pipelines.tasks.archive import FileArchiveTask
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery, render_copy_sql
import boto3
import os


@task
def staging_key(source_key="", partner_name="", flow_name="", env="dev"):
    basename = source_key.rsplit("/", 1)[-1]
    stage_key = (
        f"{env}/{partner_name}/{partner_name.lower()}_{flow_name.lower()}/{basename}"
    )
    stage_key = stage_key.replace(" ", "_")
    return stage_key


@task
def move_s3_file(bucket="", source_key="", target_key=""):
    s3_client = boto3.client("s3")
    s3_client.copy(
        CopySource={"Bucket": bucket, "Key": source_key}, Bucket=bucket, Key=target_key
    )
    return target_key


sql_template = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "vendor4.sql"
)
copy_sql = render_copy_sql(sql_template_file=sql_template)
snowflake_table = "VENDOR4_REVENUE"
title_col = "TITLE"


def Vendor4OutlookFlow(
    days_back=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    partner_name = "vendor4"
    flow_name = "revenue"
    subject_line = "vendor4"
    attachment_title_filters = [r"87958735_\d{4}_ZZ\.xlsx"]
    with Flow(
        f"{partner_name.title()} Flow - {flow_name.upper()}",
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        days_back = days_back or Parameter("days_back", 30)
        conf = Config.at_runtime(**flow.env)
        start_date, end_date = date_range(days_back=days_back, date_range=days_back - 1)

        outlook_connector = OutlookToS3Connector()
        attachment_files = outlook_connector(
            start_date=start_date,
            end_date=end_date,
            subject=subject_line,
            conf=conf,
            attachment_type=".xlsx",
        )

        filtered_files = filter_files(
            filters=attachment_title_filters, files=attachment_files
        )

        keys = key_from_s3_path.map(path=filtered_files)

        stage_keys = staging_key.map(
            source_key=keys,
            partner_name=unmapped(partner_name),
            flow_name=unmapped(flow_name),
            env=unmapped(conf["ENVIRONMENT"]),
        )

        sorted_keys = move_s3_file.map(
            bucket=unmapped(conf["landing_bucket"]),
            source_key=keys,
            target_key=stage_keys,
        )

        archiver = FileArchiveTask()
        archiver.map(
            source_bucket=unmapped(conf["landing_bucket"]),
            source_key=sorted_keys,
            conf=unmapped(conf),
        )

        to_staging = XlsToParquet()
        staged_report_paths = to_staging.map(
            source_key=sorted_keys,
            target_key=sorted_keys,
            conf=unmapped(conf),
            xls_kwargs=unmapped({"header": 0, "skipfooter": 4}),
        )

        sf_loader = CLIENTNAMESnowflakeQuery()

        loaded = sf_loader.map(
            query=copy_sql.map(
                database=unmapped(conf["snowflake_database"]),
                schema=unmapped(conf["snowflake_staging_schema"]),
                mount_location=unmapped(conf["snowflake_staging_s3"]),
                file_name=key_from_s3_path.map(path=staged_report_paths),
            ),
            conf=unmapped(conf),
        )

    return flow


if __name__ == "__main__":
    from argparse import ArgumentParser

    argp = ArgumentParser()
    argp.add_argument("--days-back", dest="days_back", type=int)
    argp.set_defaults(days_back=30)
    vargs = argp.parse_args()
    print("Running Apple Vendor4 Pipeline")
    flow = Vendor4OutlookFlow(days_back=vargs.days_back, ENVIRONMENT="dev")
    flow.run(run_on_schedule=False)
