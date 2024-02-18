from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import date_range, key_from_s3_path, filter_files
from CLIENTNAME_pipelines.config import Config
from prefect import task, unmapped, flatten, Parameter
from CLIENTNAME_pipelines.tasks.aws_lambda.outlook_connector import OutlookToS3Connector
from CLIENTNAME_pipelines.tasks.parsers import XlsToParquet
from CLIENTNAME_pipelines.tasks.archive import FileArchiveTask
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery
from jinja2 import Template
import boto3
from CLIENTNAME_pipelines.utils.sql import read_sql
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import os
import prefect


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


sql_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), "vendor5.sql")
sql_template_file = read_sql(sql_filename)
snowflake_table = "VENDOR5_MONETIZATION"
title_col = "CLIP_NAME"


@task
def render_copy_sql(
    database="DEV",
    schema="STAGING",
    mount_location="dev_s3_staging",
    file_dict={},
    **kwargs,
):
    outputs = []
    for sheet, file_name in file_dict.items():
        try:
            dt = datetime.strptime(sheet, "%m.%Y")
        except:
            dt = None
        if not dt:
            continue
        print(f"INCOMING FILE NAME: {file_name}")
        file_name = key_from_s3_path.run(file_name).lstrip("/")
        sql_template = Template(sql_template_file)
        result = sql_template.render(
            database=database,
            schema=schema,
            mount_location=mount_location,
            file_name=file_name.split("/", 1)[-1],
            start_date=dt.strftime("%Y-%m-%d"),
            end_date=(dt + relativedelta(months=1)).strftime("%Y-%m-%d"),
            flow_run_id=prefect.context.get("flow_run_id"),
            **kwargs,
        )
        outputs.append(result)
    return outputs


def Vendor5OutlookFlow(
    days_back=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    partner_name = "vendor5"
    flow_name = "monetization"
    subject_line = "vendor5_stage"
    attachment_title_filters = None  # [r"CLIENTNAME.*Revenue.Statement.*\.xlsx?"]
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

        sorted_keys = filter_files(
            filters=None,
            files=move_s3_file.map(
                bucket=unmapped(conf["landing_bucket"]),
                source_key=keys,
                target_key=stage_keys,
            ),
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
            xls_kwargs=unmapped(
                {
                    "sheet_name": None,
                    "header": 0,
                    "skiprows": 4,
                    "thousands": ",",
                    "usecols": "B:H",
                    "sheet_regex": r"\d{2}\.\d{4}",
                }
            ),
        )

        query = render_copy_sql.map(
            database=unmapped(conf["snowflake_database"]),
            schema=unmapped(conf["snowflake_staging_schema"]),
            mount_location=unmapped(conf["snowflake_staging_s3"]),
            file_dict=staged_report_paths,
        )

        sf_loader = CLIENTNAMESnowflakeQuery()

        loaded = sf_loader.map(
            query=flatten(query),
            conf=unmapped(conf),
        )

    return flow


if __name__ == "__main__":
    from argparse import ArgumentParser

    argp = ArgumentParser()
    argp.add_argument("--days-back", dest="days_back", type=int)
    argp.set_defaults(days_back=30)
    vargs = argp.parse_args()
    print("Running Vendor5 TV Monetization Pipeline")
    flow = Vendor5OutlookFlow(days_back=vargs.days_back, ENVIRONMENT="dev")
    flow.run(run_on_schedule=False)
