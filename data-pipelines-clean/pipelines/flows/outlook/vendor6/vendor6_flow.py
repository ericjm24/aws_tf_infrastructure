from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import date_range, key_from_s3_path
from CLIENTNAME_pipelines.config import Config
from prefect import task, unmapped, Parameter
from CLIENTNAME_pipelines.tasks.aws_lambda.outlook_connector import OutlookToS3Connector
from CLIENTNAME_pipelines.tasks.parsers import XlsToParquet
from CLIENTNAME_pipelines.tasks.archive import FileArchiveTask
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery, render_copy_sql
from jinja2 import Template
import re
import boto3
from CLIENTNAME_pipelines.utils.sql import read_sql
from datetime import date
from dateutil.relativedelta import relativedelta
import os
import prefect


@task
def parse_vendor6_filename_for_dates(staged_key):
    reg = r"(?P<year>\d{4})_Q(?P<quarter>\d)_TRC"
    basename = os.path.basename(staged_key)
    m = re.search(reg, basename).groupdict()
    if "year" in m.keys() and "quarter" in m.keys():
        start_date = date(year=int(m["year"]), month=int(m["quarter"]) * 3 - 2, day=1)
        end_date = start_date + relativedelta(months=3)
    else:
        start_date = None
        end_date = None
    return {"start_date": start_date, "end_date": end_date}


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


sql_filename = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "vendor6_finance.sql"
)
sql_template_file = read_sql(sql_filename)
snowflake_table = "VENDOR6_FINANCE"
title_col = "TITLE_NAME"


@task
def render_copy_sql(
    database="DEV",
    schema="STAGING",
    mount_location="dev_s3_staging",
    file_name="",
    **kwargs,
):
    print(f"INCOMING FILE NAME: {file_name}")
    file_name = file_name.lstrip("/")
    sql_template = Template(sql_template_file)
    result = sql_template.render(
        database=database,
        schema=schema,
        table=snowflake_table,
        mount_location=mount_location,
        file_name=file_name.split("/", 1)[-1],
        flow_run_id=prefect.context.get("flow_run_id"),
        **kwargs,
    )
    return result


@task
def filter_attachments(filters=None, attachments=[]):
    if not filters:
        return attachments
    if type(filters) is not list:
        filters = list(filters)
    if type(attachments) is not list:
        attachments = list(attachments)
    return [
        x
        for x in attachments
        if any(re.fullmatch(y, x.rsplit("/", 1)[-1]) for y in filters)
    ]


def Vendor6OutlookFlow(
    days_back=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    partner_name = "vendor6"
    flow_name = "finance"
    subject_line = "trc_stage"
    attachment_title_filters = [".*TRC.*\.xlsx"]
    with Flow(
        f"{partner_name.title()} Flow - {flow_name.upper()}",
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        days_back = days_back or Parameter("days_back", default=7)
        conf = Config.at_runtime(**flow.env)
        start_date, end_date = date_range(days_back=days_back, date_range=days_back - 1)

        outlook_connector = OutlookToS3Connector()
        attachment_files = outlook_connector(
            start_date=start_date,
            end_date=end_date,
            subject=subject_line,
            conf=conf,
        )
        filtered_files = filter_attachments(
            filters=attachment_title_filters, attachments=attachment_files
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
            source_key=sorted_keys, target_key=sorted_keys, conf=unmapped(conf)
        )

        extra_cols = parse_vendor6_filename_for_dates.map(staged_key=sorted_keys)

        sf_loader = CLIENTNAMESnowflakeQuery()

        loaded = sf_loader.map(
            query=render_copy_sql.map(
                database=unmapped(conf["snowflake_database"]),
                schema=unmapped(conf["snowflake_staging_schema"]),
                mount_location=unmapped(conf["snowflake_staging_s3"]),
                file_name=key_from_s3_path.map(path=staged_report_paths),
                extra_cols=extra_cols,
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
    print("Running Vendor6 Finance Pipeline")
    flow = Vendor6OutlookFlow(days_back=vargs.days_back, ENVIRONMENT="dev")
    flow.run(run_on_schedule=False)
