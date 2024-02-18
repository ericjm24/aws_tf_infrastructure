from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import date_range, key_from_s3_path, filter_files
from CLIENTNAME_pipelines.config import Config
from prefect import task, unmapped, Parameter, flatten
from CLIENTNAME_pipelines.tasks.aws_lambda.outlook_connector import OutlookToS3Connector
from CLIENTNAME_pipelines.tasks.parsers import CsvToParquet
from CLIENTNAME_pipelines.tasks.archive import FileArchiveTask
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery, render_copy_sql
import boto3
import os

VENDOR3_JOBS = {
    "fkv": {
        "partner_name": "vendor3",
        "flow_name": "fkv",
        "subject_line": "amzn_pvd_FKV_stage",
    },
    "intl": {
        "partner_name": "vendor3",
        "flow_name": "international",
        "subject_line": "amzn_pvd_INTL_stage",
    },
    "fest": {
        "partner_name": "vendor3",
        "flow_name": "festival",
        "subject_line": "amzn_pvd_FEST",
    },
}


@task
def get_amz_flow(flow_name):
    return VENDOR3_JOBS.get(flow_name, {})


@task
def get_prop(d, key):
    return d.get(key, None)


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
    os.path.dirname(os.path.realpath(__file__)), "vendor3_revenue.sql"
)
copy_sql = render_copy_sql(sql_template_file=sql_template)


def Vendor3OutlookFlow(
    days_back=None,
    amz_flow_name=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    if amz_flow_name:
        full_flow_name = f"Vendor3 Flow - {amz_flow_name.upper()}"
    else:
        full_flow_name = "Vendor3 Flow"
    with Flow(
        full_flow_name,
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        days_back = days_back or Parameter("days_back", default=7)
        amz_flow_name = amz_flow_name or Parameter("amz_flow_name", default="intl")
        flow_details = get_amz_flow(amz_flow_name)
        subject_line = get_prop(flow_details, "subject_line")
        sender_address = get_prop(flow_details, "sender_address")
        attachment_title_filters = get_prop(flow_details, "attachment_title_filters")
        partner_name = get_prop(flow_details, "partner_name")
        flow_name = get_prop(flow_details, "flow_name")
        conf = Config.at_runtime(**flow.env)
        start_date, end_date = date_range(days_back=days_back, date_range=days_back - 1)

        outlook_connector = OutlookToS3Connector()
        attachment_files = outlook_connector(
            start_date=start_date,
            end_date=end_date,
            subject=subject_line,
            sender=sender_address,
            conf=conf,
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

        to_staging = CsvToParquet()
        staged_report_paths = flatten(
            to_staging.map(
                source_key=sorted_keys, target_key=sorted_keys, conf=unmapped(conf)
            )
        )

        sf_loader = CLIENTNAMESnowflakeQuery()

        sf_loader.map(
            query=copy_sql.map(
                database=unmapped(conf["snowflake_database"]),
                schema=unmapped(conf["snowflake_staging_schema"]),
                mount_location=unmapped(conf["snowflake_staging_s3"]),
                file_name=key_from_s3_path.map(path=staged_report_paths),
                report_type=unmapped(amz_flow_name),
            ),
            conf=unmapped(conf),
        )

    return flow


if __name__ == "__main__":
    from argparse import ArgumentParser

    argp = ArgumentParser()
    argp.add_argument("--days-back", dest="days_back", type=int)
    argp.add_argument("--report", type=int)
    argp.set_defaults(days_back=30, report=0)
    vargs = argp.parse_args()
    key = sorted(VENDOR3_JOBS.keys())[vargs.report]
    task = VENDOR3_JOBS[key]
    print(f"Running {task['partner_name'].title()} Pipeline {task['flow_name']}")
    flow = Vendor3OutlookFlow(
        days_back=vargs.days_back, ENVIRONMENT="dev", amz_flow_name=key
    )
    flow.run(run_on_schedule=False)
