from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import date_range, key_from_s3_path
from CLIENTNAME_pipelines.config import Config
from CLIENTNAME_pipelines.utils.sql import gather_sql
from prefect import task, unmapped, Parameter, flatten
from CLIENTNAME_pipelines.tasks.aws_lambda.vendor1_channel_reports import Vendor1ReportsToS3
from CLIENTNAME_pipelines.tasks.parsers import CsvToParquet
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery
from CLIENTNAME_pipelines.tasks.archive import FileArchiveTask
from jinja2 import Template
import prefect

yt_report_templates = gather_sql(__file__)


@task
def render_copy_sql(
    database="DEV", schema="STAGING", mount_location="dev_s3_staging", file_name=""
):
    file_name = file_name.lstrip("/")
    env, source, channel_name, report, file = file_name.split("/", 4)
    if "channel_basic" in report:
        report = "basic_metrics"
    yt_finance_template = Template(yt_report_templates[report])
    result = yt_finance_template.render(
        database=database,
        schema=schema,
        mount_location=mount_location,
        file_name=file_name.split("/", 1)[-1],
        channel_name=channel_name,
        flow_run_id=prefect.context.flow_run_id,
    )
    return result


def Vendor1Flow(channel_name=None, days_back=None, ENVIRONMENT="dev", **kwargs):
    if channel_name:
        flow_name = f"Vendor1 Reports - {channel_name}"
    else:
        flow_name = "Vendor1 Reports"
    with Flow(
        flow_name,
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        days_back = days_back or Parameter("days_back", default=7)
        channel_name = channel_name or Parameter(
            "channel_name", default="midsomer_murders"
        )
        conf = Config.at_runtime(**flow.env)
        start_date, end_date = date_range(days_back)
        # Download all Vendor1 reports for the channel to S3
        YRtS = Vendor1ReportsToS3()
        report_names = YRtS(
            channel_name=channel_name,
            start_date=start_date,
            end_date=end_date,
            conf=conf,
        )

        # Convert all reports to parquet and save in staging.
        # Metrics data needs to have the dependency set explicitly so it waits for the process to finish.
        landed_report_keys = key_from_s3_path.map(path=report_names)
        archiver = FileArchiveTask()
        archiver.map(
            source_bucket=unmapped(conf["landing_bucket"]),
            source_key=landed_report_keys,
            conf=unmapped(conf),
        )

        to_staging = CsvToParquet()
        staged_report_paths = flatten(
            to_staging.map(source_key=landed_report_keys, conf=unmapped(conf))
        )
        sf_loader = CLIENTNAMESnowflakeQuery()

        sf_loader.map(
            query=render_copy_sql.map(
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
    argp.add_argument("channel_name", type=str)
    argp.add_argument("--days-back", dest="days_back", type=int)
    argp.set_defaults(days_back=7)
    vargs = argp.parse_args()
    print(f"Running vendor1 pipeline for channel {vargs.channel_name}")
    flow = Vendor1Flow(vargs.channel_name, vargs.days_back)
    result = flow.run(run_on_schedule=False)
    print(result.is_successful())
