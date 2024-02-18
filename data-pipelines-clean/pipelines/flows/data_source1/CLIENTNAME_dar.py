from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import date_prior, key_from_s3_path
from CLIENTNAME_pipelines.config import Config
from CLIENTNAME_pipelines.tasks.extract import ExtractTask
from CLIENTNAME_pipelines.tasks.parsers import CsvToParquet
from prefect import task, unmapped, Parameter, flatten
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery, render_copy_sql
import prefect
import os
from datetime import timedelta


CDN_BUCKET = "cdnusertracker"


@task(nout=2)
def date_to_filenames(d, land_pref):
    cdn_name = f"CLIENTNAME_DAR_Report_{d.strftime('%Y%m%d')}.zip"
    target_prefix = os.path.join(land_pref, f"{d.strftime('%Y/%m')}")
    return cdn_name, target_prefix


@task
def next_day(d):
    return d + timedelta(days=1)


@task
def printf(word):
    prefect.context.logger.info(word)


sql_template = os.path.join(os.path.dirname(os.path.realpath(__file__)), "ft_dar.sql")
copy_sql = render_copy_sql(sql_template_file=sql_template)


def FtDarFlow(
    days_back=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    with Flow(
        f"DataSource1 Detailed Asset Reports",
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        days_back = days_back or Parameter("days_back", default=7)
        conf = Config.at_runtime(**flow.env)
        start_date = date_prior(days_back=days_back)
        end_date = next_day(start_date)
        cdn_name, target_prefix = date_to_filenames(
            start_date, f"{ENVIRONMENT}/data_source1/DAR/"
        )

        zip_extractor = ExtractTask()
        written_files = zip_extractor(
            source_bucket=CDN_BUCKET,
            source_key=cdn_name,
            source_creds=conf["secrets"]["data_source1_creds"],
            target_bucket=conf["landing_bucket"],
            target_prefix=target_prefix,
            archive_bucket=conf["archive_bucket"],
        )
        to_staging = CsvToParquet()
        staged_file_paths = flatten(
            to_staging.map(
                source_key=key_from_s3_path.map(written_files), conf=unmapped(conf)
            )
        )

        sf_loader = CLIENTNAMESnowflakeQuery()
        sf_loader.map(
            query=copy_sql.map(
                database=unmapped(conf["snowflake_database"]),
                schema=unmapped(conf["snowflake_staging_schema"]),
                mount_location=unmapped(conf["snowflake_staging_s3"]),
                file_name=key_from_s3_path.map(staged_file_paths),
                start_date=unmapped(start_date),
                end_date=unmapped(end_date),
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
    print(f"Running DataSource1 Detailed Asset Report Log Flow")
    flow = FtDarFlow(days_back=vargs.days_back, ENVIRONMENT="dev")
    flow.run(run_on_schedule=False)
