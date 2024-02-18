from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import date_prior, key_from_s3_path
from CLIENTNAME_pipelines.config import Config
from CLIENTNAME_pipelines.utils.standard_clocks import daily_clock
from CLIENTNAME_pipelines.utils.sql import gather_sql
from CLIENTNAME_pipelines.tasks.glue import GlueTask
from CLIENTNAME_pipelines.tasks.extract import ExtractTask
from prefect import task, Parameter
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery
from jinja2 import Template
import prefect
import os

# outlook_sql_templates = gather_sql(__file__)

# @task
# def render_copy_sql(
#     database="DEV", schema="STAGING", mount_location="dev_s3_staging", file_name=""
# ):
#     file_name = file_name.lstrip("/")
#     env, source, report, file = file_name.split("/", 3)
#     if "channel_basic" in report:
#         report = "basic_metrics"
#     yt_finance_template = Template(outlook_sql_templates[report])
#     result = yt_finance_template.render(
#         database=database,
#         schema=schema,
#         mount_location=mount_location,
#         file_name=file_name.split("/", 1)[-1],
#     )
#     return result

glue_script = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "glue_script.py"
)

CDN_BUCKET = "cdnusertracker"


@task(nout=2)
def date_to_filenames(d, land_pref):
    cdn_name = f"cdnusertracker.log-{d.strftime('%Y%m%d')}.tar.gz"
    s3_name = os.path.join(land_pref, f"{d.strftime('%Y/%m/%d')}")
    return cdn_name, s3_name


@task
def printf(word):
    prefect.context.logger.info(word)


def FtCdnLogFlow(
    days_back=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    with Flow(
        f"DataSource1 CDN Logs Flow",
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        days_back = days_back or Parameter("days_back", default=7)
        conf = Config.at_runtime(**flow.env)
        start_date = date_prior(days_back=days_back)
        target_pref = f"{ENVIRONMENT}/data_source1/test/"
        cdn_name, s3_name = date_to_filenames(start_date, target_pref)

        tar_extractor = ExtractTask()
        written_files = tar_extractor(
            source_bucket=CDN_BUCKET,
            source_key=cdn_name,
            source_creds=conf["secrets"]["data_source1_creds"],
            target_bucket=conf["landing_bucket"],
            target_prefix=target_pref,
        )
        glue_job = GlueTask(
            glue_job_name="datasource1_cdn_logs",
            script_location=glue_script,
        )
        r = glue_job(
            conf=conf,
            data_source_bucket=conf["landing_bucket"],
            data_source_prefix=s3_name,
            data_target_bucket=conf["staging_bucket"],
            data_target_prefix="dev/data_source1/data/",
        )
        r.set_upstream(written_files)
    return flow


if __name__ == "__main__":
    from argparse import ArgumentParser

    argp = ArgumentParser()
    argp.add_argument("--days-back", dest="days_back", type=int)
    argp.set_defaults(days_back=30)
    vargs = argp.parse_args()
    print(f"Running DataSource1 CDN Log Flow")
    flow = FtCdnLogFlow(days_back=vargs.days_back, ENVIRONMENT="dev")
    flow.run(run_on_schedule=False)
