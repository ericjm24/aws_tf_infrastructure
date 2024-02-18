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

outlook_sql_templates = gather_sql(__file__)


@task
def render_copy_sql(
    database="DEV",
    schema="STAGING",
    mount_location="dev_s3_staging",
    file_name="",
    report=None,
):
    yt_finance_template = Template(outlook_sql_templates[report])
    result = yt_finance_template.render(
        database=database,
        schema=schema,
        mount_location=mount_location,
        file_name=file_name.split("/", 1)[-1],
        flow_run_id=prefect.context.flow_run_id,
    )
    return result


# glue_script = os.path.join(
#     os.path.dirname(os.path.realpath(__file__)), "vendor2_glue_get_schema.py"
# )
glue_script = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "vendor2_glue.py"
)

VENDOR2_BUCKET = "vendor2-prod-stage"
VENDOR2_DATA_SETS = [
    "item_sources",
    "items",
    "global_item_availability",
    "global_item_availability",
    # "historical",
    "item_sources",
    "items",
    # "shows_test",
    "shows",
    "sources",
]
partition_columns = {
    "items": "item_id",
    "items": "item_id",
    "shows": "show_id",
}


@task
def glue_target_prefix(env, dataset):
    return f"{env}/vendor2/{dataset}"


def Vendor2DataFlow(
    dataset=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    with Flow(
        f"DataSource1 CDN Logs Flow",
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        dataset = dataset or Parameter("dataset", default="items")
        conf = Config.at_runtime(**flow.env)
        stage_pref = glue_target_prefix(env=ENVIRONMENT, dataset=dataset)

        glue_job = GlueTask(
            glue_job_name="vendor2_data",
            script_location=glue_script,
        )
        r = glue_job(
            conf=conf,
            data_source_bucket=VENDOR2_BUCKET,
            data_source_prefix=dataset,
            data_target_bucket=conf["staging_bucket"],
            data_target_prefix=stage_pref,
            partition_column=partition_columns.get(dataset, "TIMEITEMSET"),
        )

        sf_loader = CLIENTNAMESnowflakeQuery()
        sql = sf_loader(
            query=render_copy_sql(
                database=conf["snowflake_database"],
                schema=conf["snowflake_staging_schema"],
                mount_location=conf["snowflake_staging_s3"],
                file_name=stage_pref,
                report=dataset,
            ),
            conf=conf,
        )
        sql.set_upstream(r)
    return flow


if __name__ == "__main__":
    # from argparse import ArgumentParser

    # argp = ArgumentParser()
    # argp.add_argument("--dataset", type=str)
    # argp.set_defaults(days_back="items")
    # vargs = argp.parse_args()
    # print(f"Running Vendor2 Flow for dataset {vargs.dataset}")
    from time import sleep

    for data in VENDOR2_DATA_SETS:
        flow = Vendor2DataFlow(dataset=data, ENVIRONMENT="dev")
        flow.run(run_on_schedule=False)
        sleep(120)
