from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.tasks.basics import key_from_s3_path
from CLIENTNAME_pipelines.config import Config
from prefect import task, unmapped, Parameter, flatten
from CLIENTNAME_pipelines.tasks.snowflake import CLIENTNAMESnowflakeQuery, render_copy_sql
from CLIENTNAME_pipelines.tasks.aws_lambda.DataSource2_title_match import Datasource2TitleMatchTask
from CLIENTNAME_pipelines.tasks.parsers.csv_to_parquet import CsvToParquet
import os

get_sql_template = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "get_titles_to_match.sql"
)
get_sql = render_copy_sql(sql_template_file=get_sql_template)

update_sql_template = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "update_title_matches.sql"
)
update_sql = render_copy_sql(sql_template_file=update_sql_template)


@task
def sql_query_to_lists(sql_query, titles_per_run):
    return [
        [x[0] for x in sql_query[i : i + titles_per_run]]
        for i in range(0, len(sql_query), titles_per_run)
    ]


def Datasource2TitleMatchFlow(
    limit=None,
    ENVIRONMENT="dev",
    **kwargs,
):
    with Flow(
        f"DATASOURCE2 Title Match",
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:
        limit = limit or Parameter("limit", default=1000)
        conf = Config.at_runtime(**flow.env)
        sf_query = CLIENTNAMESnowflakeQuery()

        titles_to_match = sql_query_to_lists(
            sf_query(
                query=get_sql(
                    database=conf["snowflake_database"],
                    stage_schema=conf["snowflake_staging_schema"],
                    metrics_schema=conf["snowflake_metrics_schema"],
                    limit=limit,
                ),
                conf=conf,
            ),
            titles_per_run=50,
        )
        match_task = Datasource2TitleMatchTask()
        mapping_files = match_task.map(
            titles=titles_to_match,
            target_bucket=unmapped(conf["landing_bucket"]),
            target_prefix=unmapped(f"{ENVIRONMENT}/DataSource2"),
            conf=unmapped(conf),
        )

        csv_to_parquet = CsvToParquet()
        staged_files = flatten(
            csv_to_parquet.map(
                source_key=key_from_s3_path.map(mapping_files), conf=unmapped(conf)
            )
        )

        sf_query.map(
            query=update_sql.map(
                database=unmapped(conf["snowflake_database"]),
                schema=unmapped(conf["snowflake_staging_schema"]),
                mount_location=unmapped(conf["snowflake_staging_s3"]),
                file_name=key_from_s3_path.map(staged_files),
            ),
            conf=unmapped(conf),
        )
    return flow


if __name__ == "__main__":
    print("Running DATASOURCE2 Title Match Pipeline")
    flow = Datasource2TitleMatchFlow(ENVIRONMENT="dev")
    flow.run(run_on_schedule=False)
