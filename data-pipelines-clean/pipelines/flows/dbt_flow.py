from CLIENTNAME_pipelines.core import Flow
from CLIENTNAME_pipelines.config import Config
from CLIENTNAME_pipelines.tasks.dbt import CLIENTNAMEDbtShellTaskSet
from json import dumps


def build_dbt_run(model="", **kwargs):
    return f"""dbt run --select {model} --vars '{dumps(kwargs)}'"""


dbt_commands = [
    build_dbt_run(
        model="models/facts/fact_content_revenue.sql", provider_platform_nm="vendor1"
    ),
    build_dbt_run(
        model="models/facts/fact_content_revenue.sql", provider_platform_nm="vendor3"
    ),
    build_dbt_run(
        model="models/facts/fact_content_revenue.sql", provider_platform_nm="vendor6"
    ),
    build_dbt_run(
        model="models/facts/fact_content_revenue.sql", provider_platform_nm="vendor5"
    ),
    build_dbt_run(
        model="models/facts/map_title_match.sql", provider_platform_nm="vendor3"
    ),
    build_dbt_run(
        model="models/facts/map_title_match.sql", provider_platform_nm="vendor6"
    ),
    build_dbt_run(
        model="models/facts/map_title_match.sql", provider_platform_nm="vendor5"
    ),
]


def DbtFlow(ENVIRONMENT="dev", **kwargs):
    with Flow(
        f"DBT Flow",
        ENVIRONMENT=ENVIRONMENT,
        **kwargs,
    ) as flow:

        conf = Config.at_runtime(**flow.env)

        dbt = CLIENTNAMEDbtShellTaskSet()
        dbt(
            command=dbt_commands,
            conf=conf,
            dbt_kwargs={
                "database": ENVIRONMENT,
                "schema": conf["snowflake_metrics_schema"],
            },
        )
    return flow


if __name__ == "__main__":
    flow = DbtFlow()
    flow.run(run_on_schedule=False)
