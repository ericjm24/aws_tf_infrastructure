from prefect.tasks.snowflake.snowflake import SnowflakeQuery
from jinja2 import Template
import prefect
from prefect import Task
from dateutil.parser import parse


class render_copy_sql(Task):
    def __init__(self, sql_template_str=None, sql_template_file=None, **kwargs):
        if sql_template_str:
            self.sql_template_str = sql_template_str
            self.sql_template_file = None
        elif sql_template_file:
            with open(sql_template_file, "rt") as f:
                self.sql_template_str = f.read()
            self.sql_template_file = sql_template_file
        super().__init__(**kwargs)

    def run(self, sql_template_str=None, **kwargs):
        sql_template_str = sql_template_str or self.sql_template_str
        if "file_name" in kwargs:
            prefect.context.logger.info(f"Incoming file name: {kwargs['file_name']}")
            kwargs["file_name"] = kwargs["file_name"].lstrip("/").split("/", 1)[-1]
        for k, v in kwargs.items():
            if k.endswith("_date"):
                if hasattr(v, "strftime"):
                    kwargs[k] = v.strftime("%Y-%m-%d")
                elif type(v) is str:
                    dt = parse(v)
                    kwargs[k] = dt.strftime("%Y-%m-%d")
            if k.endswith("_ts"):
                if hasattr(v, "strftime"):
                    kwargs[k] = v.strftime("%Y-%m-%d %H:%M:%S")
                elif type(v) is str:
                    dt = parse(v)
                    kwargs[k] = dt.strftime("%Y-%m-%d %H:%M:%S")
        sql_template = Template(sql_template_str)
        result = sql_template.render(
            flow_run_id=prefect.context.get("flow_run_id"),
            **kwargs,
        )
        return result


class CLIENTNAMESnowflakeQuery(SnowflakeQuery):
    def __init__(self, creds_location="secrets.snowflake_creds", **kwargs):
        self.creds_location = creds_location
        super().__init__(**kwargs)

    def run(self, query=None, conf=None, **kwargs):
        if not query.strip():
            self.logger.info(f"No query string provided. Skipping.")
            return None
        creds = conf.get(self.creds_location)
        self.logger.info(f"Attempting to execute Snowflake query: {query}")
        if conf._is_test_run:
            self.logger.info("Query skipped due to this being a test run.")
            return None
        results = super().run(
            query=query,
            account=creds["account"],
            user=creds["user"],
            password=creds["password"],
            role=creds.get("role", None),
            warehouse=creds.get("warehouse", None),
            database=creds.get("database", None),
            schema=creds.get("schema", None),
            **kwargs,
        )
        self.logger.info(f"Query returned {len(results)} records.")
        return results
