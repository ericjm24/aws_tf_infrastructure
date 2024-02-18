from CLIENTNAME_pipelines.tasks.aws_lambda.lambda_invoke import CLIENTNAMELambdaInvoke
from json import dumps
from base64 import b64encode


class CsvToParquet(CLIENTNAMELambdaInvoke):
    def __init__(self, **kwargs):
        super().__init__(
            function_config_name="s3.pipelines_tf_state.csv_to_parquet_lambda_name",
            **kwargs
        )

    def set_payload(
        self,
        source_key,
        target_key=None,
        schema=None,
        conf=None,
        csv_kwargs={},
        **kwargs
    ):
        if not target_key:
            if source_key.endswith((".csv", ".tsv", ".txt")):
                target_key = source_key.rsplit(".", 1)[0]
            else:
                target_key = source_key
        if conf._is_test_run:
            target_key = "temp/" + target_key.split("/", 1)[-1]
        self.target_key = target_key
        payload = {
            "source_bucket": conf.landing_bucket,
            "source_key": source_key,
            "target_bucket": conf.staging_bucket,
            "target_key": target_key,
            "csv_kwargs": b64encode(dumps(csv_kwargs).encode("utf-8")).decode("utf-8"),
        }
        if schema:
            payload({"schema": schema})
        return payload
