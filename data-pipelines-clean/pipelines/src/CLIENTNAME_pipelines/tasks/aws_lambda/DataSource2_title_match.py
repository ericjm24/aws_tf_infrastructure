from .lambda_invoke import CLIENTNAMELambdaInvoke


class Datasource2TitleMatchTask(CLIENTNAMELambdaInvoke):
    def __init__(self, **kwargs):
        super().__init__(
            function_config_name="s3.pipelines_tf_state.title_to_DataSource2_id_lambda_name",
            **kwargs,
        )

    def set_payload(self, target_bucket=None, target_prefix=None, titles=[], **kwargs):

        payload = {
            "target_bucket": target_bucket,
            "target_prefix": target_prefix,
            "titles": titles,
        }
        return {k: v for k, v in payload.items() if v}
