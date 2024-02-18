from .aws_lambda.lambda_invoke import CLIENTNAMELambdaInvoke
from datetime import datetime


class FileArchiveTask(CLIENTNAMELambdaInvoke):
    def __init__(self, **kwargs):
        super().__init__(
            function_config_name="s3.pipelines_tf_state.file_mover_lambda_name",
            **kwargs,
        )

    def set_payload(self, source_bucket, source_key, conf=None, **kwargs):
        sk, fx = source_key.rsplit(".", 1)
        source_bucket = source_bucket.rstrip("/")
        if source_bucket.startswith(("s3://", "s3a://", "s3n://")):
            source_bucket = source_bucket.split("//", 1)[-1]
        target_key = (
            f"{source_bucket}/{sk}_{datetime.now().strftime('%y%m%d_%H%M%S')}.{fx}"
        )
        if conf._is_test_run:
            target_key = "temp/" + target_key
        self.target_key = target_key
        payload = {
            "source_bucket": source_bucket,
            "source_key": source_key,
            "target_bucket": conf.archive_bucket,
            "target_key": target_key,
        }
        return payload

    def run(self, conf=None, **kwargs):
        if conf._is_test_run:
            return None
        return super().run(conf=conf, **kwargs)
