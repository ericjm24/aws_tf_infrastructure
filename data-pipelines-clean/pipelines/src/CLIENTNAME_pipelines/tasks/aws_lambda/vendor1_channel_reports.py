from .lambda_invoke import CLIENTNAMELambdaInvoke
from json import dumps
from base64 import b64encode


class Vendor1ReportsToS3(CLIENTNAMELambdaInvoke):
    def __init__(self, **kwargs):
        super().__init__(
            function_config_name="s3.pipelines_tf_state.vendor1_lambda_name"
        )

    def set_payload(self, channel_name, start_date, end_date, conf):
        yt_creds = conf.s3.vendor1_creds
        if channel_name not in yt_creds.keys():
            return
        payload = {
            "start_time": start_date.strftime("%Y-%m-%d"),
            "end_time": end_date.strftime("%Y-%m-%d"),
            "channel_name": channel_name,
            "credentials": b64encode(
                dumps(yt_creds[channel_name]).encode("utf-8")
            ).decode("utf-8"),
        }
        return payload
