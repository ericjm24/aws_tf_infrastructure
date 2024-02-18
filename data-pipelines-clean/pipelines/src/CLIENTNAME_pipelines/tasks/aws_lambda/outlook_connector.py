from .lambda_invoke import CLIENTNAMELambdaInvoke
from datetime import date


class OutlookToS3Connector(CLIENTNAMELambdaInvoke):
    def __init__(self, **kwargs):
        super().__init__(
            function_config_name="s3.pipelines_tf_state.outlook_lambda_name", **kwargs
        )

    def set_payload(
        self,
        start_date=date(1970, 1, 1),
        end_date=date(9999, 12, 31),
        attachment_type=None,
        sender=None,
        subject=None,
        **kwargs
    ):

        payload = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "attachment_type": attachment_type,
            "sender": sender,
            "subject": subject,
        }
        return {k: v for k, v in payload.items() if v}
