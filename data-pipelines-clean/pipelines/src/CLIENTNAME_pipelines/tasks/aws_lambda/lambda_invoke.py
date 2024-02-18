from prefect.tasks.aws.lambda_function import LambdaInvoke
from ...utils.logging_utils import process_lambda_log
from json import dumps, loads
from botocore.client import Config


class Vendor3LambdaException(Exception):
    pass


class CLIENTNAMELambdaInvoke(LambdaInvoke):
    def __init__(self, function_config_name, **kwargs):
        self.function_config_name = function_config_name
        boto_config_kwargs = {"read_timeout": 900}
        boto_kwargs = kwargs.pop("boto_kwargs", {})
        boto_kwargs.update({"config": Config(**boto_config_kwargs)})
        super().__init__(
            self.function_config_name,
            log_type="Tail",
            log_stdout=True,
            boto_kwargs=boto_kwargs,
            **kwargs
        )

    def run(self, conf=None, **kwargs):
        fn = conf.get(self.function_config_name)
        self.logger.info(kwargs)
        payload = self.set_payload(conf=conf, **kwargs) or {}
        valid_keys = super().inputs()
        x = super().run(
            function_name=fn,
            payload=dumps(payload),
            **{k: v for k, v in kwargs.items() if k in valid_keys.keys()}
        )
        if "LogResult" in x.keys():
            for line in process_lambda_log(x["LogResult"]):
                self.logger.info(line)
        if "Payload" in x.keys():
            response = loads(x["Payload"].read())
        else:
            response = None
        if "FunctionError" in x.keys():
            raise Vendor3LambdaException(response)
        return response

    def set_payload(self, *args, **kwargs):
        ## Override this method to get task logic
        pass
