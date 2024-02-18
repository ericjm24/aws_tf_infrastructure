from prefect import Flow
from ..utils.run_configs import standard_run_config
from ..config import Config


class CLIENTNAMEFlow(Flow):
    def __init__(self, name, ENVIRONMENT="dev", run_config=None, **kwargs):
        self.env = {k: kwargs.pop(k) for k in list(kwargs.keys()) if k == k.upper()}
        self.env.update({"ENVIRONMENT": ENVIRONMENT})
        if not run_config:
            run_config = standard_run_config(Config(**self.env))
        super().__init__(name, run_config=run_config, **kwargs)
