import boto3
import sys
from .._base_config import BaseConfig, SwitchedData
import json

mod = sys.modules[__name__]
secrets_client = boto3.client("secretsmanager")


class AwsSecret(object):
    def __init__(self, name):
        self.name = name


class AwsSecretManager(BaseConfig):
    def __init__(self, **kwargs):
        for item in dir(mod):
            if not item.startswith("_") and type(mod.__dict__[item]) in (
                str,
                SwitchedData,
            ):
                self.__setattr__(item, AwsSecret(mod.__dict__[item]))

    def __getattribute__(self, attr):
        val = super(BaseConfig, self).__getattribute__(attr)
        if type(val) is AwsSecret:
            return self._get_sec(val, attr)
        else:
            return val

    def _get_sec(self, sec, attr):
        attr_val = self._fill_env(sec.name)
        try:
            print(f"Attempting to retrieve SecretId={attr_val} as {attr}")
            sec = secrets_client.get_secret_value(SecretId=attr_val)
            try:
                secret_value = json.loads(sec["SecretString"])
            except json.decoder.JSONDecodeError:
                secret_value = sec["SecretString"]
        except:
            print(f"Secret <{attr_val}> could not be retrieved.")
            secret_value = None
        self.__setattr__(attr, secret_value)
        return secret_value
