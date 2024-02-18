from ._aws_secrets import AwsSecretManager, AwsSecret
from .._base_config import SwitchedData

from importlib import import_module
import os

for f in os.listdir(os.path.dirname(__file__)):
    if f.startswith("_") or not f.endswith(".py"):
        continue
    mod = import_module("." + f.rsplit(".", 1)[0], package=__name__)
    for k, v in mod.__dict__.items():
        if (
            not k.startswith("_")
            and not hasattr(AwsSecretManager, k)
            and type(v) in (str, SwitchedData)
        ):
            setattr(AwsSecretManager, k, AwsSecret(v))
