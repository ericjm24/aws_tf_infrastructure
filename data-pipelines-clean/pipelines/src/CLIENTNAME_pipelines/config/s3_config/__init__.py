from ._s3_config import S3Config, S3File, EncryptedS3File
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
            and not hasattr(S3Config, k)
            and type(v) in [str, S3File, EncryptedS3File]
        ):
            setattr(S3Config, k, v)
