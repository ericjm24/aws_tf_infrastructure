from ._base_config import BaseConfig


def _register_subconf(cls, subconf, name=None):
    if not name:
        name = subconf.__name__
    if not hasattr(cls, "_registered_subconfs"):
        setattr(cls, "_registered_subconfs", {name: subconf})
    else:
        cls._registered_subconfs.update({name: subconf})


class Config(BaseConfig):
    def __new__(cls, SUBCONFIGS="all", **kwargs):
        conf_obj = super().__new__(cls)
        bAll = False
        if not SUBCONFIGS:
            return conf_obj
        elif SUBCONFIGS == "all" or (type(SUBCONFIGS) is list and "all" in SUBCONFIGS):
            bAll = True
        elif type(SUBCONFIGS) is str:
            SUBCONFIGS = [SUBCONFIGS.lower()]
        elif type(SUBCONFIGS) is list:
            SUBCONFIGS = [x.lower() for x in SUBCONFIGS]
        else:
            return conf_obj
        if bAll or "secrets" in SUBCONFIGS:
            from .aws_secrets import AwsSecretManager

            _register_subconf(conf_obj, AwsSecretManager, "secrets")
        if bAll or "s3" in SUBCONFIGS:
            from .s3_config import S3Config

            _register_subconf(conf_obj, S3Config, "s3")
        return conf_obj

    def __init__(self, ENVIRONMENT="dev", **kwargs):
        if "SUBCONFIGS" in kwargs.keys():
            kwargs.pop("SUBCONFIGS")
        super().__init__(ENVIRONMENT=ENVIRONMENT, **kwargs)


DEFAULT_ENV = {"ENVIRONMENT": "dev"}
