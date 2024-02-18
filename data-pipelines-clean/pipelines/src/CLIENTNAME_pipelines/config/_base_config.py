import prefect
from prefect import task
from collections import UserDict

__DEFAULTS__ = {
    "ENVIRONMENT": "dev",
    "HOST": "local",
}


class SwitchedData(dict):
    def __init__(self, switch, default=None, **kwargs):
        if not switch:
            raise Exception("SwitchedData type needs a key to switch on")
        self.switch = switch
        self.default = default
        self.update(**kwargs)

    def get_value(self, env_dict):
        switch_value = env_dict.get(self.switch, self.default)
        if switch_value not in self.keys():
            if not self.default:
                raise KeyError(
                    f"Provided environment {env_dict} doesn't match any known configuration, and no default behavior was provided."
                )
            return self.default
        return self[switch_value]


class BaseConfig(object):
    def __init__(self, **kwargs):
        if hasattr(self, "_registered_subconfs"):
            for name, cls in self._registered_subconfs.items():
                self.__setattr__(name, cls(**kwargs))
        self._update_keys(**kwargs)

    def _update_keys(self, **kwargs):
        rk = self._reserved_keys
        for k, v in kwargs.items():
            self.__dict__.update({k: v})
            if k == k.upper() and (not k.startswith("_")):
                if k not in rk:
                    rk.append(k)
        setattr(self, "__reserved_keys", rk)
        if hasattr(self, "_registered_subconfs"):
            for name in self._registered_subconfs.keys():
                self.__getattribute__(name)._update_keys(**kwargs)

    def _fill_env(self, value):
        if type(value) is str:
            for k in self._reserved_keys:
                return value.replace("${" + k + "}", str(self.__dict__[k]))
        elif type(value) is list:
            return [self._fill_env(v) for v in value]
        elif type(value) is SwitchedData:
            return self._fill_env(value.get_value(self._env))
        elif type(value) is dict:
            return {k: self._fill_env(v) for k, v in value.items()}
        else:
            return value

    def __getattribute__(self, attr):
        val = super().__getattribute__(attr)
        if attr.startswith("_") or attr in self._reserved_keys:
            return val
        else:
            return self._fill_env(val)

    def __getitem__(self, attr):
        if not attr.startswith("_"):
            return getattr(self, attr)

    def _as_dict(self):
        return {key: getattr(self, key) for key in dir(self) if not key.startswith("_")}

    @property
    def _reserved_keys(self):
        return getattr(self, "__reserved_keys", [])

    @property
    def _env(self):
        return {k: self.__dict__[k] for k in self._reserved_keys}

    @property
    def _is_test_run(self):
        return bool(self._env.get("TEST_RUN", False))

    def get(self, attr, default=None):
        if "." in attr:
            x, y = attr.split(".", 1)
        else:
            x, y = attr, None
        if y:
            return getattr(self, x).get(y, default)
        else:
            if not hasattr(self, x):
                return self._fill_env(default)
            else:
                return self.__getattribute__(x)

    def __enter__(self):
        self.__previous_context = prefect.context.__dict__.copy()
        prefect.context.update({"CLIENTNAME_config": self})
        prefect.context.update(self._env)
        return prefect.context

    def __exit__(self, type, value, traceback):
        prefect.context.clear()
        prefect.context.update(self.__previous_context)
        del self.__previous_context

    @classmethod
    @task
    def at_runtime(cls, **kwargs):
        return cls(**kwargs)
