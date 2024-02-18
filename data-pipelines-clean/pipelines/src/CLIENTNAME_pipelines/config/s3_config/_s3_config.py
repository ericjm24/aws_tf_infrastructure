import boto3
import sys
import json
import os
from .._base_config import BaseConfig, SwitchedData
from importlib import import_module, invalidate_caches
from Crypto.Cipher import AES
from io import BytesIO

s3_client = boto3.client("s3")


def get_secret(secret_name):
    secretmanager = boto3.client("secretsmanager")
    secret = secretmanager.get_secret_value(SecretId=secret_name)
    try:
        out = list(json.loads(secret["SecretString"]).values())[0]
    except json.decoder.JSONDecodeError:
        out = secret["SecretString"]
    return out


class EncryptedS3File(object):
    def __init__(self, s3_path="", encrypt_key="", key_secret_name=None):
        self.s3_path = s3_path
        if key_secret_name:
            encrypt_key = get_secret(key_secret_name)
        self.encrypt_key = encrypt_key.encode("utf-8")


class S3File(object):
    def __init__(self, s3_path=""):
        self.s3_path = s3_path


def s3_path_split(path):
    if path.startswith(("s3://", "s3n://", "s3a://")):
        path = path.split("//", 1)[-1]
    bucket, key = path.split("/", 1)
    return bucket, key


def load_json(file):
    try:
        json_obj = json.load(file)
    except:
        json_obj = {}
    return json_obj


def load_tfstate(file):
    json_obj = load_json(file)
    outputs = json_obj.get("outputs", {})
    return {k: v.get("value", None) for k, v in outputs.items()}


def load_py(file, name):
    file_name = os.path.join(os.getcwd(), f"{name}.py")
    output = None
    try:
        with open(file_name, mode="w+b") as f:
            f.write(file.read())
        invalidate_caches()
        temp_mod = import_module(name)
        output = {
            item: temp_mod.__dict__[item]
            for item in dir(temp_mod)
            if not item.startswith("_")
        }
    except:
        raise
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
    return output


def decrypt_file(file, key):
    nonce = file.read(16)
    tag = file.read(16)
    ciphertext = file.read()
    cipher = AES.new(key, AES.MODE_EAX, nonce)
    data = cipher.decrypt_and_verify(ciphertext, tag)
    return data


def encrypt_file(file_name, key):
    # Included just for completeness to ensure files are encrypted correctly.
    # To actually run this, use the version from CLIENTNAME_pipelines/functions/aes_encrypt.py
    cipher = AES.new(key, AES.MODE_EAX)
    with open(file_name, mode="rb") as file:
        ciphertext, tag = cipher.encrypt_and_digest(file)
    with open(file_name + ".bin", "wb") as file_out:
        [file_out.write(x) for x in (cipher.nonce, tag, ciphertext)]


class S3Config(BaseConfig):
    def __init__(self, ENVIRONMENT="dev", load_s3=False, **kwargs):
        self._update_keys(ENVIRONMENT=ENVIRONMENT, **kwargs)
        if load_s3:
            self._load_all_s3()

    def _load_all_s3(self):
        for item in dir(self):
            if item.startswith("_") or item == "subconfigs":
                continue
            value = getattr(self, item)
            if type(value) is str and item != item.upper():
                self._load_s3_config(value, item)
            if type(value) is EncryptedS3File:
                self._load_encrypted_s3_config(value, item)

    def _load_s3_config(self, path, name):
        print(f"Attempting to load s3 file {path} as {name}")
        path = self._fill_env(path)
        bucket, key = s3_path_split(path)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        output = None
        if key.endswith(".json"):
            output = load_json(obj["Body"])
        elif key.endswith(".py"):
            output = load_py(obj["Body"], name)
        elif key.endswith(".tfstate"):
            output = load_tfstate(obj["Body"])
        if output:
            self.__dict__.update({name: output})
        return output

    def _load_encrypted_s3_config(self, s3_file, name):
        path = self._fill_env(s3_file.s3_path)
        bucket, key = s3_path_split(path)
        output = None
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key).get("Body", None)
        except:
            obj = None
        if not obj:
            return
        if key.endswith(".bin"):
            key = key.rsplit(".", 1)[0]
        with BytesIO() as f:
            f.write(decrypt_file(obj, s3_file.encrypt_key))
            f.seek(0)
            if key.endswith(".json"):
                output = load_json(f)
            elif key.endswith(".py"):
                output = load_py(f, name).get("outputs", {})
            elif key.endswith(".tfstate"):
                output = load_tfstate(f)
        if output:
            self.__dict__.update({name: output})
        return output

    def __getattribute__(self, attr):
        val = super().__getattribute__(attr)
        if type(val) is SwitchedData:
            val = self._fill_env(val)
        if type(val) is EncryptedS3File:
            out = self._load_encrypted_s3_config(val, attr)
        elif type(val) is S3File:
            out = self._load_s3_config(val.s3_path, attr)
        else:
            out = val
        return out
