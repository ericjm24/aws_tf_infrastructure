from base64 import b64decode


def process_lambda_log(encoded_logs):
    s = b64decode(encoded_logs.encode("utf-8")).decode("utf-8")
    return s.split("\n")
