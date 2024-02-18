import boto3
from io import BytesIO
import logging

s3_client = boto3.client("s3")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def process_bucketname(bucket):
    if bucket.startswith(("s3://", "s3a://", "s3n://")):
        bucket = bucket.split("//", 1)[-1]
    return bucket.strip("/")


def process_key(key):
    return key.lstrip("/")


def move_file(source_bucket, source_key, target_bucket, target_key):
    logger.info(f"File source: s3://{source_bucket}/{source_key}")
    logger.info(f"File target: s3://{target_bucket}/{target_key}")
    s3_client.copy(
        {"Bucket": source_bucket, "Key": source_key}, target_bucket, target_key
    )
    logger.info("File copy successful.")
    return f"s3://{target_bucket}/{target_key}"


def lambda_handler(event, context):
    source_key = event["source_key"]
    source_bucket = event["source_bucket"]
    target_key = event["target_key"]
    target_bucket = event["target_bucket"]
    return move_file(
        source_bucket=process_bucketname(source_bucket),
        source_key=process_key(source_key),
        target_bucket=process_bucketname(target_bucket),
        target_key=process_key(target_key),
    )
