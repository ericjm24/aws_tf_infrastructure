import boto3
import logging
import pandas as pd
from base64 import b64decode
from json import loads
import s3fs
import boto3

fs = s3fs.S3FileSystem()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_csv_kwargs = {"skipinitialspace": True}

file_extensions = (
    ".gz",
    ".bz2",
    ".zip",
    ".xz",
    ".zst",
    ".parquet",
    ".csv",
    ".txt",
    ".tsv",
)

MAX_FILESIZE = 31457280  # 30


def validate_schema(source_bucket, source_key, schema=None, csv_kwargs={}, **kwargs):
    if not schema:
        return True
    kwargs.update({"nrows": 10})
    with fs.open(f"s3://{source_bucket}/{source_key}", "r") as file:
        temp = pd.read_csv(file, **csv_kwargs)
    src_schema = set(list(temp.columns))
    if schema.issubset(src_schema):
        logging.info("Target schema matches source. Continuing...")
        return True
    else:
        logging.error("Target schema conflicts with source schema. Aborting process")
        logging.error(f"Target Columns: {schema}")
        logging.error(f"Source Columns: {src_schema}")
        logging.error(f"Errors found: {schema - src_schema}")
        return False


def need_iterator(source_bucket, source_key, force_iterate=False, **kwargs):
    if force_iterate:
        return True
    try:
        filesize = (
            boto3.resource("s3").Bucket(source_bucket).Object(source_key).content_length
        )
    except:
        filesize = 0
    if filesize >= MAX_FILESIZE:
        return True
    else:
        return False


def single_convert(
    source_bucket, source_key, target_bucket, target_key, schema, csv_kwargs, **kwargs
):
    with fs.open(f"s3://{source_bucket}/{source_key}", "rt") as file:
        result = pd.read_csv(file, **csv_kwargs)
    if schema:
        result = result[list(schema)]
    if target_key.endswith(".parquet"):
        target_key += ".gz"
    elif not target_key.endswith(".parquet.gz"):
        target_key += ".parquet.gz"
    outkey = f"s3://{target_bucket}/{target_key}"
    result.to_parquet(outkey, compression="gzip")
    logging.info(
        f"File successfully moved and converted to parquet. Schema is:\n{pd.io.sql.get_schema(result, 'TABLENAME')}"
    )
    return [outkey]


def multipart_convert(
    source_bucket, source_key, target_bucket, target_key, schema, csv_kwargs, **kwargs
):
    if target_key.endswith(file_extensions):
        target_key = target_key.rsplit(".", 1)[0]
    target_key = target_key.strip("/")
    with fs.open(f"s3://{source_bucket}/{source_key}", "rt") as file:
        with pd.read_csv(file, iterator=True, chunksize=30000, **csv_kwargs) as reader:
            part_number = 0
            outkeys = []
            for chunk in reader:
                if schema:
                    chunk = chunk[list(schema)]
                outkey = f"s3://{target_bucket}/{target_key}/part_{part_number:03}.parquet.gz"
                chunk.to_parquet(outkey, compression="gzip")
                outkeys.append(outkey)
                part_number += 1
    logging.info(
        f"Files successfully moved and converted to parquet as {part_number} parts."
    )
    return outkeys


def clean_bucketname(bucket):
    if bucket.startswith(("s3://", "s3a://", "s3n://")):
        bucket = bucket.split("//", 1)[-1]
    return bucket.strip("/")


def lambda_handler(event, context):
    target_bucket = clean_bucketname(event["target_bucket"])
    source_bucket = clean_bucketname(event["source_bucket"])
    source_key = event["source_key"].strip("/")
    target_key = (event.get("target_key", "") or source_key).strip("/")
    csv_kwargs = default_csv_kwargs
    schema = event.get("schema", None)
    if schema:
        schema = set(schema)
    if "csv_kwargs" in event.keys():
        csv_kwargs.update(loads(b64decode(event["csv_kwargs"].encode("utf-8"))))
    event.update(
        {
            "target_bucket": target_bucket,
            "source_bucket": source_bucket,
            "target_key": target_key,
            "source_key": source_key,
            "schema": schema,
            "csv_kwargs": csv_kwargs,
        }
    )
    if not validate_schema(**event):
        raise Exception("Aborted the process due to change in file schema")
    if need_iterator(**event):
        return multipart_convert(**event)
    else:
        return single_convert(**event)
