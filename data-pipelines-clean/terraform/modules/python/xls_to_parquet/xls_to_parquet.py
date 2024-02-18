import boto3
import logging
import pandas as pd
import botocore
from base64 import b64decode
from json import loads
from io import BytesIO
import re

s3_client = boto3.client("s3")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_xls_kwargs = {}


def remove_nan(df):
    cols = df.select_dtypes(include="object").columns
    df[cols] = df[cols].where(pd.notnull(df[cols]), "")
    for col in cols:
        df[col] = df[col].str.encode("utf-8", errors="replace")
    return df.rename(columns={c: str(c) for c in df.columns})


def read_excel(body, **kwargs):
    sn = kwargs.pop("sheet_name", 0)
    if sn is None or type(sn) is list:
        sheet_regex = kwargs.pop("sheet_regex", None)
        if sheet_regex and type(sheet_regex) is str:
            sheet_regex = [sheet_regex]
        file = pd.ExcelFile(body)
        out = {}
        for s in file.sheet_names:
            if sheet_regex and not any(re.match(x, s) for x in sheet_regex):
                continue
            try:
                dat = remove_nan(file.parse(sheet_name=s, **kwargs))
            except Exception as e:
                dat = pd.DataFrame({"Error": [str(e)]})
                logging.warning(f"Failed to parse sheet name {s}")
                logging.warning(e)
            out.update({s: dat})
        if len(out) == 1:
            out = list(out.values())[0]
        return out
    else:
        kwargs.pop("sheet_regex", None)
        return remove_nan(pd.read_excel(body, sheet_name=sn, **kwargs))


class ReportMover:
    """
    1. Read file from s3
    2. Validate schema
    3. Write file to s3
    """

    def __init__(self, bucket, file_key, xls_kwargs={}):
        self.file_key = file_key
        self.bucket = bucket
        self.data = self.read_file(**xls_kwargs)

    def read_file(self, **kwargs):
        try:
            response = s3_client.get_object(Bucket=self.bucket, Key=self.file_key)
        except botocore.exceptions.ClientError as e:
            logger.error(f"Process failed: {e.response['Error']['Code']}")
            raise

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            logger.info(f"Successfully read file {self.file_key}")
            logger.info(f"Response status: {status}")
            return read_excel(BytesIO(response.get("Body").read()), **kwargs)
        else:
            logger.error(f"Response error while reading file {status}")
            raise Exception("Unacceptable status while reading file")

    def validate_report(self, schema=None):
        """
        This function validates the schema to make sure that there are no
        changes in the source format

        Args:
            df (Dataframe): Source dataframe read from s3
        """
        if schema is None:
            return True

        src_schema = set(list(self.df.columns))

        if schema.issubset(src_schema):
            logging.info("Target schema matches source. Continuing...")
            return True
        else:
            logging.error(
                "Target schema conflicts with source schema. Aborting process"
            )
            logging.error(f"Target Columns: {schema}")
            logging.error(f"Source Columns: {src_schema}")
            logging.error(f"Errors founc: {schema - src_schema}")
            return False

    def uploader(self, file_key):
        if file_key.endswith(".gz"):
            file_key = file_key.rsplit(".", 1)[0]
        if file_key.endswith(".parquet"):
            file_key = file_key.rsplit(".", 1)[0]
        if type(self.data) is pd.DataFrame:
            final_key = f"{file_key}.parquet.gz"
            self.data.to_parquet(final_key, compression="gzip")
            logging.info(
                f"File successfully moved and converted to parquet at {final_key}"
            )
            return final_key
        elif type(self.data) is dict:
            final_keys = {}
            for k, v in self.data.items():
                outkey = f"{file_key}_{k}.parquet.gz"
                v.to_parquet(outkey, compression="gzip")
                logging.info(
                    f"File successfully moved and converted to parquet at {outkey}"
                )
                final_keys.update({k: outkey})
            return final_keys


def lambda_handler(event, context):
    source_key = event["source_key"]
    source_bucket = event["source_bucket"]
    target_key = event["target_key"]
    target_bucket = event["target_bucket"]
    # Schema validation and conversion will need a bit of work
    schema = None  # event.get("schema", None)
    xls_kwargs = default_xls_kwargs
    if "xls_kwargs" in event.keys():
        xls_kwargs.update(loads(b64decode(event["xls_kwargs"].encode("utf-8"))))
    if schema:
        schema = set(schema)
    report = ReportMover(
        bucket=source_bucket, file_key=source_key, xls_kwargs=xls_kwargs
    )
    if report.validate_report(schema):
        return report.uploader(f"s3://{target_bucket}/{target_key}")
    else:
        raise Exception("Aborted the process due to change in file schema")
