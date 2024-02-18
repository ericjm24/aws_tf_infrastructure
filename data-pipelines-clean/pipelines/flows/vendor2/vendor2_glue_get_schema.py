import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "data_source_bucket",
        "data_source_prefix",
        "data_target_bucket",
        "data_target_prefix",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)
from pyspark.sql import functions as fx

# open
source_bucket = args["data_source_bucket"].rstrip("/")
if not source_bucket.startswith(("s3://", "s3a://", "s3n://")):
    source_bucket = "s3://" + source_bucket
source_prefix = args["data_source_prefix"].strip("/")
source_dir = f"{source_bucket}/{source_prefix}/*.gz"

working_data = spark.read.option("mode", "PERMISSIVE").json(source_dir)

working_data.printSchema()

# working_data.write.mode("append").parquet(
#     f"s3://{args['data_target_bucket']}/{args['data_target_prefix']}"
# )
