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
        "partition_column",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

from pyspark.sql import functions as fx
from pyspark.sql import SparkSession, DataFrame, Window

# open
source_bucket = args["data_source_bucket"].rstrip("/")
if not source_bucket.startswith(("s3://", "s3a://", "s3n://")):
    source_bucket = "s3://" + source_bucket
source_prefix = args["data_source_prefix"].strip("/")
source_dir = f"{source_bucket}/{source_prefix}/*.gz"

working_data = spark.read.option("mode", "PERMISSIVE").json(source_dir)

if args["partition_column"] == "TIMEITEMSET":
    working_data = (
        working_data.withColumn("source_filename", fx.input_file_name())
        .withColumn(
            "report_ts",
            fx.to_timestamp(
                fx.regexp_extract(
                    fx.col("source_filename"), r".*/(\d{8}_\d{6})[^/]*", 1
                ),
                "yyyyMMdd_HHmmss",
            ),
        )
        .filter(fx.dayofweek(fx.col("report_ts")) == 4)  # Wednesday
        .withColumn(
            "report_ts", fx.date_format(fx.col("report_ts"), "yyyy-MM-dd HH:mm:ss")
        )
    )
else:
    working_data = (
        working_data.withColumn("source_filename", fx.input_file_name())
        .withColumn(
            "row_rank",
            fx.row_number().over(
                Window.partitionBy(args["partition_column"].split(",")).orderBy(
                    fx.col("source_filename").desc()
                )
            ),
        )
        .filter(fx.col("row_rank") == 1)
        .drop("row_rank")
    )

working_data.printSchema()
working_data.show(n=20)
working_data.write.mode("append").parquet(
    f"s3://{args['data_target_bucket']}/{args['data_target_prefix']}"
)
