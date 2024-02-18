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

cdn_log_schema = StructType(
    [
        StructField("date", StringType()),
        StructField("time", StringType()),
        StructField("cs_method", StringType()),
        StructField("c_ip", StringType()),
        StructField("cs_version", StringType()),
        StructField("cd_referrer", StringType()),
        StructField("cs_user_agent", StringType()),
        StructField("filesize", IntegerType()),
        StructField("cs_bytes", IntegerType()),
        StructField("sc_bytes", IntegerType()),
        StructField("s_ip", StringType()),
        StructField("time_taken", DoubleType()),
        StructField("sc_status", StringType()),
        StructField("cs_uri_query", StringType()),
        StructField("cs_uri_stem", StringType()),
        StructField("x_byte_range", StringType()),
        StructField("comment", StringType()),
        StructField("sc_x_hw", StringType()),
    ]
)

# open
source_bucket = args["data_source_bucket"].rstrip("/")
if not source_bucket.startswith(("s3://", "s3a://", "s3n://")):
    source_bucket = "s3://" + source_bucket
source_prefix = args["data_source_prefix"].strip("/")
source_dir = f"{source_bucket}/{source_prefix}/*.log.gz"

working_data = spark.read.option("mode", "PERMISSIVE").csv(
    source_dir, schema=cdn_log_schema, sep="\t", comment="#", header=False
)

raw_data = working_data.select(
    "date",
    "time",
    "c_ip",
    "cs_user_agent",
    "sc_status",
    fx.expr("substring(cs_uri_stem, -2)").alias("extension"),
    fx.split(fx.col("cs_uri_stem"), "/").alias("cs_uri_split"),
).where(
    (fx.expr("substring(cs_uri_stem, -2)") == "ts") & (fx.col("sc_status") == "200")
)

real_data = raw_data.select(
    "date",
    "time",
    "c_ip",
    "cs_user_agent",
    "sc_status",
    "extension",
    fx.element_at(fx.col("cs_uri_split"), 7).alias("file_id"),
    fx.element_at(fx.col("cs_uri_split"), 9).alias("file_id_and_content_ts_w_ext"),
    fx.regexp_replace(
        fx.regexp_replace(fx.element_at(fx.col("cs_uri_split"), 9), ".ts", ""), "s", ""
    ).alias("file_id_and_content_ts"),
).select(
    "date",
    "time",
    "c_ip",
    "cs_user_agent",
    "file_id",
    fx.expr("substring(file_id_and_content_ts, 9)").cast("int").alias("content_ts"),
)

max_view_point = real_data.groupby("c_ip", "cs_user_agent", "file_id").agg(
    fx.max("content_ts").alias("content_ts")
)

merged = real_data.join(
    max_view_point, on=["c_ip", "cs_user_agent", "file_id", "content_ts"], how="inner"
)

sample_event_logic = (
    merged.groupby("c_ip", "cs_user_agent", "file_id", "content_ts")
    .agg(
        fx.min("date").alias("date"),
        fx.min("time").alias("time"),
    )
    .select(
        "*",
        fx.lit(f"{source_bucket}/{source_prefix}").alias("SOURCE_DIR"),
        fx.current_timestamp().alias("RUN_TS"),
    )
)

sample_event_logic.write.mode("append").parquet(
    f"s3://{args['data_target_bucket']}/{args['data_target_prefix']}"
)
