# Ticket (NWEREP-2793)
# Imports
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql.functions import col, desc, when, current_timestamp, regexp_extract, lpad, row_number
from pyspark.sql.window import Window
import os
import sys

# arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
env = args["ENV"]

# S3 Bucket and Key Prefix Definitions
source_bucket_name = f"s3-kpi-dataplatform-{env}-inbound"
source_key_prefix = f"kpnwe/database/tokheim/station_mapping"
destination_bucket_name = f"s3-kpi-dataplatform-{env}-landing-kpnwe-retail"
destination_key_prefix = "database/tokheim/station_mapping"

# Reading Inbound Data from Shared Services S3 Bucket
dyf = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ";",
        "optimizePerformance": False,
        "attachFilename": "s3_key",
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            f"s3://{source_bucket_name}/{source_key_prefix}/"
        ],
        "recurse": True,
    },
    transformation_ctx="station_mapping_dyf",
)

# If records dont exist end the job run
if dyf.count() == 0:
    job.commit()
    os._exit(0)
    
df = (
    dyf.toDF()
    .withColumn("ingest_year", regexp_extract(col("s3_key"), "year=(\\d{4})", 1))
    .withColumn("ingest_month", lpad(regexp_extract(col("s3_key"), "month=(\\d{1,2})", 1), 2, "0"))
    .withColumn("ingest_day", lpad(regexp_extract(col("s3_key"), "day=(\\d{1,2})", 1), 2, "0"))
    .withColumn("ingest_timestamp", current_timestamp())
    .drop("s3_key")
)

df = df.select(
    *[
        when(col(c) == "", None).otherwise(col(c)).alias(c)
        for c in df.columns
    ]
)

window_spec = Window.partitionBy("tokheim_id_company_station", "sap_station_number").orderBy(
    desc("ingest_year"), desc("ingest_month"), desc("ingest_day")
)

# Adding row_number and filter the latest rows
df = (
    df.withColumn("row_number", row_number().over(window_spec))
    .filter(col("row_number") == 1)
    .drop("row_number")
)

final_dyf = DynamicFrame.fromDF(df, glueContext, "prices_new_dyf")

# Write to S3 landing zone as Parquet
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": f"s3://{destination_bucket_name}/{destination_key_prefix}/",
        "partitionKeys": ["ingest_year", "ingest_month", "ingest_day"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="land_prices_node"
)

job.commit()

