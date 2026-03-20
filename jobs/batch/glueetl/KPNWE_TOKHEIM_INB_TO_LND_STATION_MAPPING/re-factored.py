# -----------------------------------------------------------------------------
# Imports
# -----------------------------------------------------------------------------

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, desc, when, current_timestamp, regexp_extract, lpad, row_number
from pyspark.sql.window import Window

import os
import sys


# -----------------------------------------------------------------------------
# 1) Initialize Glue Job
# -----------------------------------------------------------------------------

def initialise_job() -> tuple:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV"])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    env = args["ENV"]

    return args, sc, glueContext, spark, job, env


# -----------------------------------------------------------------------------
# 2) Define S3 Paths
# -----------------------------------------------------------------------------

def get_s3_paths(env: str) -> tuple:
    """
    Define S3 bucket names and key prefixes for source and destination.
    Args:
        env (str): The environment in which the job is running (e.g., dev, prod).
    Returns:
        tuple: A tuple containing source bucket name, source key prefix, destination bucket name, and destination key prefix.
    """
    source_bucket_name = f"s3-kpi-dataplatform-{env}-inbound"
    source_key_prefix = "kpnwe/database/tokheim/station_mapping"

    destination_bucket_name = f"s3-kpi-dataplatform-{env}-landing-kpnwe-retail"
    destination_key_prefix = "database/tokheim/station_mapping"

    return (
        source_bucket_name,
        source_key_prefix,
        destination_bucket_name,
        destination_key_prefix,
    )


# -----------------------------------------------------------------------------
# 3) Read CSV Data from S3
# -----------------------------------------------------------------------------

def read_station_mapping(glueContext: GlueContext, source_bucket_name: str, source_key_prefix: str) -> DynamicFrame:
    """ Read station mapping data from S3 bucket using GlueContext and return as a DynamicFrame.
    Args:
        glueContext (GlueContext): The GlueContext to use for reading data.
        source_bucket_name (str): The name of the S3 bucket where the source data is located.
        source_key_prefix (str): The key prefix in the S3 bucket where the source data is located.
    Returns:
        DynamicFrame: A DynamicFrame containing the station mapping data read from S3.
    """
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

    return dyf


# -----------------------------------------------------------------------------
# 4) Stop Job If No Data Exists
# -----------------------------------------------------------------------------

def stop_if_empty(dyf: DynamicFrame, job: Job) -> None:
    """  
    Check if the DynamicFrame is empty and stop the Glue job if it is.
    Args: dyf (DynamicFrame): The DynamicFrame to check for data.
        job (Job): The Glue job instance to commit before exiting if the DynamicFrame is empty.
    """
    if dyf.count() == 0:
        job.commit()
        os._exit(0)


# -----------------------------------------------------------------------------
# 5) Transform Data
# -----------------------------------------------------------------------------

def transform_data(dyf: DynamicFrame) -> DataFrame:
    """
    Perform necessary transformations on the input DynamicFrame and return a Spark DataFrame.
    Args:
        dyf (DynamicFrame): Input DynamicFrame read from S3.
    Returns:    DataFrame: Transformed Spark DataFrame ready for deduplication and writing to S3.
    """
    df = (
        dyf.toDF()
        .withColumn("ingest_year", regexp_extract(col("s3_key"), "year=(\\d{4})", 1))
        .withColumn("ingest_month", lpad(regexp_extract(col("s3_key"), "month=(\\d{1,2})", 1), 2, "0"))
        .withColumn("ingest_day", lpad(regexp_extract(col("s3_key"), "day=(\\d{1,2})", 1), 2, "0"))
        .withColumn("ingest_timestamp", current_timestamp())
        .drop("s3_key")
    )

    # Convert empty strings to NULL
    df = df.select(
        *[
            when(col(c) == "", None).otherwise(col(c)).alias(c)
            for c in df.columns
        ]
    )

    return df


# -----------------------------------------------------------------------------
# 6) Deduplicate Latest Records
# -----------------------------------------------------------------------------

def deduplicate_latest(df: DataFrame) -> DataFrame:

    window_spec = Window.partitionBy(
        "tokheim_id_company_station",
        "sap_station_number"
    ).orderBy(
        desc("ingest_year"),
        desc("ingest_month"),
        desc("ingest_day")
    )

    df = (
        df.withColumn("row_number", row_number().over(window_spec))
        .filter(col("row_number") == 1)
        .drop("row_number")
    )

    return df


# -----------------------------------------------------------------------------
# 7) Write Data to S3 Landing Zone
# -----------------------------------------------------------------------------

def write_to_s3(glueContext: GlueContext, df: DataFrame, destination_bucket_name: str, destination_key_prefix: str) -> None:
    """ Write the transformed and deduplicated DataFrame to S3 landing zone in Parquet format, partitioned by ingest year, month, and day.
    Args:        glueContext (GlueContext): The GlueContext to use for writing data.
        df (DataFrame): The transformed and deduplicated Spark DataFrame to write to S3.
        destination_bucket_name (str): The name of the S3 bucket where the data should be written.
        destination_key_prefix (str): The key prefix in the S3 bucket where the data should be written.
    """
    final_dyf = DynamicFrame.fromDF(df, glueContext, "prices_new_dyf")

    glueContext.write_dynamic_frame.from_options(
        frame=final_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"s3://{destination_bucket_name}/{destination_key_prefix}/",
            "partitionKeys": ["ingest_year", "ingest_month", "ingest_day"],
        },
        format_options={"compression": "snappy"},
        transformation_ctx="land_prices_node",
    )


# -----------------------------------------------------------------------------
# 8) Main Pipeline Execution
# -----------------------------------------------------------------------------

def main():
    """ 
    Main function to orchestrate the Glue ETL job execution.
    It initialises the job, defines S3 paths, reads data, checks for empty data, 
    transforms data, deduplicates records, and writes the final output to S3.
    """

    (
        args,
        sc,
        glueContext,
        spark,
        job,
        env
    ) = initialise_job()

    (
        source_bucket_name,
        source_key_prefix,
        destination_bucket_name,
        destination_key_prefix,
    ) = get_s3_paths(env)

    dyf = read_station_mapping(glueContext, source_bucket_name, source_key_prefix)

    stop_if_empty(dyf, job)

    df = transform_data(dyf)

    df = deduplicate_latest(df)

    write_to_s3(glueContext, df, destination_bucket_name, destination_key_prefix)

    job.commit()


# -----------------------------------------------------------------------------
# 9) Script Entry Point
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    main()