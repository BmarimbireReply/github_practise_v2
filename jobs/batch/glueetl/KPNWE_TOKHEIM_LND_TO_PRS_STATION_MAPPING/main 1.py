# Ticket (NWEREP-2793)
# Imports
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql.functions import col, desc, when, current_timestamp
from pyspark.sql.window import Window
import os
import sys

# arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "ACCOUNT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
env = args["ENV"]
account = args["ACCOUNT"]

# Read DataCatalog landing table
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="landing_kpnwe_tokheim",
    table_name="table-station_mapping",
    transformation_ctx="land_station_mapping_dyf",
)

if dyf.count() == 0:
    job.commit()
    os._exit(0)
    
df = dyf.toDF()

to_drop = [c for c in ["ingest_year", "ingest_month", "ingest_day"] if c in df.columns]
if to_drop:
    df = df.drop(*to_drop)

# Final Select with desired columns
df = df.select("tokheim_id_company_station", "sap_station_number", "ingest_timestamp")

final_dyf = DynamicFrame.fromDF(df, glueContext, "final_station_mapping_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": f"s3://aws-glue-assets-{account}-eu-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "kpnwe_tokheim_stg.station_mapping",
        "connectionName": "rdh-kpnwe-connector",
        "preactions": "TRUNCATE TABLE kpnwe_tokheim_stg.station_mapping;",
    },
    transformation_ctx="station_mapping_dyf",
)

job.commit()

