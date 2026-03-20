# Ticket 2829: LND -> PRS Glue job for Liberty Vehicles table
# Imports
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys, os

# Glue job initialisation
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ENV", "ACCOUNT"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

env = args["ENV"]
account = args["ACCOUNT"]

# Reading landing vehicles data
dyf = glueContext.create_dynamic_frame.from_catalog(
    database="landing_kpnwe_ev_full",
    table_name="table-liberty_vehicles",
    additional_options={"mergeSchema": "true"},
    transformation_ctx="land_vehicles_dyf",
)

# Exit early if no data
if dyf.count() == 0:
    print('Exiting the job as the source data is empty')
    job.commit()
    os._exit(0)

df = dyf.toDF()

# Cast data types for Redshift (matches DDL)
source_columns = {
    "companyId": "string",
    "companyBrand": "string",
    "model": "string",
    "registrationNumber": "string",
    "tankCapacity": "int",
    "batteryCapacity": "int",
    "initialOdometer": "int",
    "lastOdometer": "int",
    "leasingStartDate": "bigint",
    "vehicleDate": "bigint",
    "vehicleType": "string",
    "euRegistered": "string",
    "code": "string",
    "creationDate": "timestamp",
    "lastModifiedDate": "timestamp",
    "co2": "int",
    # New fields (28/02/2026) - still source names
    "powerSupply": "string",
    "chassisNumber": "string",
    "horsePower": "string",
    "targetConsumption": "string",
    "leasingCompany": "string",
    "leasingContractRef": "string",
    "wltpRange": "int",
    "connectorType": "string",
    "maxPowerAc": "int",
    "maxPowerDc": "int",
    "ingest_timestamp": "timestamp",
    "ingest_year": "int",
    "ingest_month": "int",
    "ingest_day": "int",
}

for c, t in source_columns.items():
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast(t))

records = df.select(
    # Keys / identity
    F.concat_ws("-", F.col("companyId").cast("string"), F.col("code").cast("string")).alias("vehicle_key"),
    F.col("companyId").cast("string").alias("company_id"),
    F.col("companyBrand").cast("string").alias("company_brand"),
    F.col("model").cast("string").alias("model"),
    F.col("registrationNumber").cast("string").alias("registration_number"),
    F.col("tankCapacity").cast("int").alias("tank_capacity"),
    F.col("batteryCapacity").cast("int").alias("battery_capacity"),
    F.col("initialOdometer").cast("int").alias("initial_odometer"),
    F.col("lastOdometer").cast("int").alias("last_odometer"),
    F.to_timestamp(F.col("leasingStartDate") / 1000).alias("leasing_start_date"),
    F.to_timestamp(F.col("vehicleDate") / 1000).alias("vehicle_date"),
    F.col("vehicleType").cast("string").alias("vehicle_type"),
    F.col("euRegistered").cast("string").alias("eu_registered"),
    F.col("code").cast("string").alias("code"),
    F.col("creationDate").cast("timestamp").alias("creation_date"),
    F.col("lastModifiedDate").cast("timestamp").alias("last_modified_date"),
    F.col("co2").cast("int").alias("co2"),
    F.col("powerSupply").cast("string").alias("power_supply"),
    F.col("chassisNumber").cast("string").alias("chassis_number"),
    F.col("horsePower").cast("string").alias("horse_power"),
    F.col("targetConsumption").cast("string").alias("target_consumption"),
    F.col("leasingCompany").cast("string").alias("leasing_company"),
    F.col("leasingContractRef").cast("string").alias("leasing_contract_ref"),
    F.col("wltpRange").cast("int").alias("wltp_range"),
    F.col("connectorType").cast("string").alias("connector_type"),
    F.col("maxPowerAc").cast("int").alias("max_power_ac"),
    F.col("maxPowerDc").cast("int").alias("max_power_dc"),
    F.col("ingest_timestamp").cast("timestamp").alias("ingest_timestamp"),
    F.col("ingest_year").cast("int").alias("ingest_year"),
    F.col("ingest_month").cast("int").alias("ingest_month"),
    F.col("ingest_day").cast("int").alias("ingest_day"),
)

# Replace empty strings with NULLs (only for string columns)
string_cols = [name for name, dtype in records.dtypes if dtype == "string"]
records = records.select(
    *[
        F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) if c in string_cols else F.col(c)
        for c in records.columns
    ]
)

# Deduplication logic (latest record per primary keys)
window_spec = Window.partitionBy("vehicle_key").orderBy(
    F.desc("ingest_year"),
    F.desc("ingest_month"),
    F.desc("ingest_day"),
)

df_dedup = (
    records.withColumn("row_number", F.row_number().over(window_spec))
    .filter(F.col("row_number") == 1)
    .drop("row_number", "ingest_year", "ingest_month", "ingest_day")
)


final_dyf = DynamicFrame.fromDF(df_dedup, glueContext, "final_vehicles_dyf")
# Writing to Redshift
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": f"s3://aws-glue-assets-{account}-eu-west-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "kpnwe_ev_stg.stg_lw_vehicles",
        "connectionName": "rdh-kpnwe-connector",
        "preactions": "TRUNCATE TABLE kpnwe_ev_stg.stg_lw_vehicles;",
    },
    transformation_ctx="redshift_vehicles_write",
)

job.commit()


