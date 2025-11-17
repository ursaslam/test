import sys
import json
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import *


# ------------------------------------------------------------
# INIT — Spark & Glue
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# ------------------------------------------------------------
# INPUT ARGUMENTS
# ------------------------------------------------------------
args = sys.argv

input_path   = [x for x in args if x.startswith("--input_path=")][0].split("=")[1]
dataset_name = [x for x in args if x.startswith("--dataset_name=")][0].split("=")[1]
output_json  = [x for x in args if x.startswith("--output_json=")][0].split("=")[1]


# ------------------------------------------------------------
# READ INPUT FILE FROM S3
# ------------------------------------------------------------
if input_path.endswith(".csv"):
    df = spark.read.option("header", True).csv(input_path)
else:
    df = spark.read.parquet(input_path)

schema = df.schema


# ------------------------------------------------------------
# EXTRACT PRECISION ONLY (NO SCALE)
# ------------------------------------------------------------
def extract_precision(datatype):
    if isinstance(datatype, DecimalType):
        return datatype.precision
    return None


# ------------------------------------------------------------
# BUILD METADATA FIELDS
# ------------------------------------------------------------
fields_metadata = []

for field in schema.fields:
    precision = extract_precision(field.dataType)

    field_entry = {
        "name": field.name,
        "nullable": field.nullable,

        # Source structure
        "source_name": field.name,
        "source_data_type": str(field.dataType),
        "source_format": None,

        # Target structure
        "target_name": field.name,
        "target_data_type": str(field.dataType),
        "target_format": None,

        "precision": precision,

        # DQ RULE DEFINITIONS ONLY (NO RESULTS)
        "validation_regex": None,

        # For documentation purposes
        "row_level_comments": f"Business description for {field.name}",

        # Change tracking
        "change_type": "none",
        "update_dt": None
    }

    fields_metadata.append(field_entry)


# ------------------------------------------------------------
# FINAL METADATA JSON OBJECT
# ------------------------------------------------------------
metadata_json = {
    "dataset": {
        "name": dataset_name,
        "version": "v1.0",
        "description": f"Auto-generated metadata JSON for dataset {dataset_name}",
        "owner": "data-eng",
        "ingestion_frequency": "daily",
        "id_columns": [],
        "update_id": [],
        "fields": fields_metadata
    }
}


# ------------------------------------------------------------
# WRITE METADATA JSON TO S3
# ------------------------------------------------------------
s3 = boto3.client("s3")

bucket = output_json.split("/")[2]
key = "/".join(output_json.split("/")[3:])

s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=json.dumps(metadata_json, indent=4)
)

print(f"Metadata JSON successfully created and uploaded to: {output_json}")
