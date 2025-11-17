import json
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import *


# ------------------------------------------------------------
# INIT Spark + Glue
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# ------------------------------------------------------------
# FIXED INPUT/OUTPUT PATHS (replace with yours)
# ------------------------------------------------------------
input_path = "s3://your-bucket/input/data.csv"
output_json_path = "s3://your-bucket/metadata/financial_transactions.json"
dataset_name = "financial_transactions"


# ------------------------------------------------------------
# READ INPUT FILE
# ------------------------------------------------------------
if input_path.endswith(".csv"):
    df = spark.read.option("header", True).csv(input_path)
else:
    df = spark.read.parquet(input_path)

schema = df.schema


# ------------------------------------------------------------
# Extract datatype into string
# ------------------------------------------------------------
def dtype_string(dt):
    if isinstance(dt, DecimalType):
        return f"decimal({dt.precision},{dt.scale})"
    return str(dt)


# ------------------------------------------------------------
# Build column metadata
# ------------------------------------------------------------
cols_meta = []

for f in schema.fields:
    cols_meta.append({
        "src_nm": f.name,
        "tgt_nm": f.name,

        "src_dtype": dtype_string(f.dataType),
        "tgt_dtype": dtype_string(f.dataType),

        "src_fmt": None,
        "tgt_fmt": None,

        "nullable": f.nullable,
        "required_src": True,

        "regex": None,
        "len": None,

        "chg_types": [],

        "comment": f"Business definition for {f.name}"
    })


# ------------------------------------------------------------
# Build full metadata JSON
# ------------------------------------------------------------
metadata_json = {
    "ds": {
        "nm": dataset_name,
        "ver": "1.0",
        "desc": f"Auto-generated metadata for {dataset_name}",
        "src_sys": "source_system",
        "tgt_sys": "GlueCatalog",
        "format": "csv|parquet",

        "dq_rules": {
            "check_schema": True,
            "check_row_count": True,
            "allow_extra_columns": False,
            "allow_missing_columns": False
        },

        "cols": cols_meta
    }
}


# ------------------------------------------------------------
# WRITE JSON TO S3
# ------------------------------------------------------------
s3 = boto3.client("s3")
bucket = output_json_path.split("/")[2]
key = "/".join(output_json_path.split("/")[3:])

s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(metadata_json, indent=4))

print("✔ Metadata JSON created:", output_json_path)
