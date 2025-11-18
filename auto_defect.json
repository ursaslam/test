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
# === USER CONFIGURABLE INPUTS (per dataset) ===
# ------------------------------------------------------------
dataset_name = "financial_transactions"

# Hardcoded prefix
src_prefix = "s3://your-landing-bucket/data/financial_transactions/"

# Output JSON path
output_json_path = f"s3://your-test-bucket/metadata/{dataset_name}.json"


# ------------------------------------------------------------
# AUTO-DETECT DATASET FORMAT (CSV vs PARQUET)
# ------------------------------------------------------------
def detect_dataset_format(src_prefix):
    s3 = boto3.client("s3")
    bucket = src_prefix.split("/")[2]
    prefix = "/".join(src_prefix.split("/")[3:])

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in resp:
        raise Exception(f"No objects found under prefix: {src_prefix}")

    formats_found = set()

    for obj in resp["Contents"]:
        key = obj["Key"].lower()

        if key.endswith(".parquet"):
            formats_found.add("parquet")
        elif key.endswith(".csv") or key.endswith(".csv.gz") or key.endswith(".txt"):
            formats_found.add("csv")

    if "parquet" in formats_found:
        return "parquet"

    if "csv" in formats_found:
        return "csv"

    raise Exception(f"Cannot auto-detect format under: {src_prefix}")


dataset_format = detect_dataset_format(src_prefix)
print("✔ Detected dataset format:", dataset_format)


# ------------------------------------------------------------
# READ SAMPLE FILE BASED ON DETECTED FORMAT
# ------------------------------------------------------------
if dataset_format == "csv":
    df = spark.read.option("header", True).csv(src_prefix)
else:
    df = spark.read.parquet(src_prefix)

schema = df.schema


# ------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------
def dtype_string(dt):
    if isinstance(dt, DecimalType):
        return f"decimal({dt.precision})"
    return str(dt)

def extract_precision(dt):
    return dt.precision if isinstance(dt, DecimalType) else ""


# ------------------------------------------------------------
# Build Columns Section
# ------------------------------------------------------------
cols_meta = []

for f in schema.fields:

    cols_meta.append({
        "src_nm": f.name,
        "tgt_nm": f.name,
        "src_dtype": dtype_string(f.dataType),
        "tgt_dtype": dtype_string(f.dataType),
        "precision": extract_precision(f.dataType),
        "src_fmt": "",
        "tgt_fmt": "",
        "nullable": f.nullable,
        "required_src": True,
        "regex": "",
        "len": "",
        "chg_types": [],
        "comment": f"Business definition for {f.name}"
    })


# ------------------------------------------------------------
# Build Final Metadata JSON
# ------------------------------------------------------------
metadata_json = {
    "ds": {
        "nm": dataset_name,
        "ver": "1.0",
        "desc": f"Metadata contract for {dataset_name} dataset",
        "src_sys": "source_system",
        "tgt_sys": "GlueCatalog",
        "format": dataset_format,
        "src_prefix": src_prefix,
        "id_cols": ["txn_id"],
        "upd_cols": ["ingest_ts"],
        "dq_rules": {
            "check_schema": True,
            "check_row_count": True,
            "allow_extra_columns": True,
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

s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=json.dumps(metadata_json, indent=4)
)

print("✔ Metadata JSON created successfully:", output_json_path)
