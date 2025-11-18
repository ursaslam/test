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
# === HARDCODED INPUTS FOR NOW ===
# ------------------------------------------------------------
dataset_name = "financial_transactions"

src_prefix = "s3://your-landing-bucket/data/financial_transactions/"

# Where to write the metadata JSON
output_json_path = f"s3://your-bucket/metadata/{dataset_name}.json"

# Force format (can use auto detect later)
dataset_format = "parquet"   # or "csv"


# ------------------------------------------------------------
# === Read source sample ===
# ------------------------------------------------------------
if dataset_format == "csv":
    df = spark.read.option("header", True).csv(src_prefix)
else:
    df = spark.read.parquet(src_prefix)

schema = df.schema


# ------------------------------------------------------------
# === Helper Functions ===
# ------------------------------------------------------------
def dtype_string(dt):
    """Return valid Spark dtype string: decimal(precision,scale) or dt.simpleString()."""
    if isinstance(dt, DecimalType):
        return f"decimal({dt.precision},{dt.scale})"
    return dt.simpleString()

def extract_precision(dt):
    """Extract precision for decimals, else empty."""
    return dt.precision if isinstance(dt, DecimalType) else ""

def extract_scale(dt):
    """Extract scale for decimals, else empty."""
    return dt.scale if isinstance(dt, DecimalType) else ""


# ------------------------------------------------------------
# === Build Column Metadata ===
# ------------------------------------------------------------
cols_meta = []

for f in schema.fields:

    cols_meta.append({
        "src_nm": f.name,
        "tgt_nm": f.name,

        # correct Spark types
        "src_dtype": dtype_string(f.dataType),
        "tgt_dtype": dtype_string(f.dataType),

        "precision": extract_precision(f.dataType),
        "scale": extract_scale(f.dataType),

        # formats (always empty, no nulls)
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
# === FULL METADATA JSON CONTRACT ===
# ------------------------------------------------------------
metadata_json = {
    "ds": {
        "nm": dataset_name,
        "ver": "1.0",
        "desc": f"Metadata contract for {dataset_name}",

        # dataset-level properties
        "src_sys": "source_system",
        "tgt_sys": "glue_catalog",
        "format": dataset_format,

        # prefix only — NOT specific file names
        "src_prefix": src_prefix,

        # business/key columns
        "id_cols": ["txn_id"],

        # update/sort columns (used for dedupe)
        "upd_cols": ["ingest_ts"],

        # dataset-level DQ rules
        "dq_rules": {
            "check_schema": True,
            "check_row_count": True,
            "allow_extra_columns": True,
            "allow_missing_columns": False
        },

        # column-level metadata
        "cols": cols_meta
    }
}


# ------------------------------------------------------------
# === WRITE METADATA JSON TO S3 ===
# ------------------------------------------------------------
s3 = boto3.client("s3")
bucket = output_json_path.split("/")[2]
key = "/".join(output_json_path.split("/")[3:])

s3.put_object(
    Bucket=bucket,
    Key=key,
    Body=json.dumps(metadata_json, indent=4)
)

print("✔ Metadata JSON generated successfully:", output_json_path)
