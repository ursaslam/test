# ============================================================
# FULL GLUE PIPELINE — RAW → CURATED → BUSINESS (SCD2)
# ============================================================

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import boto3
import sys
from datetime import datetime

# ------------------------------------------------------------
# INIT
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# INPUTS (normally passed through --args)
raw_input_path       = "s3://your-bucket/raw_input/source/"
raw_output_path      = "s3://your-bucket/raw/financial_transactions/"
curated_output_path  = "s3://your-bucket/curated/financial_transactions/"
business_output_path = "s3://your-bucket/business/financial_transactions/"
rejects_output_path  = "s3://your-bucket/rejects/financial_transactions/"
metadata_path        = "s3://your-bucket/metadata/financial_transactions_meta.json"

# ============================================================
# STEP 1 — RAW COPY (dt + hr PARTITIONING)
# ============================================================

current_ts = datetime.utcnow()
load_date = current_ts.strftime("%Y-%m-%d")
load_hour = current_ts.strftime("%H")

# Read RAW source
df_raw = spark.read.parquet(raw_input_path)

# RAW Layer path
raw_partition_path = f"{raw_output_path}/dt={load_date}/hr={load_hour}"

# Overwrite only the hour partition, not the whole day
df_raw.write.mode("overwrite").parquet(raw_partition_path)

print(f"[RAW COPY] Data copied to: {raw_partition_path}")

# Use RAW data for processing
df = df_raw


# ============================================================
# STEP 2 — SCHEMA BUILDER (YOUR SCRIPT)
# ============================================================

clean_schema = {"fields": []}

for field in df.schema.fields:
    clean_schema["fields"].append({
        "name": field.name,
        "type": field.dataType.simpleString(),
        "nullable": field.nullable,
        "source": "raw_layer",
        "target": "curated_layer",
        "description": f"Column {field.name} description"
    })

# Save updated schema to S3
updated_schema_json = json.dumps(clean_schema, indent=4)
s3 = boto3.client("s3")

s3.put_object(
    Bucket="your-bucket",
    Key="metadata/clean_schema.json",
    Body=updated_schema_json.encode("utf-8")
)


# ============================================================
# STEP 3 — LOAD METADATA (Silent — No Prints)
# ============================================================

meta_raw = spark.read.text(metadata_path).collect()[0][0]
# Fix common JSON issues: single quotes -> double quotes
meta_fixed = meta_raw.replace("'", '"')

# Remove trailing commas before closing brackets/braces
meta_fixed = re.sub(r",(\s*[\]}])", r"\1", meta_fixed)

# Load JSON safely
try:
    metadata = json.loads(meta_fixed)
except json.JSONDecodeError as e:
    raise Exception(f"Failed to parse metadata JSON after auto-fix: {e}")

mapping_fields = metadata["fields"]
print(f"[METADATA] Loaded {len(mapping_fields)} field mappings successfully")
# ============================================================
# STEP 4 — METADATA-DRIVEN FIELD MAPPING
# ============================================================

for f in mapping_fields:
    source  = f["source_field"]
    target  = f["target_field"]
    ttype   = f["target_type"]
    change  = f["change_type"]

    # renamed
    if change == "renamed":
        df = df.withColumnRenamed(source, target)

    # type change
    if change == "type_modified":
        df = df.withColumn(target, col(target).cast(ttype))

    # add column
    if change == "added":
        df = df.withColumn(target, lit(None).cast(ttype))


# ============================================================
# STEP 5 — VALIDATION + REJECTS
# ============================================================

reject_schema = df.schema.add("error_reason", StringType())
reject_df = spark.createDataFrame([], reject_schema)
valid_df = df

for f in mapping_fields:
    col_name = f["target_field"]

    # min/max
    if "constraints" in f:
        min_v = f["constraints"]["min"]
        max_v = f["constraints"]["max"]

        bad = valid_df.filter((col(col_name) < min_v) | (col(col_name) > max_v)) \
                      .withColumn("error_reason", lit(f"{col_name} outside range"))

        reject_df = reject_df.unionByName(bad)

        valid_df = valid_df.filter((col(col_name) >= min_v) & (col(col_name) <= max_v))

    # allowed values
    if "allowed_values" in f:
        allowed_vals = f["allowed_values"]

        bad = valid_df.filter(~col(col_name).isin(allowed_vals)) \
                      .withColumn("error_reason", lit(f"{col_name} invalid value"))

        reject_df = reject_df.unionByName(bad)

        valid_df = valid_df.filter(col(col_name).isin(allowed_vals))


# save rejects
reject_df.write.mode("append").parquet(
    f"{rejects_output_path}/dt={load_date}/hr={load_hour}"
)


# ============================================================
# STEP 6 — THRESHOLD LOGIC (Diagram Node)
# ============================================================

total_rows = df.count()
valid_rows = valid_df.count()
ratio = valid_rows / total_rows if total_rows > 0 else 0

if ratio < 0.95:
    raise Exception(f"THRESHOLD FAILED — Only {ratio*100:.2f}% rows valid")


# ============================================================
# STEP 7 — RAW → CURATED TRANSITION
# ============================================================

curated_df = valid_df.withColumn("ingest_ts", current_timestamp())

curated_df_path = f"{curated_output_path}/dt={load_date}/hr={load_hour}"

curated_df.write.mode("overwrite").parquet(curated_df_path)

print(f"[CURATED] Written to: {curated_df_path}")


# ============================================================
# STEP 8 — BUSINESS LAYER MERGE (SCD2)
# ============================================================

key = "transaction_id"

try:
    existing_df = spark.read.parquet(business_output_path)
except:
    existing_df = spark.createDataFrame([], curated_df.schema)

new_df = curated_df \
    .withColumn("effective_start_date", lit(load_date)) \
    .withColumn("effective_end_date", lit("9999-12-31")) \
    .withColumn("is_current", lit(True))

# records that change
join_cond = [existing_df[key] == new_df[key]]
updates = existing_df.alias("e") \
    .join(new_df.alias("n"), join_cond, "inner") \
    .filter("e.amount != n.amount OR e.transaction_type != n.transaction_type")

# close history rows
closed_history = updates.select("e.*") \
    .withColumn("effective_end_date", lit(load_date)) \
    .withColumn("is_current", lit(False))

# new inserts (all)
new_inserts = new_df

# unchanged historical rows
unchanged = existing_df.join(new_df, join_cond, "left_anti")

final_df = unchanged.unionByName(closed_history).unionByName(new_inserts)

# save SCD2
final_df.write.mode("overwrite").parquet(business_output_path)

print(f"[BUSINESS LAYER] MERGE completed → {business_output_path}")
