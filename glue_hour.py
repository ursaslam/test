from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import boto3
from datetime import datetime

# ------------------------------------------------------------
# INIT
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# ------------------------------------------------------------
# INPUT / OUTPUT PATHS
# ------------------------------------------------------------
raw_input_path = "s3://bucketname/from_adw/ESM_SPEND/VGI_FIN_GL_LOB_DH/data_as_of=20251107_155119/data.parquet"
raw_output_path = "s3://your-bucket/raw/financial_transactions/"
curated_output_path = "s3://your-bucket/curated/financial_transactions/"
business_output_path = "s3://your-bucket/business/financial_transactions/"
rejects_output_path = "s3://your-bucket/rejects/financial_transactions/"
metadata_path = "s3://your-bucket/metadata/financial_transactions_meta.json"
updated_schema_path = "s3://your-bucket/metadata/clean_schema.json"

# ------------------------------------------------------------
# STEP 1 — RAW COPY WITH DYNAMIC PARTITION OVERWRITE
# ------------------------------------------------------------
current_ts = datetime.utcnow()
load_date = current_ts.strftime("%Y-%m-%d")
load_hour = current_ts.strftime("%H")

df_raw = spark.read.parquet(raw_input_path)

# Write RAW data with partitioning (dt + hr) dynamically
df_raw.write.mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .parquet(raw_output_path)

print(f"[RAW COPY] Data copied to: {raw_output_path}/dt={load_date}/hr={load_hour}")

df = df_raw  # use for downstream processing

# ------------------------------------------------------------
# STEP 2 — EXTRACT SCHEMA AND SAVE AS JSON
# ------------------------------------------------------------
schema_dict = json.loads(df.schema.json())

# Add metadata fields
for field in schema_dict['fields']:
    field['description'] = f"Column {field['name']} of type {field['type']}"
    field['source'] = field['name']
    field['target'] = field['name']

# Save updated schema JSON to S3
updated_schema_json = json.dumps(schema_dict, indent=4)
s3 = boto3.client('s3')
bucket = updated_schema_path.split("/")[2]
key = "/".join(updated_schema_path.split("/")[3:])
s3.put_object(Bucket=bucket, Key=key, Body=updated_schema_json.encode('utf-8'))

print(f"[SCHEMA] Updated schema saved to: {updated_schema_path}")

from pyspark.sql.types import StructType, StructField, StringType

# ------------------------------------------------------------
# STEP 3 — LOAD METADATA (SPARK 2.3 SAFE)
# ------------------------------------------------------------

# Define the schema manually (must include at least one primitive type)
metadata_schema = StructType([
    StructField("source_field", StringType(), True),
    StructField("target_field", StringType(), True),
    StructField("change_type", StringType(), True),
    StructField("target_type", StringType(), True),
    StructField("constraints", StringType(), True),     # optional JSON as string
    StructField("allowed_values", StringType(), True)   # optional JSON as string
])

# Read metadata JSON using manual schema
meta_df = spark.read.schema(metadata_schema).json(metadata_path)

# Convert rows to dicts
mapping_fields = []
for row in meta_df.collect():
    row_dict = row.asDict()
    
    # Convert constraints and allowed_values from string to dict/list if needed
    if row_dict.get("constraints"):
        try:
            row_dict["constraints"] = json.loads(row_dict["constraints"])
        except:
            row_dict["constraints"] = None
    if row_dict.get("allowed_values"):
        try:
            row_dict["allowed_values"] = json.loads(row_dict["allowed_values"])
        except:
            row_dict["allowed_values"] = None

    mapping_fields.append(row_dict)

print(f"[METADATA] Loaded {len(mapping_fields)} field mappings safely on Spark 2.3")


# ------------------------------------------------------------
# STEP 4 — METADATA-DRIVEN FIELD MAPPING
# ------------------------------------------------------------
for f in mapping_fields:
    source = f.get("source_field")
    target = f.get("target_field")
    ttype = f.get("target_type", "string")
    change = f.get("change_type", "added")
    # renamed
    if change == "renamed":
        df = df.withColumnRenamed(source, target)
    # type change
    if change == "type_modified":
        df = df.withColumn(target, col(target).cast(ttype))
    # add column
    if change == "added":
        df = df.withColumn(target, lit(None).cast(ttype))

# ------------------------------------------------------------
# STEP 5 — VALIDATION + REJECTS
# ------------------------------------------------------------
reject_schema = df.schema.add("error_reason", StringType())
reject_df = spark.createDataFrame([], reject_schema)
valid_df = df

for f in mapping_fields:
    col_name = f["target_field"]
    # min/max constraints
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

# Save rejects
reject_df.write.mode("append") \
    .parquet(f"{rejects_output_path}/dt={load_date}/hr={load_hour}")

# ------------------------------------------------------------
# STEP 6 — THRESHOLD LOGIC
# ------------------------------------------------------------
total_rows = df.count()
valid_rows = valid_df.count()
ratio = valid_rows / total_rows if total_rows > 0 else 0
if ratio < 0.95:
    raise Exception(f"THRESHOLD FAILED — Only {ratio*100:.2f}% rows valid")

# ------------------------------------------------------------
# STEP 7 — RAW → CURATED TRANSITION
# ------------------------------------------------------------
curated_df = valid_df.withColumn("ingest_ts", current_timestamp())
curated_df.write.mode("overwrite") \
    .option("partitionOverwriteMode", "dynamic") \
    .parquet(curated_output_path)

print(f"[CURATED] Written to: {curated_output_path}/dt={load_date}/hr={load_hour}")

# ------------------------------------------------------------
# STEP 8 — BUSINESS LAYER MERGE (SCD2)
# ------------------------------------------------------------
key = "transaction_id"
try:
    existing_df = spark.read.parquet(business_output_path)
except:
    existing_df = spark.createDataFrame([], curated_df.schema)

new_df = curated_df \
    .withColumn("effective_start_date", lit(load_date)) \
    .withColumn("effective_end_date", lit("9999-12-31")) \
    .withColumn("is_current", lit(True))

# Records that change
join_cond = [existing_df[key] == new_df[key]]
updates = existing_df.alias("e") \
    .join(new_df.alias("n"), join_cond, "inner") \
    .filter("e.amount != n.amount OR e.transaction_type != n.transaction_type")

# Close history rows
closed_history = updates.select("e.*") \
    .withColumn("effective_end_date", lit(load_date)) \
    .withColumn("is_current", lit(False))

# New inserts (all)
new_inserts = new_df

# Unchanged historical rows
unchanged = existing_df.join(new_df, join_cond, "left_anti")

# Final union
final_df = unchanged.unionByName(closed_history).unionByName(new_inserts)

# Save SCD2
final_df.write.mode("overwrite").parquet(business_output_path)

print(f"[BUSINESS LAYER] MERGE completed → {business_output_path}")
