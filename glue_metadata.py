from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import boto3
import sys

# ----------------------------------------------------
# Initialize
# ----------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = sys.argv
job_date = "2025-11-14"   # normally from args

raw_input_path = "s3://your-bucket/raw/financial_transactions/"
curated_output_path = "s3://your-bucket/curated/financial_transactions/"
business_layer_path = "s3://your-bucket/business/financial_transactions/"
rejects_path = "s3://your-bucket/rejects/financial_transactions/"

metadata_json_path = "s3://your-bucket/metadata/financial_transactions_meta.json"

# ----------------------------------------------------
# Load Metadata (silent)
# ----------------------------------------------------
metadata_str = spark.read.text(metadata_json_path).collect()[0][0]
metadata = json.loads(metadata_str)
mapping_fields = metadata["fields"]

# ----------------------------------------------------
# STEP 1: Read RAW parquet
# ----------------------------------------------------
raw_df = spark.read.parquet(raw_input_path)

# ----------------------------------------------------
# STEP 2: SCHEMA BUILDER — clean technical metadata
# ----------------------------------------------------
clean_schema = {"fields": []}

for field in raw_df.schema.fields:
    clean_schema["fields"].append({
        "name": field.name,
        "type": field.dataType.simpleString(),
        "nullable": field.nullable,
        "source": "raw_layer",
        "target": "curated_layer",
        "description": f"Column {field.name} description"
    })

# Save schema JSON to S3
updated_schema_json = json.dumps(clean_schema, indent=4)
s3 = boto3.client("s3")
bucket = "your-bucket"
key = "metadata/clean_schema.json"
s3.put_object(Bucket=bucket, Key=key, Body=updated_schema_json.encode("utf-8"))

# ----------------------------------------------------
# STEP 3: METADATA MAPPING (rename, cast, add missing)
# ----------------------------------------------------
df = raw_df

for f in mapping_fields:
    source = f.get("source_field")
    target = f.get("target_field")
    target_type = f.get("target_type")
    change_type = f.get("change_type")

    # field renamed
    if change_type == "renamed":
        df = df.withColumnRenamed(source, target)

    # type modifications
    if change_type == "type_modified":
        df = df.withColumn(target, col(target).cast(target_type))

    # added field
    if change_type == "added":
        df = df.withColumn(target, lit(None).cast(target_type))

# ----------------------------------------------------
# STEP 4: VALIDATION + REJECTS
# ----------------------------------------------------
valid_df = df
reject_df = spark.createDataFrame([], df.schema.add("error_reason", StringType()))

for f in mapping_fields:
    target = f["target_field"]

    # Min/max constraint
    if "constraints" in f:
        min_val = f["constraints"].get("min")
        max_val = f["constraints"].get("max")

        # amount must be >= min
        rej = valid_df.filter((col(target) < min_val) | (col(target) > max_val)) \
            .withColumn("error_reason", lit(f"{target} outside valid range"))

        reject_df = reject_df.unionByName(rej)
        valid_df = valid_df.filter((col(target) >= min_val) & (col(target) <= max_val))

    # allowed values
    if "allowed_values" in f:
        allowed = f["allowed_values"]
        rej = valid_df.filter(~col(target).isin(allowed)) \
            .withColumn("error_reason", lit(f"{target} invalid allowed value"))

        reject_df = reject_df.unionByName(rej)
        valid_df = valid_df.filter(col(target).isin(allowed))

# ----------------------------------------------------
# Save rejected rows
# ----------------------------------------------------
reject_df.write.mode("append").parquet(f"{rejects_path}/dt={job_date}")

# ----------------------------------------------------
# STEP 5: THRESHOLD LOGIC (diagram)
# ----------------------------------------------------
total_rows = raw_df.count()
valid_rows = valid_df.count()

threshold_ratio = valid_rows / total_rows if total_rows > 0 else 0

# THRESHOLD DECISION NODE
if threshold_ratio < 0.95:
    raise Exception(f"Threshold validation failed: only {threshold_ratio*100:.2f}% rows valid")

# ----------------------------------------------------
# STEP 6: RAW → CURATED TRANSITION
# ----------------------------------------------------
curated_df = valid_df.withColumn("ingest_ts", current_timestamp())

curated_df.write.mode("overwrite").parquet(f"{curated_output_path}/dt={job_date}")

# ----------------------------------------------------
# STEP 7: BUSINESS LAYER MERGE (INCREMENTAL)
# ----------------------------------------------------
try:
    existing = spark.read.parquet(business_layer_path)
except:
    existing = spark.createDataFrame([], curated_df.schema)  # empty

# Perform SCD2-style merge
from pyspark.sql.window import Window

key_field = "transaction_id"
new = curated_df.withColumn("effective_start_date", lit(job_date)) \
                .withColumn("effective_end_date", lit("9999-12-31")) \
                .withColumn("is_current", lit(True))

# Rows that change
join_cond = [existing[key_field] == new[key_field]]
updates = existing.alias("e").join(new.alias("n"), join_cond, "inner") \
    .filter("e.amount != n.amount OR e.transaction_type != n.transaction_type")

# Close histories
closed_history = updates.select("e.*") \
    .withColumn("effective_end_date", lit(job_date)) \
    .withColumn("is_current", lit(False))

# New inserts
new_inserts = new

# unchanged rows
unchanged = existing.join(new, join_cond, "left_anti")

final_business = unchanged.unionByName(closed_history).unionByName(new_inserts)

# ----------------------------------------------------
# Save final SCD2 business layer
# ----------------------------------------------------
final_business.write.mode("overwrite").parquet(business_layer_path)
