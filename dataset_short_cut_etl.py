import json
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, regexp_replace, isnan
from pyspark.sql.types import *


# ------------------------------------------------------------
# INIT
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# ------------------------------------------------------------
# PATHS
# ------------------------------------------------------------
input_path   = "s3://your-bucket/input/data.csv"
metadata_path = "s3://your-bucket/metadata/financial_transactions.json"
curated_path = "s3://your-bucket/output/curated/"
reject_path  = "s3://your-bucket/output/rejects/"


# ------------------------------------------------------------
# READ DATA
# ------------------------------------------------------------
df = spark.read.option("header", True).csv(input_path)


# ------------------------------------------------------------
# READ METADATA
# ------------------------------------------------------------
s3 = boto3.client("s3")
bucket = metadata_path.split("/")[2]
key = "/".join(metadata_path.split("/")[3:])
meta = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))

dq_rules = meta["ds"]["dq_rules"]
cols = meta["ds"]["cols"]


# ============================================================
# 1️⃣ SCHEMA VALIDATION (DQ RULE)
# ============================================================
src_cols = set(df.columns)
expected_cols = set([c["src_nm"] for c in cols])

missing = expected_cols - src_cols
extra = src_cols - expected_cols

schema_fail = False

if missing:
    print("❌ Missing Columns:", missing)
    if not dq_rules["allow_missing_columns"]:
        schema_fail = True

if extra:
    print("❌ Unexpected Columns:", extra)
    if not dq_rules["allow_extra_columns"]:
        schema_fail = True

if not schema_fail:
    print("✔ Schema validation passed")
else:
    print("⚠ Schema validation failed — continuing but flagged")


# ============================================================
# 2️⃣ TRANSFORM + COLUMN-LEVEL DQ
# ============================================================
valid_df = df
reject_df = None

for c in cols:

    src = c["src_nm"]
    tgt = c["tgt_nm"]
    src_dtype = c["src_dtype"]
    tgt_dtype = c["tgt_dtype"]
    regex = c["regex"]
    nullable = c["nullable"]
    required_src = c["required_src"]
    src_fmt = c["src_fmt"]
    tgt_fmt = c["tgt_fmt"]

    # Skip missing source columns (already checked above)
    if src not in valid_df.columns:
        continue

    # ---------------------------
    # RENAME
    # ---------------------------
    if src != tgt:
        valid_df = valid_df.withColumnRenamed(src, tgt)

    col_ref = tgt

    # ---------------------------
    # DATA TYPE CONVERSION
    # ---------------------------
    if src_dtype != tgt_dtype:

        if "date" in tgt_dtype and src_fmt:
            valid_df = valid_df.withColumn(col_ref, to_date(col(col_ref), src_fmt))

        elif "decimal" in tgt_dtype:
            valid_df = valid_df.withColumn(col_ref, col(col_ref).cast(tgt_dtype))

        elif "int" in tgt_dtype:
            valid_df = valid_df.withColumn(col_ref, col(col_ref).cast("int"))

        else:
            valid_df = valid_df.withColumn(col_ref, col(col_ref).cast("string"))

    # ---------------------------
    # NULL CHECK
    # ---------------------------
    if not nullable:
        failed = valid_df.filter(col(col_ref).isNull() | isnan(col(col_ref)))
        if failed.count() > 0:
            reject_df = failed if reject_df is None else reject_df.union(failed)
        valid_df = valid_df.filter(col(col_ref).isNotNull())

    # ---------------------------
    # REGEX CHECK
    # ---------------------------
    if regex:
        failed = valid_df.filter(~col(col_ref).rlike(regex))
        if failed.count() > 0:
            reject_df = failed if reject_df is None else reject_df.union(failed)
        valid_df = valid_df.filter(col(col_ref).rlike(regex))


# ============================================================
# 3️⃣ SOURCE VS TARGET ROW COUNT (DQ RULE)
# ============================================================
if dq_rules["check_row_count"]:
    source_count = df.count()
    curated_count = valid_df.count()
    print("Source count:", source_count)
    print("Curated count:", curated_count)
    print("Rejected:", source_count - curated_count)

    if source_count != curated_count:
        print("⚠ Row count mismatch (DQ rule active)")



# ============================================================
# 4️⃣ WRITE OUTPUTS
# ============================================================
valid_df.write.mode("overwrite").parquet(curated_path)
if reject_df is not None:
    reject_df.dropDuplicates().write.mode("overwrite").parquet(reject_path)

print("✔ ETL + DQ complete.")
