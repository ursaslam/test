import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, current_timestamp, monotonically_increasing_id
)
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# -------------------------------------------------------------------------------------------
# LOAD JOB PARAMS
# -------------------------------------------------------------------------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "raw_path", "staging_path", "business_path", "metadata_path"]
)

raw_path = args["raw_path"]
staging_path = args["staging_path"]
business_path = args["business_path"]
metadata_path = args["metadata_path"]

# -------------------------------------------------------------------------------------------
# SPARK / GLUE SETUP
# -------------------------------------------------------------------------------------------
glueContext = GlueContext(SparkSession.builder.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------------------------------
# 1. METADATA LOADER
# -------------------------------------------------------------------------------------------
metadata_json = spark.read.text(metadata_path).collect()[0][0]
metadata = json.loads(metadata_json)

dataset_name = metadata["dataset_name"]
fields_metadata = metadata["fields"]

# Thresholds from metadata
thresholds = metadata.get("thresholds", {
    "max_reject_pct": 0.05,       # default 5%
    "max_null_pct": 0.10          # default 10%
})


# -------------------------------------------------------------------------------------------
# 2. SCHEMA BUILDER
# Build Spark StructType for curated/business layers
# -------------------------------------------------------------------------------------------
def parse_type(dtype):
    if dtype.startswith("decimal"):
        precision, scale = map(int, dtype.replace("decimal(", "").replace(")", "").split(","))
        return DecimalType(precision, scale)
    if dtype == "string":
        return StringType()
    if dtype == "date":
        return DateType()
    if dtype == "int":
        return IntegerType()
    if dtype == "bigint":
        return LongType()
    if dtype == "double":
        return DoubleType()
    return StringType()

curated_schema = StructType([
    StructField(f["target_field"], parse_type(f["target_type"]), f["nullable"])
    for f in fields_metadata
])


# -------------------------------------------------------------------------------------------
# 3. LOAD RAW DATA AND OVERWRITE RAW PARTITION
# ("Integration → RAW (overwrite partition)")
# -------------------------------------------------------------------------------------------
raw_df = spark.read.option("header", "true").csv(raw_path)

# Overwrite the RAW zone partition
raw_df.write.mode("overwrite").parquet(raw_path + "/processed/")


# -------------------------------------------------------------------------------------------
# 4. FIELD MAPPING ENGINE (rename + cast + add fields + validations)
# -------------------------------------------------------------------------------------------

rename_map = {}
cast_map = {}
added_fields = []
allowed_value_rules = {}
constraint_rules = []

for f in fields_metadata:
    src = f["source_field"]
    tgt = f["target_field"]

    # RENAME
    if src != "-" and src != tgt:
        rename_map[src] = tgt

    # CAST
    if src != "-" and f["source_type"] != f["target_type"]:
        cast_map[tgt] = f["target_type"]

    # ADD NEW FIELD
    if src == "-":
        added_fields.append((tgt, f["target_type"]))

    # ALLOWED VALUES
    if "allowed_values" in f:
        allowed_value_rules[tgt] = f["allowed_values"]

    # CONSTRAINTS (min/max)
    if "constraints" in f:
        constraints = f["constraints"]
        if "min" in constraints:
            constraint_rules.append((tgt, ">=", constraints["min"]))
        if "max" in constraints:
            constraint_rules.append((tgt, "<=", constraints["max"]))


# ---- APPLY RENAME ----
df = raw_df
for s, t in rename_map.items():
    df = df.withColumnRenamed(s, t)


# ---- APPLY CAST ----
def cast_column(df, col_name, spark_type):
    if spark_type.startswith("decimal"):
        p, s = map(int, spark_type.replace("decimal(", "").replace(")", "").split(","))
        return df.withColumn(col_name, col(col_name).cast(DecimalType(p, s)))
    if spark_type == "date":
        return df.withColumn(col_name, to_date(col(col_name)))
    if spark_type == "string":
        return df.withColumn(col_name, col(col_name).cast(StringType()))
    return df.withColumn(col_name, col(col_name).cast(StringType()))


for c, ttype in cast_map.items():
    df = cast_column(df, c, ttype)


# ---- ADD NEW FIELDS ----
for name, ttype in added_fields:
    df = df.withColumn(name, lit(None).cast(parse_type(ttype)))


# -------------------------------------------------------------------------------------------
# 5. APPLY VALIDATION + THRESHOLD LOGIC (diagram decision nodes)
# -------------------------------------------------------------------------------------------

invalid_df = df

# Allowed values
for col_name, allowed in allowed_value_rules.items():
    invalid_df = invalid_df.filter(~col(col_name).isin(allowed))

# Constraints
for col_name, op, value in constraint_rules:
    if op == ">=":
        invalid_df = invalid_df.filter(col(col_name) < value)
    if op == "<=":
        invalid_df = invalid_df.filter(col(col_name) > value)

invalid_count = invalid_df.count()
total_count = df.count()

reject_pct = invalid_count / max(total_count, 1)
null_pct = sum(df.filter(col(f["target_field"]).isNull()).count() for f in fields_metadata) / max(total_count, 1)

print(f"Invalid rows: {invalid_count}, Total: {total_count}")
print(f"Reject %: {reject_pct}, Null %: {null_pct}")

# Decision Node: If reject_pct > threshold → stop job
if reject_pct > thresholds["max_reject_pct"]:
    raise Exception(f"Job failed due to high reject percentage: {reject_pct}")

# Decision Node: Null percentage threshold
if null_pct > thresholds["max_null_pct"]:
    raise Exception(f"Job failed due to high null percentage: {null_pct}")


# -------------------------------------------------------------------------------------------
# 6. CURATED STAGING LOAD (overwrite curated layer)
# -------------------------------------------------------------------------------------------
staging_df = df.select([col(f["target_field"]) for f in fields_metadata])
staging_df = staging_df.withColumn("ingest_ts", current_timestamp())

staging_df.write.mode("overwrite").parquet(staging_path)


# -------------------------------------------------------------------------------------------
# 7. BUSINESS LAYER INCREMENTAL MERGE (SCD-1 logic)
# -------------------------------------------------------------------------------------------

# Primary Key from metadata
pk = [f["target_field"] for f in fields_metadata if f.get("primary_key")][0]

# Load existing business table
try:
    business_df = spark.read.parquet(business_path)
except:
    business_df = spark.createDataFrame([], staging_df.schema)

# Join keys for incremental merge
joined = staging_df.alias("s").join(
    business_df.alias("b"),
    on=[col("s." + pk) == col("b." + pk)],
    how="left"
)

updates = joined.filter(col("b." + pk).isNotNull()).select("s.*")
inserts = joined.filter(col("b." + pk).isNull()).select("s.*")

# Rebuild business table (SCD-1 overwrite on PK)
final_business_df = updates.unionByName(
    business_df.filter(~col(pk).isin([row[pk] for row in updates.collect()]))
).unionByName(inserts)

final_business_df.write.mode("overwrite").parquet(business_path)

print("Business layer merge completed.")


# -------------------------------------------------------------------------------------------
# END JOB
# -------------------------------------------------------------------------------------------
job.commit()
