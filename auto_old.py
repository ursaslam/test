import json
import boto3
import sys
import traceback
from datetime import datetime

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, isnan, regexp_replace
import pyspark.sql.functions as F
from pyspark.sql.types import *


# ----------------------------------------------------------------------
# GLOBAL JOB INFO
# ----------------------------------------------------------------------
JOB_NM = "financial_transactions"
JOB_ID = f"{JOB_NM}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


# ----------------------------------------------------------------------
# STRUCTURED LOG FUNCTION
# ----------------------------------------------------------------------
def log_json(level, stage, message, **kwargs):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level.upper(),
        "job_nm": JOB_NM,
        "job_id": JOB_ID,
        "stage": stage,
        "message": message,
        "details": kwargs
    }
    print(json.dumps(log_entry))


# =====================================================================
# ETL PROCESS
# =====================================================================
try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    log_json("info", "init", "Job initialized")

    # ------------------------------------------------------------
    # LOAD METADATA
    # ------------------------------------------------------------
    metadata_path = "s3://your-bucket/metadata/financial_transactions.json"

    log_json("info", "metadata_load", "Loading metadata", path=metadata_path)

    s3 = boto3.client("s3")
    bucket = metadata_path.split("/")[2]
    key = "/".join(metadata_path.split("/")[3:])
    meta = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))

    ds = meta["ds"]
    cols = ds["cols"]
    src_prefix = ds["src_prefix"]
    data_format = ds["format"]

    log_json("info", "metadata_parse", "Metadata parsed",
             src_prefix=src_prefix, data_format=data_format)

    # ------------------------------------------------------------
    # READ SOURCE (NO DROPS)
    # ------------------------------------------------------------
    df = spark.read.format(data_format).load(src_prefix)
    src_count = df.count()

    log_json("info", "read_input", "Loaded source dataset", row_count=src_count)

    valid_df = df  # no filtering
    reject_df = None  # never used, since we keep all rows

    # ------------------------------------------------------------
    # SCHEMA VALIDATION (DO NOT DROP ANY ROW)
    # ------------------------------------------------------------
    source_cols = set(df.columns)
    expected_cols = set(c["src_nm"] for c in cols)

    missing = expected_cols - source_cols
    extra = source_cols - expected_cols

    log_json("info", "schema_validation", "Schema differences logged",
             missing=list(missing), extra=list(extra))

    # ------------------------------------------------------------
    # MAIN COLUMN TRANSFORMATIONS (MATCH OLD SCRIPT)
    # ------------------------------------------------------------
    for c in cols:
        src = c["src_nm"]
        tgt = c["tgt_nm"]
        src_dtype = c["src_dtype"]
        tgt_dtype = c["tgt_dtype"]
        regex = c["regex"]
        nullable = c["nullable"]
        src_fmt = c["src_fmt"]

        if src not in valid_df.columns:
            continue

        # RENAME
        if src != tgt:
            valid_df = valid_df.withColumnRenamed(src, tgt)
            log_json("info", "rename", f"Renamed {src} → {tgt}")

        col_ref = tgt

        # ---------------------------
        # LENIENT TYPE CAST (NO DROPS)
        # ---------------------------
        try:
            valid_df = valid_df.withColumn(
                col_ref,
                F.col(col_ref).cast(tgt_dtype)
            )
        except:
            # fallback to string (match old behavior)
            valid_df = valid_df.withColumn(col_ref, F.col(col_ref).cast("string"))

        log_json("info", "cast", f"Casted {col_ref} to {tgt_dtype}")

        # ---------------------------
        # NULL CHECK (ONLY LOG, DO NOT DROP)
        # ---------------------------
        null_fail_cnt = valid_df.filter(F.col(col_ref).isNull()).count()
        if null_fail_cnt > 0:
            log_json("warn", "null_check", f"NULL values in {col_ref}", count=null_fail_cnt)

        # ---------------------------
        # REGEX CHECK (ONLY LOG, DO NOT DROP)
        # ---------------------------
        if regex:
            regex_fail_cnt = valid_df.filter(~F.col(col_ref).rlike(regex)).count()
            if regex_fail_cnt > 0:
                log_json("warn", "regex_check", f"Regex failed for {col_ref}", count=regex_fail_cnt)

    # ------------------------------------------------------------
    # ROW COUNT CHECK (FOR LOGGING ONLY, NOT DROPPING)
    # ------------------------------------------------------------
    curated_count = valid_df.count()

    log_json(
        "info",
        "row_count_check",
        "Source vs Curated Row Count",
        source_count=src_count,
        curated_count=curated_count,
        difference=src_count - curated_count
    )

    # ------------------------------------------------------------
    # WRITE OUTPUT (MATCH OLD SCRIPT)
    # ------------------------------------------------------------
    curated_path = "s3://your-bucket/output/curated/"

    valid_df.write.mode("overwrite").parquet(curated_path)
    log_json("info", "write_output", "Curated data written", path=curated_path)

    log_json("info", "complete", "ETL completed (matching old script behavior)")

# =====================================================================
# EXCEPTION HANDLER
# =====================================================================
except Exception as e:
    log_json("error", "failure", f"ETL FAILED: {str(e)}", traceback=traceback.format_exc())
    sys.exit(1)
