import json
import boto3
import sys
import traceback
import uuid
from datetime import datetime

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import *


# ----------------------------------------------------------------------
# GLOBAL JOB INFO
# ----------------------------------------------------------------------
JOB_NM = "financial_transactions"
JOB_ID = f"{JOB_NM}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


# ----------------------------------------------------------------------
# CUSTOM STRUCTURED LOG FUNCTION
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
    # Init Spark
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    log_json("info", "init", "Glue job initialized")


    # ------------------------------------------------------------
    # 1) LOAD METADATA
    # ------------------------------------------------------------
    metadata_path = "s3://your-bucket/metadata/financial_transactions.json"
    log_json("info", "metadata_load", "Loading metadata JSON", path=metadata_path)

    s3 = boto3.client("s3")
    bucket = metadata_path.split("/")[2]
    key = "/".join(metadata_path.split("/")[3:])
    meta = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8"))
    ds = meta["ds"]

    cols = ds["cols"]
    src_prefix = ds["src_prefix"]
    data_format = ds["format"]
    id_cols = ds["id_cols"]
    upd_cols = ds["upd_cols"]
    dq = ds["dq_rules"]

    log_json("info", "metadata_parse",
             "Parsed dataset metadata",
             src_prefix=src_prefix,
             format=data_format,
             id_cols=id_cols,
             upd_cols=upd_cols)


    # ------------------------------------------------------------
    # 2) READ SOURCE
    # ------------------------------------------------------------
    log_json("info", "read_input", f"Reading input from {src_prefix}")

    df = spark.read.format(data_format).load(src_prefix)
    src_count_initial = df.count()

    log_json("info", "read_input", "Source read complete", row_count=src_count_initial)


    # ------------------------------------------------------------
    # 3) SCHEMA VALIDATION
    # ------------------------------------------------------------
    log_json("info", "schema_validation", "Validating schema")

    source_cols = set(df.columns)
    expected_cols = set(c["src_nm"] for c in cols)

    missing = expected_cols - source_cols
    extra = source_cols - expected_cols

    if missing:
        log_json("error", "schema_validation", "Missing columns", missing=list(missing))

    if extra:
        log_json("warn", "schema_validation", "Extra columns found", extra=list(extra))

    log_json("info", "schema_validation", "Schema validation completed")


    # ------------------------------------------------------------
    # 4) ADD MISSING UPDATE COLUMNS
    # ------------------------------------------------------------
    for upd in upd_cols:
        if upd not in df.columns:
            df = df.withColumn(upd, F.current_timestamp())
            log_json("info", "dedupe_setup", f"Added missing update column {upd}")


    # ------------------------------------------------------------
    # 5) DEDUPE
    # ------------------------------------------------------------
    if id_cols and upd_cols:
        log_json("info", "dedupe", "Running dedupe", id_cols=id_cols, upd_cols=upd_cols)

        w = Window.partitionBy(*id_cols).orderBy(
            *[F.col(u).desc() for u in upd_cols]
        )
        df = df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")

        log_json("info", "dedupe", "Dedupe completed", deduped_rows=df.count())
    else:
        log_json("warn", "dedupe", "Skipping dedupe; id_cols or upd_cols missing")


    # ------------------------------------------------------------
    # 6) APPLY TRANSFORMATIONS + DQ
    # ------------------------------------------------------------
    valid_df = df
    reject_df = None

    log_json("info", "transform", "Starting column-level transformations + DQ")

    for c in cols:
        src = c["src_nm"]
        tgt = c["tgt_nm"]
        regex = c["regex"]
        nullable = c["nullable"]
        src_dtype = c["src_dtype"]
        tgt_dtype = c["tgt_dtype"]

        if src not in df.columns:
            continue

        # rename
        if src != tgt:
            valid_df = valid_df.withColumnRenamed(src, tgt)
            log_json("info", "transform", f"Renamed {src} → {tgt}")

        col = tgt

        # type conversion
        if src_dtype != tgt_dtype:
            valid_df = valid_df.withColumn(col, F.col(col).cast(tgt_dtype))
            log_json("info", "datatype_cast", f"Casting {col}",
                     src_dtype=src_dtype, tgt_dtype=tgt_dtype)

        # null check
        if not nullable:
            failed = valid_df.filter(F.col(col).isNull())
            if failed.count() > 0:
                log_json("warn", "dq_null",
                         f"Null check failed on {col}",
                         failed_rows=failed.count())
                reject_df = failed if reject_df is None else reject_df.union(failed)
            valid_df = valid_df.filter(F.col(col).isNotNull())

        # regex check
        if regex:
            failed = valid_df.filter(~F.col(col).rlike(regex))
            if failed.count() > 0:
                log_json("warn", "dq_regex",
                         f"Regex check failed on {col}",
                         failed_rows=failed.count())
                reject_df = reject_df.union(failed) if reject_df else failed
            valid_df = valid_df.filter(F.col(col).rlike(regex))

    log_json("info", "transform", "Column-level transformations complete")


    # ------------------------------------------------------------
    # 6.1) ROW COUNT CHECK (Source vs Curated)
    # ------------------------------------------------------------
    src_count = src_count_initial
    curated_count = valid_df.count()
    rejected_count = src_count - curated_count
    mismatch = (src_count != curated_count)

    log_json(
        "info",
        "row_count_check",
        "Row count comparison complete",
        source_count=src_count,
        curated_count=curated_count,
        rejected_count=rejected_count,
        mismatch=mismatch
    )

    if mismatch:
        log_json(
            "warn",
            "row_count_check",
            "Row count mismatch detected",
            source_count=src_count,
            curated_count=curated_count,
            rejected_rows=rejected_count,
            pct_loss=f"{round((rejected_count/src_count)*100, 2)}%" if src_count > 0 else "0%"
        )
    else:
        log_json(
            "info",
            "row_count_check",
            "Row count validation passed",
            source_count=src_count,
            curated_count=curated_count,
            rejected_rows=rejected_count
        )


    # ------------------------------------------------------------
    # 7) WRITE OUTPUT
    # ------------------------------------------------------------
    curated_path = "s3://your-bucket/output/curated/"
    reject_path  = "s3://your-bucket/output/rejects/"

    valid_df.write.mode("overwrite").parquet(curated_path)
    log_json("info", "write_output", f"Curated dataset written", path=curated_path)

    if reject_df is not None:
        reject_df.write.mode("overwrite").parquet(reject_path)
        log_json("info", "write_output", f"Rejects written", path=reject_path)

    log_json("info", "complete", "ETL Completed Successfully")


# =====================================================================
# FAILURE HANDLER
# =====================================================================
except Exception as e:
    log_json("error", "failure", f"ETL FAILED: {str(e)}", traceback=traceback.format_exc())
    sys.exit(1)
