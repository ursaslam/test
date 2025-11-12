{
  "pipeline_name": "adw_to_aws_glue_etl",
  "description": "Automated pipeline from Oracle ADW to curated warehouse using AWS Glue Workflow and EventBridge trigger",
  "source": {
    "system": "Oracle_ADW",
    "connection": {
      "jdbc_url": "jdbc:oracle:thin:@adw-db.example.com:1522/ADWDB",
      "username": "adw_user",
      "password_secret_arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:adw_creds"
    },
    "export_mode": "incremental_or_full",
    "dataset_prefix_rule": "s3://landing-bucket/raw/{dataset}/{load_type}/",
    "load_types": ["full", "incremental"]
  },
  "landing_zone": {
    "bucket": "landing-bucket",
    "prefix_structure": "raw/{dataset}/{load_type}/load_dt={YYYY-MM-DD}/",
    "metadata_file": "manifest.json"
  },
  "eventbridge": {
    "rule_name": "adw-s3-upload-trigger",
    "event_pattern": {
      "source": ["aws.s3"],
      "detail-type": ["Object Created"],
      "detail": {
        "bucket": ["landing-bucket"]
      }
    },
    "target": "aws_glue_workflow"
  },
  "glue_workflow": {
    "name": "adw_to_curated_workflow",
    "trigger_type": "event",
    "jobs": [
      {
        "name": "adw_etl_validation_job",
        "type": "pyspark",
        "staging_path": "s3://data-stage/temp/{dataset}/",
        "validations": {
          "id_check": true,
          "schema_validation": true,
          "row_count_validation": true,
          "datatype_enforcement": true,
          "metadata_enrichment": true
        },
        "validation_rules": {
          "schema_match_mode": "strict",
          "row_count_threshold": "±5%",
          "enrich_metadata_fields": ["load_id", "source_system", "ingestion_ts"]
        },
        "output": {
          "curated_path": "s3://curated-warehouse/{dataset}/",
          "write_mode": "upsert",
          "primary_key": "id"
        }
      }
    ]
  },
  "curated_zone": {
    "warehouse_type": "S3-based Data Lakehouse",
    "glue_catalog_database": "curated_dw",
    "table_properties": {
      "classification": "parquet",
      "compression": "snappy"
    }
  },
  "monitoring": {
    "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:glue-pipeline-notifications",
    "log_group": "/aws/glue/jobs/logs/adw_to_curated"
  },
  "metadata_enrichment": {
    "common_fields": {
      "source_system": "ADW",
      "pipeline_run_id": "{uuid}",
      "ingestion_timestamp": "{current_ts}"
    }
  }
}
