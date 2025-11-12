{
  "pipeline": {
    "name": "customer_orders_etl",
    "description": "Load and transform customer and order data into curated zone",
    "run_id": "2025-11-12T07:00:00Z",
    "owner": "data_engineering_team",
    "execution_mode": "batch",
    "batch_window": "daily"
  },
  "source": {
    "type": "s3",
    "format": "csv",
    "connection": {
      "bucket": "raw-zone-data",
      "prefix": "orders/",
      "region": "us-east-1"
    },
    "schema": [
      {"name": "order_id", "type": "string"},
      {"name": "customer_id", "type": "string"},
      {"name": "order_date", "type": "date"},
      {"name": "amount", "type": "double"},
      {"name": "status", "type": "string"}
    ],
    "options": {
      "delimiter": ",",
      "header": true,
      "inferSchema": false
    }
  },
  "transformations": [
    {
      "type": "filter",
      "expression": "status != 'CANCELLED'"
    },
    {
      "type": "derive_column",
      "name": "order_year",
      "expression": "year(order_date)"
    },
    {
      "type": "lookup",
      "lookup_table": "dim_customers",
      "lookup_key": "customer_id",
      "join_type": "left",
      "join_condition": "source.customer_id = lookup.customer_id"
    },
    {
      "type": "cast",
      "columns": [
        {"name": "amount", "to_type": "decimal(10,2)"}
      ]
    }
  ],
  "target": {
    "type": "s3",
    "format": "parquet",
    "connection": {
      "bucket": "curated-zone-data",
      "prefix": "orders_curated/",
      "region": "us-east-1"
    },
    "partition_keys": ["order_year"],
    "mode": "overwrite"
  },
  "validations": [
    {
      "rule_name": "non_null_check",
      "column": "order_id",
      "condition": "is_not_null",
      "action": "fail_pipeline"
    },
    {
      "rule_name": "amount_positive",
      "column": "amount",
      "condition": ">= 0",
      "action": "warn"
    }
  ],
  "audit": {
    "log_to": "s3://etl-logs/customer_orders_etl/",
    "alert_on_failure": true,
    "alert_channel": "sns",
    "sns_topic": "arn:aws:sns:us-east-1:123456789012:etl-failure-alerts"
  }
}
