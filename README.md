Schema metadata for Dataset:


{
  "dataset": {
    "name": "customer_orders",
    "version": "v1.0",
    "description": "Customer orders data including customer info and order details",
    "owner": "data_engineering_team",
    "ingestion_frequency": "daily",
    "last_updated": "2025-11-12T00:00:00Z"
  },
  "schema": {
    "fields": [
      {
        "name": "order_id",
        "type": "string",
        "nullable": false,
        "description": "Unique identifier for the order",
        "business_key": true
      },
      {
        "name": "customer_id",
        "type": "string",
        "nullable": false,
        "description": "Identifier for the customer placing the order",
        "foreign_key": {
          "dataset": "customers",
          "field": "customer_id"
        }
      },
      {
        "name": "order_date",
        "type": "date",
        "nullable": false,
        "description": "Date when the order was created"
      },
      {
        "name": "amount",
        "type": "decimal(10,2)",
        "nullable": false,
        "description": "Monetary amount of the order"
      },
      {
        "name": "status",
        "type": "string",
        "nullable": false,
        "description": "Status of the order (e.g., PENDING, SHIPPED, CANCELLED)",
        "allowed_values": ["PENDING", "SHIPPED", "CANCELLED"]
      },
      {
        "name": "created_at",
        "type": "timestamp",
        "nullable": false,
        "description": "Timestamp when the record was created"
      },
      {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": true,
        "description": "Timestamp when the record was last updated"
      }
    ],
    "partition_keys": ["order_date"],
    "primary_key": ["order_id"]
  },
  "additional_metadata": {
    "source_system": "ecommerce_platform",
    "integration_steps": [
      "extract from platform_db.orders",
      "transform: filter out cancelled orders",
      "load to curated_zone.customer_orders"
    ],
    "data_steward": "john.doe@company.com",
    "quality_checks": [
      {
        "check_name": "non_null_order_id",
        "field": "order_id",
        "condition": "is_not_null",
        "severity": "fail"
      },
      {
        "check_name": "amount_positive",
        "field": "amount",
        "condition": ">= 0",
        "severity": "warn"
      }
    ]
  }
}




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
