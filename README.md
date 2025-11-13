#finance_mapping.json in S3

{
  "dataset_name": "financial_transactions_curated",
  "description": "Field-level mapping with validation rules",
  "fields": [
    {
      "field_name": "transaction_id",
      "source_field": "txn_id",
      "target_field": "transaction_id",
      "source_type": "string",
      "target_type": "string",
      "nullable": false,
      "primary_key": true,
      "change_type": "renamed"
    },
    {
      "field_name": "account_number",
      "source_field": "acct_no",
      "target_field": "account_number",
      "source_type": "string",
      "target_type": "string",
      "nullable": false,
      "change_type": "renamed"
    },
    {
      "field_name": "transaction_date",
      "source_field": "txn_date",
      "target_field": "transaction_date",
      "source_type": "date",
      "target_type": "date",
      "nullable": false,
      "change_type": "renamed"
    },
    {
      "field_name": "amount",
      "source_field": "amount",
      "target_field": "amount",
      "source_type": "decimal(10,2)",
      "target_type": "decimal(12,2)",
      "nullable": false,
      "constraints": {"min": 0.01, "max": 1000000.00},
      "change_type": "type_modified"
    },
    {
      "field_name": "transaction_type",
      "source_field": "-",
      "target_field": "transaction_type",
      "source_type": "-",
      "target_type": "string",
      "nullable": false,
      "allowed_values": ["debit", "credit"],
      "change_type": "added"
    }
  ]
}
