📘 Technical Documentation
Hydrating DynamoDB from Athena (Glue Data Catalog) and XML Files (Local File System)
🔹 1. Overview
This system hydrates data into Amazon DynamoDB from two sources:

Source A: AWS Glue Data Catalog (queried via Athena)

Source B: XML files stored in a local file system

The primary hydration is executed by an AWS Lambda function, triggered and orchestrated via AWS Step Functions (state machine).
