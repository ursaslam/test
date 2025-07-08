📘 Technical Documentation
Hydrating DynamoDB from Athena (Glue Data Catalog) and XML Files (Local File System)
🔹 1. Overview
This system hydrates data into Amazon DynamoDB from two sources:

Source A: AWS Glue Data Catalog (queried via Athena)

Source B: XML files stored in a local file system

The primary hydration is executed by an AWS Lambda function, triggered and orchestrated via AWS Step Functions (state machine).

2. Architecture Diagram
A. Athena-to-DynamoDB via Step Function
pgsql
Copy
Edit
          ┌────────────────────────────┐
          │ AWS Step Function (Trigger)│
          └────────────┬───────────────┘
                       │
           ┌───────────▼────────────┐
           │   Lambda: Athena Query │
           └───────────┬────────────┘
                       │
           ┌───────────▼────────────┐
           │ Wait for Athena Result │
           └───────────┬────────────┘
                       │
           ┌───────────▼────────────┐
           │ Parse + Transform Data │
           └───────────┬────────────┘
                       │
           ┌───────────▼────────────┐
           │ Write to DynamoDB      │
           └────────────────────────┘
B. Local File System XML to DynamoDB
pgsql
Copy
Edit
┌──────────────────────────────┐
│  On-prem or EC2-based script │
└──────────────┬───────────────┘
               │
      ┌────────▼────────┐
      │ Parse XML Files │
      └────────┬────────┘
               │
      ┌────────▼────────┐
      │ Format Data     │
      └────────┬────────┘
               │
      ┌────────▼────────┐
      │ Write to DDB    │
      └─────────────────┘
🔹 3. Data Flow Description
🔸 Source A: Glue → Athena → Lambda → DynamoDB
Step	Component	Action
1	Step Function	Initiates the Lambda execution
2	Lambda	Executes Athena query against Glue Catalog
3	Lambda	Polls Athena query results
4	Lambda	Transforms data into DynamoDB format
5	Lambda	Writes data to DynamoDB using PutItem or BatchWriteItem

🔸 Source B: File System XML → Local Script → DynamoDB
Step	Component	Action
1	Python Script (on EC2 or on-prem)	Reads XML files from a folder path
2	Script	Parses and maps XML to DDB schema
3	Script	Uses boto3 to update DynamoDB

🔹 4. Step Function Configuration
Sample State Machine JSON (Simplified)
json
Copy
Edit
{
  "StartAt": "QueryAthena",
  "States": {
    "QueryAthena": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:region:account:function:AthenaQueryLambda",
      "Next": "WaitForResults"
    },
    "WaitForResults": {
      "Type": "Wait",
      "Seconds": 10,
      "Next": "ProcessResults"
    },
    "ProcessResults": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:region:account:function:ProcessAndWriteLambda",
      "End": true
    }
  }
}
🔹 5. DynamoDB Table Schema Suggestion
Field Name	Type	Notes
id	String	Primary key
source_type	String	athena or filesystem
payload	Map	Actual business data
updated_at	String	ISO timestamp

🔹 6. Lambda Technical Notes
Lambda 1: Athena Query Executor
Uses boto3.start_query_execution

Stores output in S3

Returns QueryExecutionId

Lambda 2: Result Processor
Polls Athena until SUCCEEDED

Parses S3 CSV result

Transforms and writes to DynamoDB

🔹 7. Error Handling & Monitoring
Component	Monitoring/Retry
Step Function	Built-in retries, catch/fail states
Lambda	DLQ configured for unhandled errors
DynamoDB	Throttling monitored via CloudWatch
XML Parser	Try/except with file name logging
