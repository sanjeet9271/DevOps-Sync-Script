# Salesforce to PostgreSQL Sync

JSON-driven sync system. No code changes needed.

## Quick Start

**Deploy:** Upload `sync_lambda_deployment.zip` to AWS Lambda
- Runtime: Python 3.9
- Handler: `lambda_function.lambda_handler`
- Timeout: 300s
- Memory: 512 MB

## Configuration

Pass JSON event to Lambda:

```json
{
  "tables": [
    {
      "sf_object": "WOD_2__Inventory__c",
      "soql_query": "SELECT Unique_Id_UPPER__c, WOD_2__Serial_Number__c FROM WOD_2__Inventory__c",
      "pg_table": "inventory",
      "field_mapping": {
        "Unique_Id_UPPER__c": "unique_id",
        "WOD_2__Serial_Number__c": "serial_number"
      },
      "primary_keys": ["unique_id"]
    }
  ],
  "batch_size": 2000
}
```

See `CONFIG_TEMPLATE.json` for full example.

## Features

- **Watermark-based sync**: Only fetches records modified since last sync
- **Auto-adds fields**: `IsDeleted` and `LastModifiedDate` added automatically
- **Batch processing**: 2000 records at a time
- **Composite keys**: Supports multi-column primary keys
- **Nested fields**: Handles relationships (e.g., `Account__r.Name`)

## Watermark Table

Auto-created on first run:

```sql
CREATE TABLE watermark (
    table_name VARCHAR(255) PRIMARY KEY,
    last_synced_time TIMESTAMP
);
```

## Important

- PostgreSQL tables must exist before sync (not auto-created)
- Tables will fail with clear error if missing
- First sync fetches all records, subsequent syncs are incremental

## Example

```json
{
  "tables": [
    {
      "sf_object": "WOD_2__Inventory__c",
      "soql_query": "SELECT Unique_Id_UPPER__c, WOD_2__Serial_Number__c, twodscp__Part_Number__r.Name, WOD_2__Account__r.twodscp__External_ID__c FROM WOD_2__Inventory__c",
      "pg_table": "inventory",
      "field_mapping": {
        "Unique_Id_UPPER__c": "unique_id",
        "WOD_2__Serial_Number__c": "serial_number",
        "twodscp__Part_Number__r.Name": "part_number",
        "WOD_2__Account__r.twodscp__External_ID__c": "fch_party_id"
      },
      "primary_keys": ["serial_number", "part_number", "fch_party_id"]
    }
  ]
}
```

