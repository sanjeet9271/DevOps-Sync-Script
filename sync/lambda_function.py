"""
AWS Lambda Function - Salesforce to PostgreSQL Sync
Main entry point - Uses JSON configuration (no code changes needed)
"""
import json
import boto3
from botocore.exceptions import ClientError
from salesforce_accessor import SalesforceAccessor
from postgres_accessor import PostgresAccessor
from data_syncer import DataSyncer, SyncConfig


def get_secret(secret_name, region_name="us-west-2"):
    """
    Retrieve secret from AWS Secrets Manager
    
    Args:
        secret_name: Name of the secret in Secrets Manager
        region_name: AWS region
    
    Returns:
        Secret string value
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret_string = get_secret_value_response['SecretString']
        
        # Parse JSON if the secret is stored as key-value pair
        try:
            secret_dict = json.loads(secret_string)
            # If it's a dict, extract the value using the secret name as key
            if isinstance(secret_dict, dict) and secret_name in secret_dict:
                return secret_dict[secret_name]
            # Otherwise return the whole dict's first value
            elif isinstance(secret_dict, dict):
                return list(secret_dict.values())[0]
        except json.JSONDecodeError:
            # Not JSON, return as-is
            pass
        
        return secret_string
    except ClientError as e:
        print(f"[!] Error retrieving secret '{secret_name}': {str(e)}")
        raise e


def load_secrets():
    """
    Load required secrets from AWS Secrets Manager
    
    Returns:
        Dict with all configuration values
    """
    print("[*] Loading secrets from AWS Secrets Manager...")
    
    # Fetch secrets
    sf_client_id = get_secret('SEP_SALESFORCE_STG_CLIENT_ID')
    sf_client_secret = get_secret('SEP_SALESFORCE_STG_CLIENT_SECRET')
    db_password = get_secret('SEP_POSTGRES_MASTER_PASSWORD')
    
    return {
        # From Secrets Manager
        'sf_client_id': sf_client_id,
        'sf_client_secret': sf_client_secret,
        'db_password': db_password,
        
        # Hardcoded (non-sensitive)
        'sf_org_url': 'https://onetrimblesupport--stg.sandbox.my.salesforce.com',
        'db_host': 'db-sep-postgre-instance-1.cgj0rqco754z.us-west-2.rds.amazonaws.com',
        'db_user': 'master',
        'db_name': 'postgres',
        'db_port': 5432
    }


def lambda_handler(event, context):
    """
    AWS Lambda handler function - Driven by JSON configuration
    
    Event structure:
    {
        "tables": [
            {
                "sf_object": "WOD_2__Inventory__c",
                "soql_query": "SELECT ... FROM WOD_2__Inventory__c",
                "pg_table": "inventory",
                "field_mapping": {
                    "SF_Field__c": "pg_column"
                },
                "primary_keys": ["key1", "key2"]
            }
        ],
        "batch_size": 2000  // Optional, defaults to 2000
    }
    """
    print("=" * 80)
    print("Salesforce to PostgreSQL Data Sync - JSON Configuration Mode")
    print("=" * 80)
    
    # Validate event structure
    if not event or 'tables' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Invalid event structure. Required: {"tables": [...]}'
            })
        }
    
    try:
        # Load secrets from AWS Secrets Manager
        secrets = load_secrets()
        print("[+] Secrets loaded successfully")
        
        # Parse configuration
        tables_config = event['tables']
        batch_size = event.get('batch_size', 2000)
        
        print(f"\n[*] Configuration loaded: {len(tables_config)} table(s) to sync")
        
        # Initialize accessors
        print("[*] Initializing Salesforce accessor...")
        sf_accessor = SalesforceAccessor(
            secrets['sf_org_url'], 
            secrets['sf_client_id'], 
            secrets['sf_client_secret']
        )
        
        print("[*] Initializing PostgreSQL accessor...")
        pg_accessor = PostgresAccessor(
            secrets['db_host'],
            secrets['db_port'],
            secrets['db_name'],
            secrets['db_user'],
            secrets['db_password']
        )
        
        # Connect to PostgreSQL
        if not pg_accessor.connect():
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to connect to PostgreSQL'})
            }
        
        # Authenticate with Salesforce
        if not sf_accessor.authenticate():
            pg_accessor.disconnect()
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to authenticate with Salesforce'})
            }
        
        # Build sync configurations from JSON
        sync_configs = []
        for table_config in tables_config:
            # Validate required fields
            required_fields = ['sf_object', 'soql_query', 'pg_table', 'field_mapping', 'primary_keys']
            missing_fields = [f for f in required_fields if f not in table_config]
            
            if missing_fields:
                print(f"[!] Skipping table: Missing fields {missing_fields}")
                continue
            
            config = SyncConfig(
                sf_object=table_config['sf_object'],
                soql_query=table_config['soql_query'],
                pg_table=table_config['pg_table'],
                field_mapping=table_config['field_mapping'],
                primary_keys=table_config['primary_keys'],
                batch_size=table_config.get('batch_size', batch_size)
            )
            sync_configs.append(config)
        
        if not sync_configs:
            pg_accessor.disconnect()
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No valid table configurations found'})
            }
        
        # Execute sync
        print(f"\n[*] Starting data sync for {len(sync_configs)} table(s)...")
        syncer = DataSyncer(sf_accessor, pg_accessor)
        
        if len(sync_configs) == 1:
            results = [syncer.sync(sync_configs[0])]
        else:
            results = syncer.sync_multiple(sync_configs)
        
        # Disconnect
        pg_accessor.disconnect()
        
        # Prepare response
        success = all(r.get('success', False) for r in results)
        
        return {
            'statusCode': 200 if success else 500,
            'body': json.dumps({
                'success': success,
                'tables_synced': len(results),
                'results': results
            }, indent=2)
        }
        
    except Exception as e:
        print(f"[!] Lambda execution error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }


if __name__ == "__main__":
    # For local testing - Load config from file
    print("Running locally with config.json...")
    
    try:
        with open('config.json', 'r') as f:
            test_event = json.load(f)
        
        result = lambda_handler(test_event, None)
        
        print("\n" + "=" * 80)
        print("RESULT")
        print("=" * 80)
        print(result['body'])
        
    except FileNotFoundError:
        print("[!] config.json not found. Create it with your table configurations.")
        print("\nExample config.json structure:")
        print(json.dumps({
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
        }, indent=2))
