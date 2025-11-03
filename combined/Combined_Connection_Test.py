import requests
import psycopg2
import json

# Salesforce Configuration
SF_ORG_URL = "YOUR_SALESFORCE_ORG_URL"
SF_CLIENT_ID = "YOUR_SALESFORCE_CLIENT_ID"
SF_CLIENT_SECRET = "YOUR_SALESFORCE_CLIENT_SECRET"

# PostgreSQL Configuration
DB_HOST = "YOUR_DB_HOST"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "YOUR_DB_USER"
DB_PASSWORD = "YOUR_DB_PASSWORD"

def lambda_handler(event, context):
    """AWS Lambda handler - Test both Salesforce and PostgreSQL connections"""
    
    print("=" * 80)
    print("COMBINED CONNECTION TEST: Salesforce + PostgreSQL")
    print("=" * 80)
    
    # Test 1: Salesforce
    print("\n" + "=" * 80)
    print("TEST 1: Salesforce Connection & Query")
    print("=" * 80)
    sf_result = test_salesforce()
    
    # Test 2: PostgreSQL
    print("\n" + "=" * 80)
    print("TEST 2: PostgreSQL Connection & Query")
    print("=" * 80)
    pg_result = test_postgresql()
    
    # Combined result
    final_result = {
        'salesforce': sf_result,
        'postgresql': pg_result,
        'both_successful': sf_result['success'] and pg_result['success']
    }
    
    return {
        'statusCode': 200 if final_result['both_successful'] else 500,
        'body': json.dumps(final_result, indent=2)
    }

def test_salesforce():
    """Test Salesforce connection and query inventory data"""
    try:
        # Get OAuth token
        print("[*] Authenticating with Salesforce...")
        token_url = f"{SF_ORG_URL}/services/oauth2/token"
        
        token_response = requests.post(
            token_url,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data={
                'grant_type': 'client_credentials',
                'client_id': SF_CLIENT_ID,
                'client_secret': SF_CLIENT_SECRET
            },
            timeout=30
        )
        
        if token_response.status_code != 200:
            return {
                'success': False,
                'error': f"Authentication failed: {token_response.text}"
            }
        
        access_token = token_response.json()['access_token']
        instance_url = token_response.json().get('instance_url', SF_ORG_URL)
        print("[+] Authentication successful!")
        
        # Query inventory data
        print("[*] Querying inventory data...")
        soql_query = """
        SELECT 
            Unique_Id_UPPER__c,
            WOD_2__Serial_Number__c,
            twodscp__Part_Number__r.Name,
            WOD_2__Account__r.twodscp__External_ID__c 
        FROM WOD_2__Inventory__c 
        LIMIT 5
        """
        
        query_url = f"{instance_url}/services/data/v59.0/query"
        query_response = requests.get(
            query_url,
            headers={
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            },
            params={'q': soql_query.strip()},
            timeout=30
        )
        
        if query_response.status_code != 200:
            return {
                'success': False,
                'error': f"Query failed: {query_response.text}",
                'authenticated': True
            }
        
        query_result = query_response.json()
        records = query_result.get('records', [])
        
        print(f"[+] Query successful! Retrieved {len(records)} records")
        for i, record in enumerate(records, 1):
            print(f"    Record {i}:")
            print(f"      Unique_Id_UPPER__c: {record.get('Unique_Id_UPPER__c')}")
            print(f"      Serial_Number__c: {record.get('WOD_2__Serial_Number__c')}")
            part_name = record.get('twodscp__Part_Number__r', {}).get('Name') if record.get('twodscp__Part_Number__r') else None
            print(f"      Part_Number: {part_name}")
            external_id = record.get('WOD_2__Account__r', {}).get('twodscp__External_ID__c') if record.get('WOD_2__Account__r') else None
            print(f"      Account_External_ID: {external_id}")
        
        return {
            'success': True,
            'message': 'Salesforce query successful',
            'records_retrieved': len(records),
            'records': records
        }
        
    except Exception as e:
        print(f"[-] Salesforce error: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

def test_postgresql():
    """Test PostgreSQL connection and query data"""
    try:
        print("[*] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode='require',
            connect_timeout=10
        )
        print("[+] PostgreSQL connection successful!")
        
        cur = conn.cursor()
        
        # Get database info
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"[+] Database version: {version}")
        
        # Query data (limit 5)
        print("[*] Querying data (limit 5)...")
        cur.execute("""
            SELECT table_schema, table_name, table_type 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            LIMIT 5
        """)
        
        rows = cur.fetchall()
        print(f"[+] Query successful! Retrieved {len(rows)} rows")
        for i, row in enumerate(rows, 1):
            print(f"    Row {i}: Schema={row[0]}, Table={row[1]}, Type={row[2]}")
        
        # Prepare result data
        result_data = [
            {'schema': row[0], 'table': row[1], 'type': row[2]}
            for row in rows
        ]
        
        cur.close()
        conn.close()
        print("[*] Connection closed")
        
        return {
            'success': True,
            'message': 'PostgreSQL query successful',
            'records_retrieved': len(rows),
            'database_version': version,
            'records': result_data
        }
        
    except Exception as e:
        print(f"[-] PostgreSQL error: {str(e)}")
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    print("=" * 80)
    print("Combined Salesforce + PostgreSQL Connection Test")
    print("=" * 80)
    
    result = lambda_handler(None, None)
    
    print("\n" + "=" * 80)
    print("FINAL RESULT")
    print("=" * 80)
    print(result['body'])


