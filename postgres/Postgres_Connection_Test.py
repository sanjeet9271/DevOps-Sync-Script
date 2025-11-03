import psycopg2
import json

# Replace these values with your Aurora details
DB_HOST = "YOUR_DB_HOST"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "YOUR_DB_USER"
DB_PASSWORD = "YOUR_DB_PASSWORD"

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    result = test_connection()
    return {
        'statusCode': 200 if result['success'] else 500,
        'body': json.dumps(result, indent=2)
    }

def test_connection():
    try:
        print("[*] Connecting to Aurora PostgreSQL...")
        print(f"[*] Host: {DB_HOST}")
        print(f"[*] Database: {DB_NAME}")
        
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode='require',  # Database requires SSL encryption
            connect_timeout=10
        )
        print("[+] Connection successful!")

        cur = conn.cursor()
        
        # Test 1: Get PostgreSQL version
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"[+] PostgreSQL version: {version[0]}")
        
        # Test 2: Check current database
        cur.execute("SELECT current_database();")
        current_db = cur.fetchone()
        print(f"[+] Current database: {current_db[0]}")
        
        # Test 3: List available schemas
        cur.execute("SELECT schema_name FROM information_schema.schemata;")
        schemas = cur.fetchall()
        print(f"[+] Available schemas: {len(schemas)}")

        cur.close()
        conn.close()
        print("[*] Connection closed cleanly.")
        
        return {
            'success': True,
            'message': 'Database connection successful',
            'host': DB_HOST,
            'database': current_db[0],
            'version': version[0],
            'schema_count': len(schemas)
        }
    except psycopg2.OperationalError as e:
        print("[-] Connection failed (Operational Error):")
        print(str(e))
        return {
            'success': False,
            'error_type': 'OperationalError',
            'error': str(e)
        }
    except Exception as e:
        print("[-] Connection failed:")
        print(str(e))
        return {
            'success': False,
            'error_type': type(e).__name__,
            'error': str(e)
        }

if __name__ == "__main__":
    print("=" * 80)
    print("Testing PostgreSQL Connection")
    print("=" * 80)
    result = test_connection()
    
    print("\n" + "=" * 80)
    print("Result")
    print("=" * 80)
    if result['success']:
        print(f"[+] SUCCESS")
        print(f"Database: {result.get('database')}")
        print(f"Version: {result.get('version')}")
    else:
        print(f"[-] FAILED")
        print(f"Error: {result.get('error')}")

