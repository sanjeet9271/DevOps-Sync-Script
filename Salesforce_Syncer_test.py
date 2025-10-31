import psycopg2

# Replace these values with your Aurora details
DB_HOST = "db-sep-postgre-instance-1.cgj0rqco754z.us-west-2.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = "master"
DB_PASSWORD = "SmartEntryPortal"

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    result = test_connection()
    return {
        'statusCode': 200 if result['success'] else 500,
        'body': result
    }

def test_connection():
    try:
        print("[*] Connecting to Aurora PostgreSQL...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode='require'  # Database requires SSL encryption from this IP
        )
        print("[+] Connection successful!")

        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print("PostgreSQL version:", version[0])

        cur.close()
        conn.close()
        print("[*] Connection closed cleanly.")
        
        return {
            'success': True,
            'message': 'Database connection successful',
            'version': version[0]
        }
    except Exception as e:
        print("[-] Connection failed:")
        print(str(e))
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    test_connection()
