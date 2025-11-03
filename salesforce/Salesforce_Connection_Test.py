import requests
import json

# Salesforce Configuration (OAuth 2.0 Client Credentials Flow)
SF_ORG_URL = "YOUR_SALESFORCE_ORG_URL"
SF_CLIENT_ID = "YOUR_SALESFORCE_CLIENT_ID"
SF_CLIENT_SECRET = "YOUR_SALESFORCE_CLIENT_SECRET"

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    
    # Step 1: Test Internet Connectivity
    print("=" * 80)
    print("STEP 1: Testing Internet Connectivity")
    print("=" * 80)
    connectivity_result = test_internet_connectivity()
    
    # Step 2: Test Salesforce Connection (only if internet is reachable)
    print("\n" + "=" * 80)
    print("STEP 2: Testing Salesforce Connection")
    print("=" * 80)
    
    if connectivity_result['internet_accessible']:
        salesforce_result = test_salesforce_connection()
    else:
        print("[-] Skipping Salesforce test - No internet connectivity")
        salesforce_result = {
            'success': False,
            'error': 'No internet connectivity - skipped Salesforce test'
        }
    
    # Combine results
    final_result = {
        'internet_connectivity': connectivity_result,
        'salesforce_connection': salesforce_result,
        'overall_success': connectivity_result['internet_accessible'] and salesforce_result['success']
    }
    
    return {
        'statusCode': 200 if final_result['overall_success'] else 500,
        'body': json.dumps(final_result, indent=2)
    }

def test_internet_connectivity():
    """Test if Lambda can reach public internet via HTTPS"""
    test_endpoints = [
        'https://www.google.com',
        'https://www.amazon.com',
        'https://api.ipify.org'
    ]
    
    results = []
    successful_connections = 0
    
    for url in test_endpoints:
        try:
            print(f"[*] Testing HTTPS connectivity to {url}...")
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                print(f"[+] SUCCESS: {url} (Status: {response.status_code})")
                results.append({
                    'url': url,
                    'reachable': True,
                    'status_code': response.status_code
                })
                successful_connections += 1
            else:
                print(f"[-] FAILED: {url} returned status {response.status_code}")
                results.append({
                    'url': url,
                    'reachable': False,
                    'status_code': response.status_code
                })
        except requests.exceptions.Timeout:
            print(f"[-] TIMEOUT: {url} timed out")
            results.append({
                'url': url,
                'reachable': False,
                'error': 'Connection timeout'
            })
        except Exception as e:
            print(f"[-] ERROR: Failed to connect to {url} - {str(e)}")
            results.append({
                'url': url,
                'reachable': False,
                'error': str(e)
            })
    
    internet_accessible = successful_connections > 0
    
    print("\n" + "-" * 80)
    print(f"Internet Connectivity Summary: {successful_connections}/{len(test_endpoints)} endpoints reachable")
    print("-" * 80)
    
    return {
        'internet_accessible': internet_accessible,
        'successful_connections': successful_connections,
        'total_endpoints_tested': len(test_endpoints),
        'results': results
    }

def test_salesforce_connection():
    """Test Salesforce connection using OAuth 2.0 Client Credentials Flow"""
    try:
        print("[*] Connecting to Salesforce...")
        
        # OAuth 2.0 Token endpoint
        token_url = f"{SF_ORG_URL}/services/oauth2/token"
        
        # Request access token using Client Credentials flow
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': SF_CLIENT_ID,
            'client_secret': SF_CLIENT_SECRET
        }
        
        print(f"[*] Requesting access token from {SF_ORG_URL}...")
        token_response = requests.post(token_url, headers=headers, data=token_data, timeout=30)
        
        if token_response.status_code != 200:
            print(f"[-] Token request failed with status {token_response.status_code}")
            print(f"Response: {token_response.text}")
            return {
                'success': False,
                'error': f"Token request failed: {token_response.text}",
                'status_code': token_response.status_code
            }
        
        token_json = token_response.json()
        access_token = token_json.get('access_token')
        instance_url = token_json.get('instance_url', SF_ORG_URL)
        
        print("[+] Access token obtained successfully!")
        print(f"[+] Access token: {access_token[:20]}...")
        
        # Test the connection by querying Organization info
        query_headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        query_url = f"{instance_url}/services/data/v59.0/query"
        query_params = {'q': 'SELECT Id, Name FROM Organization LIMIT 1'}
        
        print("[*] Testing API access with Organization query...")
        query_response = requests.get(query_url, headers=query_headers, params=query_params, timeout=30)
        
        if query_response.status_code != 200:
            print(f"[-] API query failed with status {query_response.status_code}")
            return {
                'success': False,
                'error': f"API query failed: {query_response.text}",
                'access_token_obtained': True
            }
        
        query_result = query_response.json()
        org_name = query_result.get('records', [{}])[0].get('Name', 'Unknown')
        org_id = query_result.get('records', [{}])[0].get('Id', 'Unknown')
        
        print(f"[+] Salesforce connection successful!")
        print(f"[+] Organization: {org_name}")
        print(f"[+] Organization ID: {org_id}")
        print(f"[+] Instance URL: {instance_url}")
        
        return {
            'success': True,
            'message': 'Salesforce connection successful',
            'org_name': org_name,
            'org_id': org_id,
            'instance_url': instance_url
        }
        
    except requests.exceptions.Timeout:
        print("[-] Salesforce connection timed out")
        return {
            'success': False,
            'error': 'Connection timeout'
        }
    except Exception as e:
        print("[-] Salesforce connection failed:")
        print(str(e))
        return {
            'success': False,
            'error': str(e)
        }

if __name__ == "__main__":
    print("=" * 80)
    print("AWS Lambda Salesforce Connection Test with Internet Connectivity Check")
    print("=" * 80)
    
    # Simulate Lambda handler
    result = lambda_handler(None, None)
    
    print("\n" + "=" * 80)
    print("FINAL RESULT")
    print("=" * 80)
    print(result['body'])

