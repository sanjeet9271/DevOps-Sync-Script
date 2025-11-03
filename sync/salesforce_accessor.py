"""
Salesforce Accessor Module
Handles authentication and data retrieval from Salesforce
"""
import requests
from typing import Dict, List, Generator, Optional


class SalesforceAccessor:
    """Handles Salesforce API operations"""
    
    def __init__(self, org_url: str, client_id: str, client_secret: str):
        self.org_url = org_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.instance_url = None
    
    def authenticate(self) -> bool:
        """Authenticate with Salesforce using OAuth 2.0 Client Credentials"""
        try:
            token_url = f"{self.org_url}/services/oauth2/token"
            
            response = requests.post(
                token_url,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                data={
                    'grant_type': 'client_credentials',
                    'client_id': self.client_id,
                    'client_secret': self.client_secret
                },
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"[!] Authentication failed: {response.text}")
                return False
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            self.instance_url = token_data.get('instance_url', self.org_url)
            
            print(f"[+] Salesforce authenticated: {self.instance_url}")
            return True
            
        except Exception as e:
            print(f"[!] Authentication error: {str(e)}")
            return False
    
    def query_batch(self, soql: str, batch_size: int = 2000) -> Generator[List[Dict], None, None]:
        """
        Query Salesforce in batches using pagination
        
        Args:
            soql: SOQL query string
            batch_size: Records per batch (max 2000 for Salesforce)
        
        Yields:
            List of records for each batch
        """
        if not self.access_token:
            if not self.authenticate():
                return
        
        # Ensure LIMIT is set in query
        if 'LIMIT' not in soql.upper():
            soql = f"{soql.rstrip()} LIMIT {batch_size}"
        
        query_url = f"{self.instance_url}/services/data/v59.0/query"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        total_fetched = 0
        batch_num = 0
        
        try:
            # Initial query
            response = requests.get(
                query_url,
                headers=headers,
                params={'q': soql.strip()},
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"[!] Query failed: {response.text}")
                return
            
            result = response.json()
            records = result.get('records', [])
            
            if records:
                batch_num += 1
                total_fetched += len(records)
                print(f"[+] Batch {batch_num}: Fetched {len(records)} records (Total: {total_fetched})")
                yield records
            
            # Handle pagination using nextRecordsUrl
            while not result.get('done', True):
                next_url = f"{self.instance_url}{result['nextRecordsUrl']}"
                
                response = requests.get(
                    next_url,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code != 200:
                    print(f"[!] Pagination query failed: {response.text}")
                    break
                
                result = response.json()
                records = result.get('records', [])
                
                if records:
                    batch_num += 1
                    total_fetched += len(records)
                    print(f"[+] Batch {batch_num}: Fetched {len(records)} records (Total: {total_fetched})")
                    yield records
            
            print(f"[+] Salesforce query complete: {total_fetched} total records")
            
        except Exception as e:
            print(f"[!] Query error: {str(e)}")
    
    def query_all(self, soql: str) -> List[Dict]:
        """
        Query all records at once (use with caution for large datasets)
        
        Args:
            soql: SOQL query string
        
        Returns:
            List of all records
        """
        all_records = []
        for batch in self.query_batch(soql):
            all_records.extend(batch)
        return all_records

