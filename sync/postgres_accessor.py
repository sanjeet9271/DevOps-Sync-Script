"""
PostgreSQL Accessor Module
Handles database operations including upserts, deletes, and watermark management
"""
import psycopg2
from typing import List, Dict, Tuple, Optional
from psycopg2.extras import execute_values
from datetime import datetime


class PostgresAccessor:
    """Handles PostgreSQL database operations"""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                sslmode='require',
                connect_timeout=10
            )
            self.cursor = self.conn.cursor()
            print(f"[+] PostgreSQL connected: {self.host}/{self.database}")
            return True
            
        except Exception as e:
            print(f"[!] PostgreSQL connection failed: {str(e)}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("[+] PostgreSQL disconnected")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            self.cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            """, (table_name,))
            exists = self.cursor.fetchone()[0]
            return exists
        except Exception as e:
            print(f"[!] Error checking table existence: {str(e)}")
            return False
    
    def get_watermark(self, table_name: str) -> Optional[datetime]:
        """
        Get the last sync watermark for a table
        
        Args:
            table_name: Name of the table to get watermark for
        
        Returns:
            datetime of last sync, or None if no watermark exists
        """
        try:
            # Ensure watermark table exists
            if not self.table_exists('watermark'):
                print("[!] Watermark table does not exist. Creating it...")
                self.cursor.execute("""
                    CREATE TABLE IF NOT EXISTS watermark (
                        table_name VARCHAR(255) PRIMARY KEY,
                        last_synced_time TIMESTAMP NOT NULL
                    )
                """)
                self.conn.commit()
                print("[+] Watermark table created")
                return None
            
            # Get watermark
            self.cursor.execute(
                "SELECT last_synced_time FROM watermark WHERE table_name = %s",
                (table_name,)
            )
            result = self.cursor.fetchone()
            
            if result:
                watermark = result[0]
                print(f"[+] Watermark for '{table_name}': {watermark}")
                return watermark
            else:
                print(f"[*] No watermark found for '{table_name}' (first sync)")
                return None
                
        except Exception as e:
            print(f"[!] Error getting watermark: {str(e)}")
            return None
    
    def update_watermark(self, table_name: str, sync_time: datetime) -> bool:
        """
        Update the watermark for a table
        
        Args:
            table_name: Name of the table
            sync_time: Timestamp to set as watermark
        
        Returns:
            True if successful, False otherwise
        """
        try:
            self.cursor.execute("""
                INSERT INTO watermark (table_name, last_synced_time)
                VALUES (%s, %s)
                ON CONFLICT (table_name)
                DO UPDATE SET last_synced_time = EXCLUDED.last_synced_time
            """, (table_name, sync_time))
            
            self.conn.commit()
            print(f"[+] Watermark updated for '{table_name}': {sync_time}")
            return True
            
        except Exception as e:
            print(f"[!] Error updating watermark: {str(e)}")
            self.conn.rollback()
            return False
    
    def upsert_batch(
        self, 
        table_name: str, 
        records: List[Dict], 
        field_mapping: Dict[str, str],
        primary_keys: List[str]
    ) -> Tuple[int, int]:
        """
        Upsert a batch of records using ON CONFLICT
        
        Args:
            table_name: Target table name
            records: List of record dictionaries from Salesforce
            field_mapping: Mapping from Salesforce fields to PostgreSQL columns
            primary_keys: List of column names that form the primary key
        
        Returns:
            Tuple of (inserted_count, updated_count)
        """
        if not records:
            return 0, 0
        
        try:
            # Build column lists
            pg_columns = list(field_mapping.values())
            
            # Extract and transform data
            values = []
            for record in records:
                row = []
                for sf_field, pg_column in field_mapping.items():
                    # Handle nested relationships (e.g., "Account__r.Name")
                    if '.' in sf_field:
                        parts = sf_field.split('.')
                        value = record.get(parts[0], {})
                        for part in parts[1:]:
                            value = value.get(part) if isinstance(value, dict) else None
                    else:
                        value = record.get(sf_field)
                    row.append(value)
                values.append(tuple(row))
            
            # Build upsert SQL
            columns_str = ", ".join(pg_columns)
            placeholders = ", ".join(["%s"] * len(pg_columns))
            
            # Conflict target (primary keys)
            conflict_target = ", ".join(primary_keys)
            
            # Update set clause (exclude primary keys)
            update_columns = [col for col in pg_columns if col not in primary_keys]
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
            
            upsert_sql = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES %s
                ON CONFLICT ({conflict_target})
                DO UPDATE SET {update_set}
            """
            
            # Execute batch upsert
            execute_values(self.cursor, upsert_sql, values, template=f"({placeholders})")
            self.conn.commit()
            
            affected_rows = self.cursor.rowcount
            print(f"[+] Upserted {affected_rows} records to '{table_name}'")
            
            return affected_rows, 0
            
        except Exception as e:
            print(f"[!] Upsert error: {str(e)}")
            self.conn.rollback()
            return 0, 0
    
    def delete_by_keys(
        self, 
        table_name: str, 
        records: List[Dict],
        field_mapping: Dict[str, str],
        primary_keys: List[str]
    ) -> int:
        """
        Delete records based on primary keys
        
        Args:
            table_name: Target table name
            records: List of records to delete (with IsDeleted=True)
            field_mapping: Mapping from Salesforce fields to PostgreSQL columns
            primary_keys: List of column names that form the primary key
        
        Returns:
            Number of deleted records
        """
        if not records:
            return 0
        
        try:
            # Build WHERE clause for batch delete
            delete_conditions = []
            
            for record in records:
                conditions = []
                for pk in primary_keys:
                    # Find SF field for this PG column
                    sf_field = None
                    for sf_f, pg_c in field_mapping.items():
                        if pg_c == pk:
                            sf_field = sf_f
                            break
                    
                    if sf_field:
                        # Handle nested fields
                        if '.' in sf_field:
                            parts = sf_field.split('.')
                            value = record.get(parts[0], {})
                            for part in parts[1:]:
                                value = value.get(part) if isinstance(value, dict) else None
                        else:
                            value = record.get(sf_field)
                        
                        if value is not None:
                            conditions.append(f"{pk} = '{value}'")
                
                if conditions:
                    delete_conditions.append(f"({' AND '.join(conditions)})")
            
            if delete_conditions:
                where_clause = " OR ".join(delete_conditions)
                delete_sql = f"DELETE FROM {table_name} WHERE {where_clause}"
                
                self.cursor.execute(delete_sql)
                self.conn.commit()
                
                deleted_count = self.cursor.rowcount
                print(f"[+] Deleted {deleted_count} records from '{table_name}'")
                return deleted_count
            
            return 0
            
        except Exception as e:
            print(f"[!] Delete error: {str(e)}")
            self.conn.rollback()
            return 0
    
    def get_record_count(self, table_name: str) -> int:
        """Get total record count in table"""
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            return count
        except:
            return 0
