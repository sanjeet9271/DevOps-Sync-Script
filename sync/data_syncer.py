"""
Data Syncer Module
Orchestrates the sync process between Salesforce and PostgreSQL with watermark support
"""
from typing import Dict, List
from datetime import datetime
from salesforce_accessor import SalesforceAccessor
from postgres_accessor import PostgresAccessor


class SyncConfig:
    """Configuration for a sync operation"""
    
    def __init__(
        self,
        sf_object: str,
        soql_query: str,
        pg_table: str,
        field_mapping: Dict[str, str],
        primary_keys: List[str],
        batch_size: int = 2000
    ):
        self.sf_object = sf_object
        self.soql_query = soql_query
        self.pg_table = pg_table
        self.field_mapping = field_mapping
        self.primary_keys = primary_keys
        self.batch_size = batch_size


class DataSyncer:
    """Orchestrates data sync between Salesforce and PostgreSQL"""
    
    def __init__(self, sf_accessor: SalesforceAccessor, pg_accessor: PostgresAccessor):
        self.sf = sf_accessor
        self.pg = pg_accessor
        self.stats = {
            'total_fetched': 0,
            'total_upserted': 0,
            'total_deleted': 0,
            'batches_processed': 0
        }
    
    def sync(self, config: SyncConfig) -> Dict:
        """
        Execute sync operation for a single table with watermark support
        
        Args:
            config: SyncConfig object with sync parameters
        
        Returns:
            Dict with sync statistics
        """
        print("=" * 80)
        print(f"Starting sync: {config.sf_object} -> {config.pg_table}")
        print("=" * 80)
        
        # Reset stats
        self.stats = {
            'total_fetched': 0,
            'total_upserted': 0,
            'total_deleted': 0,
            'batches_processed': 0
        }
        
        # Check if table exists
        if not self.pg.table_exists(config.pg_table):
            error_msg = f"Table '{config.pg_table}' does not exist in database"
            print(f"[!] ERROR: {error_msg}")
            return {
                'success': False,
                'sf_object': config.sf_object,
                'pg_table': config.pg_table,
                'error': error_msg
            }
        
        # Get watermark (last sync time)
        watermark = self.pg.get_watermark(config.pg_table)
        sync_start_time = datetime.utcnow()
        
        # Build query with watermark filter
        query = self._build_query_with_watermark(config.soql_query, watermark)
        
        # Get initial record count
        initial_count = self.pg.get_record_count(config.pg_table)
        print(f"[*] Initial record count in '{config.pg_table}': {initial_count}")
        
        # Process batches
        for batch in self.sf.query_batch(query, config.batch_size):
            self.stats['batches_processed'] += 1
            self.stats['total_fetched'] += len(batch)
            
            # Separate active and deleted records
            active_records = [r for r in batch if not r.get('IsDeleted', False)]
            deleted_records = [r for r in batch if r.get('IsDeleted', False)]
            
            # Upsert active records
            if active_records:
                upserted, _ = self.pg.upsert_batch(
                    config.pg_table,
                    active_records,
                    config.field_mapping,
                    config.primary_keys
                )
                self.stats['total_upserted'] += upserted
            
            # Delete marked records
            if deleted_records:
                deleted = self.pg.delete_by_keys(
                    config.pg_table,
                    deleted_records,
                    config.field_mapping,
                    config.primary_keys
                )
                self.stats['total_deleted'] += deleted
            
            print(f"[*] Batch {self.stats['batches_processed']}: "
                  f"Active={len(active_records)}, Deleted={len(deleted_records)}")
        
        # Update watermark after successful sync
        if self.stats['total_fetched'] > 0:
            self.pg.update_watermark(config.pg_table, sync_start_time)
        else:
            print(f"[*] No new records to sync for '{config.pg_table}'")
        
        # Get final record count
        final_count = self.pg.get_record_count(config.pg_table)
        
        print("\n" + "=" * 80)
        print("Sync Complete")
        print("=" * 80)
        print(f"Records fetched from Salesforce: {self.stats['total_fetched']}")
        print(f"Records upserted to PostgreSQL: {self.stats['total_upserted']}")
        print(f"Records deleted from PostgreSQL: {self.stats['total_deleted']}")
        print(f"Batches processed: {self.stats['batches_processed']}")
        print(f"Watermark: {watermark} -> {sync_start_time}")
        print(f"Initial DB count: {initial_count}")
        print(f"Final DB count: {final_count}")
        print(f"Net change: {final_count - initial_count:+d}")
        print("=" * 80)
        
        return {
            'success': True,
            'sf_object': config.sf_object,
            'pg_table': config.pg_table,
            'records_fetched': self.stats['total_fetched'],
            'records_upserted': self.stats['total_upserted'],
            'records_deleted': self.stats['total_deleted'],
            'batches_processed': self.stats['batches_processed'],
            'initial_count': initial_count,
            'final_count': final_count,
            'net_change': final_count - initial_count,
            'previous_watermark': str(watermark) if watermark else None,
            'new_watermark': str(sync_start_time)
        }
    
    def _build_query_with_watermark(self, base_query: str, watermark: datetime) -> str:
        """
        Add watermark filter to SOQL query
        
        Args:
            base_query: Original SOQL query
            watermark: Last sync timestamp
        
        Returns:
            Modified query with LastModifiedDate filter
        """
        # Ensure IsDeleted is in the query
        if 'IsDeleted' not in base_query:
            # Add IsDeleted to SELECT clause
            if 'FROM' in base_query.upper():
                parts = base_query.split('FROM')
                select_part = parts[0].rstrip().rstrip(',')
                from_part = 'FROM' + ''.join(parts[1:])
                base_query = f"{select_part}, IsDeleted {from_part}"
        
        # Ensure LastModifiedDate is in the query for watermark
        if 'LastModifiedDate' not in base_query:
            if 'FROM' in base_query.upper():
                parts = base_query.split('FROM')
                select_part = parts[0].rstrip().rstrip(',')
                from_part = 'FROM' + ''.join(parts[1:])
                base_query = f"{select_part}, LastModifiedDate {from_part}"
        
        # Add WHERE clause for watermark if exists
        if watermark:
            watermark_str = watermark.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            if 'WHERE' in base_query.upper():
                # Append to existing WHERE
                base_query = base_query.rstrip()
                base_query += f" AND LastModifiedDate > {watermark_str}"
            else:
                # Add new WHERE clause before LIMIT if exists
                if 'LIMIT' in base_query.upper():
                    parts = base_query.split('LIMIT')
                    base_query = f"{parts[0].rstrip()} WHERE LastModifiedDate > {watermark_str} LIMIT {'LIMIT'.join(parts[1:])}"
                else:
                    base_query = base_query.rstrip() + f" WHERE LastModifiedDate > {watermark_str}"
        
        return base_query
    
    def sync_multiple(self, configs: List[SyncConfig]) -> List[Dict]:
        """
        Execute sync for multiple tables
        
        Args:
            configs: List of SyncConfig objects
        
        Returns:
            List of sync results for each table
        """
        results = []
        
        for i, config in enumerate(configs, 1):
            print(f"\n{'#' * 80}")
            print(f"Syncing table {i}/{len(configs)}: {config.sf_object}")
            print(f"{'#' * 80}\n")
            
            try:
                result = self.sync(config)
                results.append(result)
            except Exception as e:
                print(f"[!] Sync failed for {config.sf_object}: {str(e)}")
                results.append({
                    'success': False,
                    'sf_object': config.sf_object,
                    'error': str(e)
                })
        
        return results
