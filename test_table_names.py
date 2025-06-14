#!/usr/bin/env python3
"""
Test script to verify table names used in row count analysis.

This test checks if the actual table names from the database match
the names used in the row count queries.
"""

import os
import sys
from pathlib import Path

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

from v1.schema_grader.db import connection, schema_reader, build_schema, restore
from v1.schema_grader.config import GradingConfig
from v1.schema_grader.matching.table_matcher import phase1
from v1.schema_grader.grading.row_count_checker import get_all_table_counts, get_table_row_count
from v1.schema_grader.utils.log import get_logger

logger = get_logger(__name__)

def test_table_names_consistency():
    """Test if table names are consistent between schema reading and row count queries."""
    
    # Load config
    config = GradingConfig()
    
    print("Testing table name consistency...")
    print(f"Server: {config.server}")
    print(f"User: {config.user}")
    
    try:
        # Test with answer database
        answer_db = "00000001"
        print(f"\n=== Testing Answer Database: {answer_db} ===")
        
        with connection.open_conn(config.server, config.user, config.password, answer_db) as conn:
            # Method 1: Get tables from INFORMATION_SCHEMA (used in schema_reader)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE LEFT(TABLE_NAME, 3) <> 'sys'
                  AND TABLE_NAME IN (
                    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE='BASE TABLE' AND LEFT(TABLE_NAME,3) <> 'sys')
                ORDER BY TABLE_NAME, ORDINAL_POSITION
            """)
            schema_tables = set()
            rows = cursor.fetchall()
            for table_name, col_name, data_type in rows:
                # Apply same cleaning as schema_reader
                clean_name = schema_reader._clean_table_name(table_name)
                schema_tables.add(clean_name)
            
            print(f"Tables from schema reader: {sorted(schema_tables)}")
            
            # Method 2: Get tables from row count function
            row_count_tables = get_all_table_counts(conn)
            print(f"Tables from row count function: {sorted(row_count_tables.keys())}")
            
            # Method 3: Direct query for comparison
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            direct_tables = [row[0] for row in cursor.fetchall()]
            print(f"Tables from direct query: {sorted(direct_tables)}")
            
            # Compare sets
            schema_set = set(schema_tables)
            row_count_set = set(row_count_tables.keys())
            direct_set = set(direct_tables)
            
            print(f"\nComparison:")
            print(f"Schema == Row Count: {schema_set == row_count_set}")
            print(f"Schema == Direct: {schema_set == direct_set}")
            print(f"Row Count == Direct: {row_count_set == direct_set}")
            
            if schema_set != row_count_set:
                print(f"Schema only: {schema_set - row_count_set}")
                print(f"Row count only: {row_count_set - schema_set}")
            
            # Test individual table queries
            print(f"\n=== Testing Individual Table Queries ===")
            for table in sorted(schema_set):
                count = get_table_row_count(conn, table)
                print(f"Table '{table}': {count} rows")
        
        print(f"\n=== Test Complete ===")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_table_names_consistency()
