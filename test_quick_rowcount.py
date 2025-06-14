#!/usr/bin/env python3
"""
Quick test for row count queries with real table names.
"""

import os
import sys
from pathlib import Path

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

from v1.schema_grader.db import connection
from v1.schema_grader.config import GradingConfig
from v1.schema_grader.grading.row_count_checker import get_table_row_count, get_real_table_names

def test_row_count():
    """Test row count with actual student database."""
    
    config = GradingConfig()
    print(f"Testing with server: {config.server}, user: {config.user}")
    
    # Test with student database 23701621
    student_db = "23701621"
    
    try:
        with connection.open_conn(config.server, config.user, config.password, student_db) as conn:
            print(f"\n=== Testing Student Database: {student_db} ===")
            
            # Get real table names
            real_names = get_real_table_names(conn)
            print(f"Real table names mapping: {real_names}")
            
            # Direct query for all tables
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            print(f"\nTables found: {tables}")
            
            # Test row count for each table
            print(f"\n=== Row Counts ===")
            for table in tables:
                count = get_table_row_count(conn, table)
                print(f"Table '{table}': {count} rows")
                
                # Also test with different approaches
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                    count_brackets = cursor.fetchone()[0]
                    print(f"  With brackets: {count_brackets}")
                except Exception as e:
                    print(f"  With brackets failed: {e}")
                
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count_no_brackets = cursor.fetchone()[0]
                    print(f"  Without brackets: {count_no_brackets}")
                except Exception as e:
                    print(f"  Without brackets failed: {e}")
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_row_count()
