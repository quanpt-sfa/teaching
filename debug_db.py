#!/usr/bin/env python3
"""
Simple debug script for database connection and row count.
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

try:
    from v1.schema_grader.db import connection
    from v1.schema_grader.config import GradingConfig

    config = GradingConfig()
    print(f"Config loaded - Server: {config.server}, User: {config.user}")

    # Test with answer database
    print("\n=== Testing Answer Database ===")
    with connection.open_conn(config.server, config.user, config.password, "00000001") as conn:
        cursor = conn.cursor()
        
        # Get tables
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Tables in answer DB: {tables}")
        
        # Test row count for first few tables
        for table in tables[:3]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                count = cursor.fetchone()[0]
                print(f"Table {table}: {count} rows")
            except Exception as e:
                print(f"Error counting {table}: {e}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
