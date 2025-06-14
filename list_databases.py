#!/usr/bin/env python3
"""
List all available databases.
"""

import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

try:
    from v1.schema_grader.db import connection
    from v1.schema_grader.config import GradingConfig

    config = GradingConfig()
    
    # Connect to master database to list all databases
    with connection.open_conn(config.server, config.user, config.password, "master") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')")
        databases = [row[0] for row in cursor.fetchall()]
        
        print(f"Available databases: {databases}")
        
        # Try to connect to each database and get table count
        for db in databases:
            try:
                with connection.open_conn(config.server, config.user, config.password, db) as db_conn:
                    cursor = db_conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
                    table_count = cursor.fetchone()[0]
                    print(f"Database {db}: {table_count} tables")
            except Exception as e:
                print(f"Database {db}: Error - {e}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
