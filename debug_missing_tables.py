#!/usr/bin/env python3
"""
Debug script to check why some tables are missing from the row count analysis.
"""

import os
import sys
from pathlib import Path

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

from v1.schema_grader.config import GradingConfig
from v1.schema_grader.db import connection
from v1.schema_grader.grading.row_count_checker import get_real_table_names

def debug_missing_tables():
    """Debug why only 6 tables appear instead of 8."""
    
    config = GradingConfig()
    
    try:
        # Check answer database tables
        with connection.open_conn(config.server, config.user, config.password, "00000001") as conn:
            real_names = get_real_table_names(conn)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            
            tables = [row[0] for row in cursor.fetchall()]
            print(f"=== Answer Database Tables ({len(tables)}) ===")
            for i, table in enumerate(tables, 1):
                print(f"{i}. {table}")
            
            print(f"\n=== Tables from CSV Result (6) ===")
            csv_tables = [
                'ChiTietMuaHang',
                'HangHoa', 
                'LoaiTien',
                'MuaHang',
                'NhaCungCap',
                'NhanVien'
            ]
            for i, table in enumerate(csv_tables, 1):
                print(f"{i}. {table}")
            
            print(f"\n=== Missing Tables Analysis ===")
            all_table_set = set(tables)
            csv_table_set = set(csv_tables)
            
            missing_from_csv = all_table_set - csv_table_set
            print(f"Missing from CSV ({len(missing_from_csv)}): {missing_from_csv}")
            
            # Check if missing tables might be due to normalization
            print(f"\n=== Checking Normalization Issues ===")
            for table in missing_from_csv:
                print(f"Missing table '{table}' might be normalized as:")
                # Try common normalizations
                normalizations = [
                    table.lower(),
                    table.replace('Tra', 'tra'),
                    table.replace('Tien', 'tien'),
                    table.replace('Chi', 'chi'),
                    table.replace('Tiet', 'tiet')
                ]
                for norm in normalizations:
                    print(f"  - {norm}")
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_missing_tables()
