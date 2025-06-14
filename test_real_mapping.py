#!/usr/bin/env python3
"""
Test the row count functionality with the full pipeline.
"""

import os
import sys
from pathlib import Path

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

from v1.schema_grader.config import GradingConfig
from v1.schema_grader.db import connection, restore
from v1.schema_grader.grading.row_count_checker import check_mapped_table_row_counts, get_real_table_names

def test_with_fake_mapping():
    """Test row count with a fake mapping to see if real table names are used."""
    
    config = GradingConfig()
    
    # Create a fake mapping with possibly normalized names
    fake_mapping = {
        'ChiTietMuaHang': 'CT_MuaHang',  # This might be the issue - mapping has different names
        'HangHoa': 'HangTonKho',
        'NhanVien': 'NhanVien'
    }
    
    try:
        # Connect to both databases
        with connection.open_conn(config.server, config.user, config.password, "00000001") as answer_conn, \
             connection.open_conn(config.server, config.user, config.password, "23701621") as student_conn:
            
            print("=== Testing Real Table Names Mapping ===")
            
            # Get real table names from both databases
            answer_real = get_real_table_names(answer_conn)
            student_real = get_real_table_names(student_conn)
            
            print(f"Answer real tables: {sorted(answer_real.values())}")
            print(f"Student real tables: {sorted(student_real.values())}")
            
            print(f"\n=== Testing with Fake Mapping ===")
            print(f"Fake mapping: {fake_mapping}")
            
            # Run row count check
            result = check_mapped_table_row_counts(answer_conn, student_conn, fake_mapping)
            
            print(f"\nResults:")
            for table, data in result.get('mapped_tables', {}).items():
                print(f"  {table}: Answer={data['answer_count']}, Student={data['student_count']}")
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_with_fake_mapping()
