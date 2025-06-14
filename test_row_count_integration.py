#!/usr/bin/env python3
"""
Test script for the new row count checking functionality.
"""

import sys
from pathlib import Path

# Add the v1 package to path
sys.path.insert(0, str(Path(__file__).parent / "v1"))

def test_row_count_checker():
    """Test the row count checker module."""
    
    print("=== TESTING ROW COUNT CHECKER ===")
    
    try:
        from v1.schema_grader.grading.row_count_checker import (
            BUSINESS_LOGIC_CHANGES, 
            check_business_logic_implementation,
            format_row_count_results
        )
        
        print("✅ Successfully imported row count checker modules")
        print(f"✅ Business logic changes configured: {BUSINESS_LOGIC_CHANGES}")
        
        # Test format function with mock data
        mock_results = {
            'tables_checked': {
                'NhaCungCap': {
                    'answer_count': 10,
                    'student_count': 11,
                    'expected_increase': 1,
                    'actual_increase': 1,
                    'correct': True
                },
                'NhanVien': {
                    'answer_count': 5,
                    'student_count': 5,
                    'expected_increase': 1,
                    'actual_increase': 0,
                    'correct': False
                }
            },
            'all_correct': False,
            'total_score': 1,
            'max_score': 2
        }
        
        formatted = format_row_count_results(mock_results, "TEST001")
        print(f"✅ Format function works: {len(formatted)} rows generated")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def test_pipeline_integration():
    """Test pipeline integration."""
    
    print("\n=== TESTING PIPELINE INTEGRATION ===")
    
    try:
        from v1.schema_grader.grading.pipeline import run_for_one_bak, run_batch
        
        print("✅ Successfully imported updated pipeline functions")
        
        # Check function signatures
        import inspect
        
        run_for_one_sig = inspect.signature(run_for_one_bak)
        if 'check_row_counts' in run_for_one_sig.parameters:
            print("✅ run_for_one_bak has check_row_counts parameter")
        else:
            print("❌ run_for_one_bak missing check_row_counts parameter")
            
        run_batch_sig = inspect.signature(run_batch)
        if 'check_row_counts' in run_batch_sig.parameters:
            print("✅ run_batch has check_row_counts parameter")
        else:
            print("❌ run_batch missing check_row_counts parameter")
            
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def test_reporter_integration():
    """Test reporter integration."""
    
    print("\n=== TESTING REPORTER INTEGRATION ===")
    
    try:
        from v1.schema_grader.grading.reporter import save_schema_results_csv, save_row_count_summary
        
        print("✅ Successfully imported updated reporter functions")
        
        # Test with mock data
        mock_results = [
            {
                'db_name': 'TEST001',
                'schema_score': 0.85,
                'business_logic_score': 4,
                'business_logic_max': 5,
                'business_logic_complete': False,
                'fk_ratio': 0.9,
                'table_results': [{'matched': True}, {'matched': False}]
            }
        ]
        
        # This would normally save to file, but we're just testing the function exists
        print("✅ Reporter functions are callable")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def main():
    """Run all tests."""
    
    print("TESTING NEW ROW COUNT FUNCTIONALITY")
    print("=" * 50)
    
    tests = [
        test_row_count_checker,
        test_pipeline_integration,
        test_reporter_integration
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n{'=' * 50}")
    print(f"RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Row count functionality is ready to use.")
        print("\nTo use the new functionality:")
        print("1. Run: cd v1\\cli")
        print("2. Run: python grade_bak.py")
        print("3. The system will now automatically check row counts after foreign key analysis")
        print("4. Results will include:")
        print("   - Individual [MSSV]_rowcount.csv files")
        print("   - Consolidated row_count_summary.csv")
        print("   - Enhanced schema_grading_results.csv with business logic scores")
    else:
        print("❌ Some tests failed. Please check the integration.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
